{% macro get_allday_metadata() %}

{% set create_table %}
CREATE SCHEMA IF NOT EXISTS {{ target.database }}.bronze_api;
CREATE TABLE IF NOT EXISTS {{ target.database }}.bronze_api.allday_metadata(
    data VARIANT,
    fetch_time TIMESTAMP_NTZ,
    contract STRING
);
{% endset %}

{% set event_table %}

CREATE TABLE IF NOT EXISTS {{ target.database }}.bronze_api.log_messages (
    timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    log_level STRING,
    message STRING
);
{% endset %}

{% do run_query(event_table) %}
{% do run_query(create_table) %}

{% set query %}
CREATE OR REPLACE PROCEDURE {{ target.database }}.bronze_api.allday_metadata()
    RETURNS STRING 
    LANGUAGE JAVASCRIPT 
    EXECUTE AS CALLER 
    AS $$

        const nfl_contract =  'A.e4cf4bdc1751c65d.AllDay'

        function logMessage(level, message) {
            var log_sql = `INSERT INTO {{ target.database }}.bronze_api.log_messages (log_level, message) VALUES (?, ?)`;
            snowflake.execute({sqlText: log_sql, binds: [level, message]});
        }
        create_temp_moments_table = `
            CREATE OR REPLACE TEMPORARY TABLE {{ target.database }}.bronze_api.moments_table AS
                WITH mints AS (

                    SELECT
                        event_contract,
                        event_data :id :: STRING AS moment_id
                    FROM
                        flow.silver.nft_moments_s
                    WHERE
                        event_contract = '${nfl_contract}'
                        AND event_type = 'MomentNFTMinted'
                ),
                sales AS (
                    SELECT
                        nft_collection AS event_contract,
                        nft_id AS moment_id
                    FROM
                        flow.silver.nft_sales_s
                    WHERE
                        nft_collection = '${nfl_contract}'
                ),
                all_day_ids AS (
                    SELECT
                        *
                    FROM
                        mints
                    UNION
                    SELECT
                        *
                    FROM
                        sales
                )
                SELECT
                    *
                FROM
                    all_day_ids
                EXCEPT
                SELECT
                    nft_collection AS event_contract,
                    nft_id AS moment_id
                FROM
                    flow.silver.nft_allday_metadata -- old view
                ORDER BY MOMENT_ID ASC
            `;
                    
        snowflake.execute({sqlText:create_temp_moments_table})
  
        
        var res = snowflake.execute({sqlText: `
        WITH subset as (
                SELECT *
                FROM {{ target.database }}.bronze_api.moments_table
            )
            SELECT count(*)
            FROM subset
            LIMIT 10 -- Delete when has been tested`});

        res.next()
        row_count = res.getColumnValue(1);

        lambda_num = 2
        batch_size = 2 // limit on their end

        //call_groups = Math.ceil(row_count / (lambda_num * batch_size))
        call_groups = 1

        for (i = 0; i < call_groups; i++) {


            var flows_ids = snowflake.execute({sqlText: `
            WITH subset as (
                SELECT *
                FROM {{ target.database }}.bronze_api.moments_table
                ORDER BY MOMENT_ID ASC
                limit ${batch_size * lambda_num} offset ${i * batch_size * lambda_num}
            )
            SELECT ARRAY_AGG(CAST(MOMENT_ID AS INTEGER))
            FROM subset`});

            flows_ids.next()
            row_list = flows_ids.getColumnValue(1);
            
            var create_temp_table_command = `
                CREATE OR REPLACE TEMPORARY TABLE {{ target.database }}.bronze_api.response_data AS
                WITH api_call AS (
            `;
            
            for (let j = 0; j < lambda_num; j++) {
                // Extract a subset of row_list for the current API call
                let subset = row_list.slice(j * batch_size, (j + 1) * batch_size);
                let query = `{
                searchMomentNFTsV2(input: {filters: {byFlowIDs: [${subset}]}}) {
                    edges {
                    cursor
                    node {
                        id
                        ownerAddress
                        serialNumber
                        flowID
                        distributionFlowID
                        packNFTFlowID
                        editionFlowID
                        owner {
                        id
                        dapperID
                        email
                        phoneNumber
                        username
                        flowAddress
                        profileImageUrl
                        isCurrentTOSSigned
                        }
                        edition {
                        id
                        flowID
                        playFlowID
                        seriesFlowID
                        setFlowID
                        maxMintSize
                        currentMintSize
                        tier
                        description
                        numMomentsOwned
                        numMomentsInPacks
                        numMomentsUnavailable
                        numMomentsBurned
                        series {
                            flowID
                            name
                            active
                        }
                        set {
                            flowID
                            name
                        }
                        play {
                            id
                            flowID
                            metadata {
                            state
                            description
                            league
                            playType
                            videos {
                                type
                                url
                                videoLength
                            }
                            images {
                                type
                                url
                            }
                            classification
                            week
                            season
                            playerID
                            playerFullName
                            playerFirstName
                            playerLastName
                            playerPosition
                            playerNumber
                            playerWeight
                            playerHeight
                            playerBirthdate
                            playerBirthplace
                            playerRookieYear
                            playerDraftTeam
                            playerDraftYear
                            playerDraftRound
                            playerDraftNumber
                            playerCollege
                            teamID
                            gameNflID
                            gameDate
                            homeTeamName
                            homeTeamID
                            homeTeamScore
                            awayTeamName
                            awayTeamID
                            awayTeamScore
                            gameTime
                            gameQuarter
                            gameDown
                            gameDistance
                            teamName
                            }
                        }
                        }
                    }
                    }
                }
                }`;
                create_temp_table_command += `
                SELECT * FROM (
                    SELECT 
                        livequery_dev.live.udf_api('GET', CONCAT('https://nflallday.com/consumer/graphql?query=','${query}' ), {'Accept-Encoding': 'gzip', 'Content-Type': 'application/json', 'Accept': 'application/json','Connection': 'keep-alive'},{}) AS res 
                    )
                `;

                if (j < lambda_num - 1) {
                        create_temp_table_command += `  UNION ALL `;
                    }
                }

            create_temp_table_command+= `
            ),
            flatten_res AS (
                SELECT
                    flattened_array.value as data,
                    api_call.res:status_code as status_code,
                    SYSDATE() as fetch_time
                FROM api_call,
                LATERAL FLATTEN(input => api_call.res:data:data:searchMomentNFTsV2:edges) as flattened_array
                WHERE api_call.res:status_code = 200 
                AND data IS NOT NULL    
            )
            SELECT
                data,
                status_code,
                SYSDATE() as fetch_time,
                '${nfl_contract}' as contract
            FROM
            flatten_res
            `;
            snowflake.execute({sqlText: create_temp_table_command});
            // Second command: Insert data into the target table from the temporary table
            
            var insert_command = `
                INSERT INTO {{ target.database }}.bronze_api.allday_metadata(
                            data,
                            fetch_time,
                            contract
                        )
                        SELECT
                            data:node as data,
                            fetch_time,
                            contract
                        FROM {{ target.database }}.bronze_api.response_data           
            `;
            snowflake.execute({sqlText: insert_command});
            
            var log_message = `INSERT INTO {{ target.database }}.bronze_api.log_messages (log_level, message) VALUES ('INFO', ' Iteration ${i} of ${call_groups} complete.')`;
            snowflake.execute({sqlText: log_message});
        }
        return 'Success';

$$;
{% endset %}
{% do run_query(query) %}
{% endmacro %}