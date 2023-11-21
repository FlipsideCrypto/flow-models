{% macro get_allday_metadata() %}
{% set create_table %}
CREATE SCHEMA IF NOT EXISTS {{ target.database }}.bronze_api;
CREATE TABLE IF NOT EXISTS {{ target.database }}.bronze_api.allday_metadata(
    requested_ids ARRAY,
    res VARIANT,
    _inserted_timestamp TIMESTAMP_NTZ,
    contract STRING
);
{% endset %}

{% set event_table %}

CREATE OR REPLACE TABLE {{ target.database }}.bronze_api.log_messages (
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
        
        var res = snowflake.execute({sqlText: `select * from {{ target.database }}.silver.allday_moments_metadata_needed_s ORDER BY MOMENT_ID ASC`})
        query_id = res.getQueryId();

        res = snowflake.execute({sqlText: `
            SELECT count(*) FROM table(result_scan('${query_id}'))
        `});

        res.next()
        row_count = res.getColumnValue(1);

        batch_size = 40 // limit on their end - 40

        let call_groups = Math.ceil(row_count / batch_size)
        call_groups = 1

        for (i = 0; i < call_groups; i++) {

            var flows_ids = snowflake.execute({sqlText: `
            WITH subset as (
                SELECT *
                FROM table(result_scan('${query_id}'))
                limit ${batch_size } offset ${i * batch_size}
            )
            SELECT ARRAY_AGG(CAST(MOMENT_ID AS INTEGER))
            FROM subset`});

            flows_ids.next()
            row_list = flows_ids.getColumnValue(1);
            
            var create_temp_table_command = `
                INSERT INTO {{ target.database }}.bronze_api.allday_metadata
                WITH api_call AS (
            `;
            
            let query = `{
                searchMomentNFTsV2(input: {filters: {byFlowIDs: [${row_list}]}}) {
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
                SELECT 
                    {{ target.database }}.live.udf_api('GET', CONCAT('https://nflallday.com/consumer/graphql?query=','${query}' ), {'Accept-Encoding': 'gzip', 'Content-Type': 'application/json', 'Accept': 'application/json','Connection': 'keep-alive'},{}) AS res
            `;
            

            create_temp_table_command+= `
            )
            SELECT
                [${row_list}] AS requested_ids,
                res,
                SYSDATE() AS _inserted_timestamp,
                '${nfl_contract}' AS contract
            FROM api_call
            `
            
            snowflake.execute({sqlText: create_temp_table_command});
            // Second command: Insert data into the target table from the temporary table
             
            var log_message = `INSERT INTO {{ target.database }}.bronze_api.log_messages (log_level, message) VALUES ('INFO', ' Iteration ${i} of ${call_groups} complete.')`;
            snowflake.execute({sqlText: log_message});
        }
        return 'Success';

$$;
{% endset %}
{% do run_query(query) %}


{% set sql %}
    CALL {{ target.database }}.bronze_api.allday_metadata();
{% endset %}
    
    {% do run_query(sql) %}

{% endmacro %}