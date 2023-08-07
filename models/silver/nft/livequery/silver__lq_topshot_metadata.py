import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F
from snowflake.snowpark.window import Window
from datetime import datetime


def register_udf_construct_data():
    """
    Helper function to register an anonymous UDF to construct the DATA object for the API call.
    """

    udf_construct_data = (
        F.udf(
            lambda query, moment_id: {'query': query, 'variables': {'momentId': moment_id}},
            name='udf_construct_data', 
            input_types=[
                T.StringType(), 
                T.StringType()
            ], 
            return_type=T.VariantType(),
            replace=True
        )
    )

    return udf_construct_data

def batch_request(session, base_url, response_schema, df=None, api_key=None):
    """
    Function to call the UDF_API.
    df (optional) - Snowpark DataFrame of input data
    """

    # define params for UDF_API
    method = 'POST'
    headers = {
        'Content-Type': 'application/json'
    }

    query = """query getMintedMoment ($momentId: ID!) {
                getMintedMoment (momentId: $momentId) {
                    data {
                        id
                        version
                        sortID
                        set {
                            id
                            sortID
                            version
                            flowId
                            flowName
                            flowSeriesNumber
                            flowLocked
                            setVisualId
                            assetPath
                            assets {
                                images {
                                    type
                                    url
                                }
                            }
                        }
                        play {
                            id
                            version
                            description
                            flowID
                            sortID
                            status
                            assets {
                                videos {
                                    type
                                    url
                                    videoLength
                                }
                                videoLengthInMilliseconds
                            }
                            stats {
                                playerID
                                playerName
                                firstName
                                lastName
                                jerseyNumber
                                teamAtMoment
                                awayTeamName
                                awayTeamScore
                                homeTeamName
                                homeTeamScore
                                dateOfMoment
                                totalYearsExperience
                                teamAtMomentNbaId
                                height
                                weight
                                currentTeam
                                currentTeamId
                                primaryPosition
                                homeTeamNbaId
                                awayTeamNbaId
                                nbaSeason
                                draftYear
                                draftSelection
                                draftRound
                                birthplace
                                birthdate
                                draftTeam
                                draftTeamNbaId
                                playCategory
                                playType
                                quarter
                            }
                            statsPlayerGameScores {
                                blocks
                                points
                                steals
                                assists
                                minutes
                                rebounds
                                turnovers
                                plusMinus
                                flagrantFouls
                                personalFouls
                                technicalFouls
                                twoPointsMade
                                blockedAttempts
                                fieldGoalsMade
                                freeThrowsMade
                                threePointsMade
                                defensiveRebounds
                                offensiveRebounds
                                pointsOffTurnovers
                                twoPointsAttempted
                                assistTurnoverRatio
                                fieldGoalsAttempted
                                freeThrowsAttempted
                                twoPointsPercentage
                                fieldGoalsPercentage
                                freeThrowsPercentage
                                threePointsAttempted
                                threePointsPercentage
                                playerPosition
                            }
                            statsPlayerSeasonAverageScores {
                                minutes
                                blocks
                                points
                                steals
                                assists
                                rebounds
                                turnovers
                                plusMinus
                                flagrantFouls
                                personalFouls
                                technicalFouls
                                twoPointsMade
                                blockedAttempts
                                fieldGoalsMade
                                freeThrowsMade
                                threePointsMade
                                defensiveRebounds
                                offensiveRebounds
                                pointsOffTurnovers
                                twoPointsAttempted
                                assistTurnoverRatio
                                fieldGoalsAttempted
                                freeThrowsAttempted
                                twoPointsPercentage
                                fieldGoalsPercentage
                                freeThrowsPercentage
                                threePointsAttempted
                                threePointsPercentage
                                efficiency
                                true_shooting_attempts
                                points_in_paint_made
                                points_in_paint_attempted
                                points_in_paint
                                fouls_drawn
                                offensive_fouls
                                fast_break_points
                                fast_break_points_attempted
                                fast_break_points_made
                                second_chance_points
                                second_chance_points_attempted
                                second_chance_points_made
                            }
                            tags {
                                id
                                name
                                title
                                visible
                                hardcourt
                                level
                            }
                        }
                        flowId
                        flowSerialNumber
                        price
                        forSale
                        listingOrderID
                        owner {
                            dapperID
                            email
                            flowAddress
                            username
                            profileImageUrl
                            twitterHandle
                            segmentID
                        }
                        assetPathPrefix
                        setPlay {
                            ID
                            setID
                            playID
                            flowRetired
                            circulationCount
                            tags {
                                id
                                name
                                title
                                visible
                                hardcourt
                                level
                            }
                        }
                        createdAt
                        acquiredAt
                        packListingID
                        tags {
                            id
                            name
                            title
                            visible
                            hardcourt
                            level
                        }
                    }
                }
            }"""


    udf_construct_data = register_udf_construct_data()

    # turns out the below is not working as expected as the MOMENT_ID 
    # from the row being appended is not the same id as the one returned above
    # but maybe i can build a construct data udf
    response_df = df.with_columns(
        ['DATA', '_INSERTED_DATE', 'INSTERTED_TIMESTAMP', '_RES_ID'],
        [
            F.call_udf(
                'ethereum.streamline.udf_api',
                method,
                base_url,
                headers,
                udf_construct_data(
                    F.lit(query),
                    F.col('MOMENT_ID')
                )
            ),
            F.sysdate().cast(T.DateType()),
            F.sysdate(),
            F.md5(
                F.concat(
                F.col('EVENT_CONTRACT'),
                F.col('MOMENT_ID')
                )
            )
        ]
    )

    return response_df


def model(dbt, session):

    dbt.config(
        materialized='incremental',
        unique_key='_RES_ID',
        packages=['snowflake-snowpark-python']
    )

    # configure upstream tables
    # limit scope of query for testing w limit 10
    topshot_moments_needed = dbt.ref('streamline__all_topshot_moments_minted_metadata_needed').where(F.col("MOMENT_ID") < 999999).limit(2)

    # define incremental logic
    if dbt.is_incremental:
        # TODO - incomplete / placeholder
        # max_from_this = f"select max(_inserted_timestamp) from {dbt.this}"
        pass

    # build df to hold response(s)
    schema = T.StructType([
        T.StructField('EVENT_CONTRACT', T.StringType()),
        T.StructField('MOMENT_ID', T.StringType()),
        T.StructField('DATA', T.VariantType()),
        T.StructField('_INSERTED_DATE', T.TimestampType()),
        T.StructField('_INSERTED_TIMESTAMP', T.StringType()),
        T.StructField('_RES_ID', T.StringType())
    ])

    final_df = session.create_dataframe([], schema)

    # call api via request function(s)
    # TODO - only slight differences between topshot and allday, allow for param input ?
    # base_url = 'https://nbatopshot.com/marketplace/graphql'
    base_url = 'https://public-api.nbatopshot.com/graphql'
    input_df = topshot_moments_needed


    # batch_size = 25
    # ignoring any batch size for now, just want to get it working
    # ignoring try request block for testing
    # try:
    r = batch_request(
        session,
        base_url,
        schema,
        input_df
    )

    r.collect()

    final_df = final_df.union(r)
    
    # except Exception as e:
    #     # TODO - log error
    #     raise Exception(e)

    return final_df
