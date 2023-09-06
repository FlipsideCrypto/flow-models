import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F


def register_udf_construct_data():
    """
    Helper function to register a named UDF to construct the DATA object for the API call.
    This named UDF can be used with a column expression, so multiple moment_ids can be called at the same time.
    """

    udf_construct_data = (
        F.udf(
            lambda query, moment_id: {'query': query,
                                      'variables': {'momentId': moment_id}},
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


def model(dbt, session):

    dbt.config(
        materialized='incremental',
        unique_key='_RES_ID',
        packages=['snowflake-snowpark-python'],
        tags=['livequery', 'topshot', 'moment_metadata'],
        incremental_strategy='delete+insert'
    )

    # define incremental logic
    if dbt.is_incremental:
        # TODO - incomplete / placeholder
        # max_from_this = f"select max(_inserted_timestamp) from {dbt.this}"
        pass

    # define response / table schema - NOTE not needed bc appending cols via with_columns
    # schema = T.StructType(
    #     [
    #         T.StructField('EVENT_CONTRACT', T.StringType()),
    #         T.StructField('MOMENT_ID', T.StringType()),
    #         T.StructField('DATA', T.VariantType()),
    #         T.StructField('_INSERTED_DATE', T.TimestampType()),
    #         T.StructField('_INSERTED_TIMESTAMP', T.StringType()),
    #         T.StructField('_RES_ID', T.StringType())
    #     ]
    # )

    # base url and graphql query stored in table via dbt
    topshot_gql_params = dbt.ref(
        'livequery__moments_parameters').select(
        'base_url', 'query').where(
            F.col(
                'contract') == 'A.0b2a3299cc857e29.TopShot'
        ).collect()

    # define params for UDF_API
    method = 'POST'
    headers = {
        'Content-Type': 'application/json'
    }
    url = topshot_gql_params[0][0]
    
    # gql query passed with the post request
    data = topshot_gql_params[0][1] 
    
    # metadata request requires moment_id, defined in a separate view 
    # TODO - when turning into prod job, there will be moments that return null metadata
        # MUST load the null table w these to avoid over-retrying
    inputs = dbt.ref(
        'livequery__topshot_moments_metadata_needed').select(
            "EVENT_CONTRACT", "MOMENT_ID"
        ).limit(5) # TODO - incr to 2500 for prod

    # register the udf_construct_data function
    udf_construct_data = register_udf_construct_data()

    # use with_columns to source moment_id from the input_df and call multiple udf_api calls at once
    # columns defined in the array will be appended to the input dataframe
    response = inputs.with_columns(
        ['DATA', '_INSERTED_DATE', '_INSERTED_TIMESTAMP', '_RES_ID'],
        [
            F.call_udf(
                'flow.streamline.udf_api',
                method,
                url,
                headers,
                udf_construct_data(
                    F.lit(data),
                    F.col('MOMENT_ID')
                ),
                F.lit(None), # USER_ID req on Flow deployment of UDF_API
                F.lit(None) # SECRET_NAME req on Flow deployment of UDF_API
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

    return response
