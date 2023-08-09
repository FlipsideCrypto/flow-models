import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F


# NOTE - AllDay endpoint not responsive from anywhere

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


def batch_request(session, base_url, response_schema=None, df=None, api_key=None, params=None):
    """
    Function to call the UDF_API.
    df (optional) - Snowpark DataFrame of input data.
    """

    # define params for UDF_API
    method = 'POST'
    headers = {
        'Content-Type': 'application/json'
    }

    # alias query for readability in construct data param
    query = params

    # register the udf_construct_data function
    udf_construct_data = register_udf_construct_data()

    # use with_columns to source moment_id from the input_df and call multiple udf_api calls at once
    response_df = df.with_columns(
        ['DATA', '_INSERTED_DATE', '_INSERTED_TIMESTAMP', '_RES_ID'],
        [
            F.call_udf(
                # TODO - deploy udf in FLOW and use local udf_api
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

    # TODO - i cant rememebr rn but there was a config i was thinking about
    dbt.config(
        materialized='incremental',
        unique_key='_RES_ID',
        packages=['snowflake-snowpark-python']
    )

    # configure upstream tables
    # limit scope of query for testing w limit 10 and low moment id
    # TODO - when turning into prod job, there will be moments that return null metadata
        # MUST load the null table w these to avoid over-retrying
    # TODO - stress test appropriate batch size
    topshot_moments_needed = dbt.ref(
        'streamline__all_topshot_moments_minted_metadata_needed').limit(200)

    # define incremental logic
    if dbt.is_incremental:
        # TODO - incomplete / placeholder
        # max_from_this = f"select max(_inserted_timestamp) from {dbt.this}"
        pass

    # build df to hold response(s)
    schema = T.StructType(
        [
            T.StructField('EVENT_CONTRACT', T.StringType()),
            T.StructField('MOMENT_ID', T.StringType()),
            T.StructField('DATA', T.VariantType()),
            T.StructField('_INSERTED_DATE', T.TimestampType()),
            T.StructField('_INSERTED_TIMESTAMP', T.StringType()),
            T.StructField('_RES_ID', T.StringType())
        ]
    )

    final_df = session.create_dataframe([], schema)

    # call api via request function
    # base url and graphql query stored in table via dbt
    topshot_params = dbt.ref(
        'silver__lq_moments_graphql').select(
        'base_url', 'query').where(
            F.col(
                'contract') == 'A.0b2a3299cc857e29.TopShot'
        ).collect()


    input_df = topshot_moments_needed

    # TODO - explore possible failure behavior
        # bad url will cause job failure, but that's something that should raise a big red flag
        # need is to handle ok call, bad api response
    r = batch_request(
        session,
        topshot_params[0][0], # base_url
        response_schema=schema,
        df=input_df,
        params=topshot_params[0][1] # query
    )

    r.collect()

    final_df = final_df.union(r)

    return final_df
