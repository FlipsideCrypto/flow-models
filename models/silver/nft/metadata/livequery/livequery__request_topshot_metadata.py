import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F


def register_udf_construct_data():
    """
    Helper function to register a named UDF to construct the DATA object for the API call.
    This named UDF can be used with a column expression, so multiple moment_ids can be called at the same time.
    """

    udf_construct_data = F.udf(
        lambda query, moment_id: {"query": query, "variables": {"momentId": moment_id}},
        name="udf_construct_data",
        input_types=[T.StringType(), T.StringType()],
        return_type=T.VariantType(),
        replace=True,
    )

    return udf_construct_data


def model(dbt, session):
    """
    This model will call the TopShot GraphQL API to request metadata for a list of moment_ids, determined by an exeternally defined view.
    The request arguments are a GraphQL query and moment ID. The gql and API URL are stored in a table and retrieved in this workflow.
    """

    dbt.config(
        materialized="incremental",
        unique_key="_RES_ID",
        packages=["snowflake-snowpark-python"],
        tags=["livequery", "topshot", "moment_metadata"],
        incremental_strategy="delete+insert",
        cluster_by=["_INSERTED_TIMESTAMP"],
    )

    # base url and graphql query stored in table via dbt
    topshot_gql_params = (
        dbt.ref("livequery__moments_parameters")
        .select("base_url", "query")
        .where(F.col("contract") == "A.0b2a3299cc857e29.TopShot")
        .collect()
    )

    # define params for UDF_API
    method = "POST"
    headers = {"Content-Type": "application/json"}
    url = topshot_gql_params[0][0]

    # gql query passed with the post request
    data = topshot_gql_params[0][1]

    # metadata request requires moment_id, defined in a separate view
    # number of moment_ids to request set by .limit(), timeout experienced at 4000

    inputs = (
        dbt.ref("livequery__topshot_moments_metadata_needed")
        .select("EVENT_CONTRACT", "MOMENT_ID")
        .limit(1000)
    )

    # register the udf_construct_data function
    udf_construct_data = register_udf_construct_data()

    try:
        requests = F.call_udf(
            "flow.live.udf_api",
            method,
            url,
            headers,
            udf_construct_data(F.lit(data), F.col("MOMENT_ID")),
        )

    except:
        requests = F.lit({"error": F.col("MOMENT_ID")})

    # use with_columns to source moment_id from the input_df and call multiple udf_api calls at once
    # columns defined in the array will be appended to the input dataframe
    response = inputs.with_columns(
        ["DATA", "_INSERTED_DATE", "_INSERTED_TIMESTAMP", "_RES_ID"],
        [
            requests,
            F.sysdate().cast(T.DateType()),
            F.sysdate(),
            F.md5(F.concat(F.col("EVENT_CONTRACT"), F.col("MOMENT_ID"))),
        ],
    )

    # dbt will append response to table per incremental config
    return response
