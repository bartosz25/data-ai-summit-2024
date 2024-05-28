from pyspark.sql import functions as F, DataFrame


def select_raw_visits(visits_source: DataFrame) -> DataFrame:
    # the schema doesn't contain all fields; instead it focuses on the required attributes
    # for the session generation
    visit_schema = '''
        visit_id STRING, event_time TIMESTAMP, user_id STRING, page STRING,
        context STRUCT<
            technical STRUCT<
                device_type STRING, device_version STRING
            >
        >
    '''
    visits_raw = (visits_source.select(
        F.from_json(F.col('value').cast('string'), visit_schema).alias('value'))
            .selectExpr('value.*'))

    # Exploding the context columns to make it easier to query later
    return visits_raw.withColumns({'device_type': visits_raw.context.technical.device_type,
                                   'device_version': visits_raw.context.technical.device_version}).drop('context')
