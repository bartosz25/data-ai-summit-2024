from assertpy import assert_that
from pyspark import Row

from test.builders import common_devices, visit, device, session
from visits_generator.enricher import enrich_sessions_with_devices


def should_keep_the_session_even_when_there_is_no_matching_device(generate_spark_session):
    spark_session = generate_spark_session[0]
    devices_to_test = spark_session.createDataFrame([device(device_type='mac').as_row()])
    tested_session = session(device_type='pc')
    sessions_to_test = spark_session.createDataFrame(
        [tested_session.as_row()]
    )

    enriched_sessions = enrich_sessions_with_devices(sessions_to_test, devices_to_test).collect()

    assert_that(enriched_sessions).is_length(sessions_to_test.count())
    assert enriched_sessions[0] == tested_session.as_row(overrides={'device_full_name': lambda x: None})


def should_combine_visit_with_the_matching_device(generate_spark_session):
    spark_session = generate_spark_session[0]
    device_1, device_2 = common_devices()
    devices_to_test = spark_session.createDataFrame([device_1.as_row(), device_2.as_row()])
    tested_session = session(device_type=device_1.type, device_version=device_1.version)
    sessions_to_test = spark_session.createDataFrame(
        [tested_session.as_row()]
    )

    enriched_sessions = enrich_sessions_with_devices(sessions_to_test, devices_to_test).collect()

    assert_that(enriched_sessions).is_length(sessions_to_test.count())
    assert enriched_sessions[0] == tested_session.as_row(overrides={'device_full_name': lambda x: device_1.full_name})
