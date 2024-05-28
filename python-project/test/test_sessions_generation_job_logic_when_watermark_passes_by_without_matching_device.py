import sys

from assertpy import assert_that
from pyspark.sql import DataFrame

from test.assertions import DataToAssertWriterReader
from test.builders import visit, device
from test.dataset_writer import DatasetWriter
from visits_generator.sessions_generation_job_logic import generate_sessions


def should_generate_sessions_as_the_watermark_when_watermark_passes_by_without_matching_device(generate_spark_session):
    test_name = sys._getframe().f_code.co_name
    spark_session = generate_spark_session[0]
    assertions_io = DataToAssertWriterReader(spark_session)
    visit_1 = (visit(visit_id='v1', event_time='2024-01-05T10:00:00.000Z', page='page 2'))
    visits_writer = DatasetWriter(f'/tmp/visits_{test_name}', spark_session)
    visits_writer.write_dataframe([visit_1.as_kafka_row()])
    devices_to_test = spark_session.createDataFrame([
        device(device_type='pc', version='0.0.1', full_name='PC version 0.0.1-beta').as_row(),
        device(device_type='pc', version='0.0.2', full_name='PC version 0.0.2-beta').as_row()])

    visits_reader: DataFrame = spark_session.readStream.schema('value STRING').json(visits_writer.output_dir)
    visit_writer = (generate_sessions(visits_reader, devices_to_test,
                                      {'processingTime': '0 seconds'},
                                      checkpoint_location=visits_writer.checkpoint_dir))  # use 0 to disable the trigger
    started_query = visit_writer.foreachBatch(assertions_io.write_results_to_batch_id_partitioned_storage()).start()
    started_query.processAllAvailable()

    emitted_visits_0 = assertions_io.get_results_to_assert_for_micro_batch(0)
    assert_that(emitted_visits_0).is_empty()
    emitted_visits_1 = assertions_io.get_results_to_assert_for_micro_batch(1)
    assert_that(emitted_visits_1).is_empty()

    # Next
    visit_2, visit_3, visit_4 = (visit(visit_id='v1', event_time='2024-01-05T10:01:00.000Z', page='page 3'),
                                 visit(visit_id='v1', event_time='2024-01-05T10:03:00.000Z', page='page 4'),
                                 visit(visit_id='v2', event_time='2024-01-05T10:00:00.000Z', user_id=None,
                                       page='home_page'))
    visits_writer.write_dataframe([visit_2.as_kafka_row(), visit_3.as_kafka_row(), visit_4.as_kafka_row()])
    started_query.processAllAvailable()

    emitted_visits_2 = assertions_io.get_results_to_assert_for_micro_batch(2)
    assert_that(emitted_visits_2).is_empty()
    emitted_visits_3 = assertions_io.get_results_to_assert_for_micro_batch(3)
    assert_that(emitted_visits_3).is_empty()

    # Next...
    visit_5 = (visit(visit_id='v3', event_time='2024-01-05T10:13:00.000Z', page='page 5'))
    visits_writer.write_dataframe([visit_5.as_kafka_row(), ])
    started_query.processAllAvailable()

    emitted_visits_4 = assertions_io.get_results_to_assert_for_micro_batch(4)
    assert_that(emitted_visits_4).is_empty()
    emitted_visits_5 = assertions_io.get_results_to_assert_for_the_last_micro_batch()
    assert_that(emitted_visits_5).is_not_empty()
    assert_that(emitted_visits_5).is_length(2)
    sorted_emitted_visits_5 = sorted(emitted_visits_5, key=lambda row: row['visit_id'])
    assert sorted_emitted_visits_5[0] == {'visit_id': 'v1', 'user_id': 'user A id',
                                          'start_time': '2024-01-05T10:00:00.000Z',
                                          'end_time': '2024-01-05T10:03:00.000Z',
                                          'device_type': 'pc',
                                          'device_version': '1.2.3',
                                          'visited_pages': [
                                              {'page': 'page 2', 'event_time_as_milliseconds': 1704448800000},
                                              {'page': 'page 3', 'event_time_as_milliseconds': 1704448860000},
                                              {'page': 'page 4', 'event_time_as_milliseconds': 1704448980000}]}
    assert sorted_emitted_visits_5[1] == {'visit_id': 'v2',
                                          'start_time': '2024-01-05T10:00:00.000Z',
                                          'end_time': '2024-01-05T10:00:00.000Z', 'device_type': 'pc',
                                          'device_version': '1.2.3',
                                          'visited_pages': [
                                              {'page': 'home_page', 'event_time_as_milliseconds': 1704448800000}]}
