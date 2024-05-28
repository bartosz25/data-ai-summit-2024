import sys

from assertpy import assert_that
from pyspark.sql import DataFrame

from test.assertions import DataToAssertWriterReader
from test.builders import visit, device, technical_context
from test.dataset_writer import DatasetWriter
from visits_generator.sessions_generation_job_logic import generate_sessions


def should_generate_sessions_as_the_watermark_when_watermark_passes_by_with_matching_device(generate_spark_session):
    test_name = sys._getframe().f_code.co_name
    spark_session = generate_spark_session[0]
    assertions_io = DataToAssertWriterReader(spark_session)
    technical_ctx = technical_context(device_type='pc', device_version='0.0.2')
    visit_1 = (visit(visit_id='v1', event_time='2024-01-06T10:00:00.000Z', page='page 2', technical_cxt=technical_ctx))
    visits_writer = DatasetWriter(f'/tmp/visits_{test_name}', spark_session)
    visits_writer.write_dataframe([visit_1.as_kafka_row()])
    devices_to_test = spark_session.createDataFrame([
        device(device_type='pc', version='0.0.1', full_name='PC version 0.0.1-beta').as_row(),
        device(device_type='pc', version='0.0.2', full_name='PC version 0.0.2-beta').as_row()])

    visits_reader: DataFrame = spark_session.readStream.schema('value STRING').json(visits_writer.output_dir)
    visit_writer = generate_sessions(visits_reader, devices_to_test,
                                     {'processingTime': '0 seconds'},
                                     checkpoint_location=visits_writer.checkpoint_dir)  # use 0 to disable the trigger
    started_query = visit_writer.foreachBatch(assertions_io.write_results_to_batch_id_partitioned_storage()).start()
    started_query.processAllAvailable()

    emitted_sessions_0 = assertions_io.get_results_to_assert_for_micro_batch(0)
    assert_that(emitted_sessions_0).is_empty()
    emitted_sessions_1 = assertions_io.get_results_to_assert_for_micro_batch(1)
    assert_that(emitted_sessions_1).is_empty()

    # Next, watermark is at 09:59; we get the data that is far away from it
    # If this happens, the next row expires just after, as the expiration time is based on the watermark
    # and the next micro-batch will clean the expired state rows up (expiration time for v2 will be 10:09 whereas
    # the watermark after processing this row will be 11:55)
    visit_2 = (
        visit(visit_id='v2', event_time='2024-01-06T12:00:00.000Z', page='home_page', technical_cxt=technical_ctx))
    visits_writer.write_dataframe([visit_2.as_kafka_row()])
    started_query.processAllAvailable()

    emitted_sessions_2 = assertions_io.get_results_to_assert_for_micro_batch(2)
    assert_that(emitted_sessions_2).is_empty()
    emitted_sessions_3 = assertions_io.get_results_to_assert_for_the_last_micro_batch()
    assert_that(emitted_sessions_3).is_length(2)
    sorted_emitted_visits_3 = sorted(emitted_sessions_3, key=lambda row: row['visit_id'])
    assert sorted_emitted_visits_3[0] == {'visit_id': 'v1', 'user_id': 'user A id',
                                          'start_time': '2024-01-06T10:00:00.000Z',
                                          'end_time': '2024-01-06T10:00:00.000Z',
                                          'device_full_name': 'PC version 0.0.2-beta',
                                          'device_type': 'pc',
                                          'device_version': '0.0.2',
                                          'visited_pages': [
                                              {'page': 'page 2', 'event_time_as_milliseconds': 1704535200000}]}
    assert sorted_emitted_visits_3[1] == {'visit_id': 'v2', 'user_id': 'user A id',
                                          'start_time': '2024-01-06T12:00:00.000Z',
                                          'end_time': '2024-01-06T12:00:00.000Z',
                                          'device_full_name': 'PC version 0.0.2-beta',
                                          'device_type': 'pc',
                                          'device_version': '0.0.2',
                                          'visited_pages': [
                                              {'page': 'home_page', 'event_time_as_milliseconds': 1704542400000}]}
