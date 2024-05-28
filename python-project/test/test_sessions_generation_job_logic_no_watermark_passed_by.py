import sys

from assertpy import assert_that
from pyspark.sql import DataFrame

from test.assertions import DataToAssertWriterReader
from test.builders import visit, common_devices
from test.dataset_writer import DatasetWriter
from visits_generator.sessions_generation_job_logic import generate_sessions


def should_not_generate_sessions_as_the_watermark_did_not_pass_by(generate_spark_session):
    test_name = sys._getframe().f_code.co_name
    spark_session = generate_spark_session[0]
    assertions_io = DataToAssertWriterReader(spark_session)
    device_1, device_2 = common_devices()
    devices_to_test = spark_session.createDataFrame([device_1.as_row(), device_2.as_row()])
    visits_writer = DatasetWriter(f'/tmp/visits_{test_name}', spark_session)
    visit_1, visit_2, visit_3, visit_4 = (visit(visit_id='v1', event_time='2024-01-04T10:00:00.000Z', page='page 2'),
                                          visit(visit_id='v1', event_time='2024-01-04T10:01:00.000Z', page='page 3'),
                                          visit(visit_id='v1', event_time='2024-01-04T10:03:00.000Z', page='page 4'),
                                          visit(visit_id='v2', event_time='2024-01-04T10:00:00.000Z', user_id=None,
                                                page='home_page'))
    visits_writer.write_dataframe([visit_1.as_kafka_row(), visit_2.as_kafka_row(), visit_3.as_kafka_row(),
                                   visit_4.as_kafka_row()])

    visits_reader: DataFrame = spark_session.readStream.schema('value STRING').json(visits_writer.output_dir)
    sessions_writer = generate_sessions(visits_reader, devices_to_test, {'processingTime': '0 seconds'},
                                        checkpoint_location=visits_writer.checkpoint_dir)
    started_query = sessions_writer.foreachBatch(assertions_io.write_results_to_batch_id_partitioned_storage()).start()
    started_query.processAllAvailable()

    emitted_sessions_0 = assertions_io.get_results_to_assert_for_micro_batch(0)
    assert_that(emitted_sessions_0).is_empty()
    emitted_sessions_1 = assertions_io.get_results_to_assert_for_micro_batch(1)
    assert_that(emitted_sessions_1).is_empty()

    # Next...
    visit_1, visit_2, visit_3 = (visit(visit_id='v1', event_time='2024-01-04T10:04:00.000Z', page='page 5'),
                                 visit(visit_id='v1', event_time='2024-01-04T10:05:00.000Z', page='page 6'),
                                 visit(visit_id='v1', event_time='2024-01-04T10:07:00.000Z', page='page 7'))
    visits_writer.write_dataframe([visit_1.as_kafka_row(), visit_2.as_kafka_row(), visit_3.as_kafka_row()])
    started_query.processAllAvailable()

    emitted_sessions_2 = assertions_io.get_results_to_assert_for_micro_batch(2)
    assert_that(emitted_sessions_2).is_empty()
    emitted_sessions_3 = assertions_io.get_results_to_assert_for_the_last_micro_batch()
    assert_that(emitted_sessions_3).is_empty()
