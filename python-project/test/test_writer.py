import sys

from assertpy import assert_that
from pyspark.sql import DataFrame, functions as F

from test.assertions import DataToAssertWriterReader
from test.builders import visit
from visits_generator.writer import set_up_sessions_writer


def should_emit_only_complete_results(generate_spark_session):
    spark_session = generate_spark_session[0]
    assertions_io = DataToAssertWriterReader(spark_session)
    visits_output_dir = '/tmp/visits'
    (spark_session.createDataFrame([(visit(event_time='2024-01-05T10:00:00.000Z'))])
     .write.mode('overwrite').json(visits_output_dir))
    visits_reader: DataFrame = spark_session.readStream.schema('event_time TIMESTAMP').json(visits_output_dir)
    visits_windowed = (visits_reader
                       .withWatermark('event_time', '20 minutes')
                       .groupBy(F.window('event_time', '10 minutes')).count()
                       .selectExpr('TO_JSON(STRUCT(*)) AS value'))

    visit_writer = set_up_sessions_writer(visits_windowed, {'processingTime': '0 seconds'})
    started_query = visit_writer.foreachBatch(assertions_io.write_results_to_batch_id_partitioned_storage()).start()
    started_query.processAllAvailable()

    emitted_visits_0 = assertions_io.get_results_to_assert_for_micro_batch(0)
    assert_that(emitted_visits_0).is_empty()
    emitted_visits_1 = assertions_io.get_results_to_assert_for_micro_batch(1)
    assert_that(emitted_visits_1).is_empty()

    (spark_session.createDataFrame([(visit(event_time='2024-01-05T10:50:00.000Z'))])
     .write.mode('overwrite').json(visits_output_dir))
    started_query.processAllAvailable()

    emitted_visits_2 = assertions_io.get_results_to_assert_for_micro_batch(2)
    assert_that(emitted_visits_2).is_empty()
    emitted_visits_3 = assertions_io.get_results_to_assert_for_the_last_micro_batch()
    assert_that(emitted_visits_3).is_length(1)
    assert (emitted_visits_3[0] ==
            {'window': {'start': '2024-01-05T10:00:00.000Z', 'end': '2024-01-05T10:10:00.000Z'}, 'count': 1})
