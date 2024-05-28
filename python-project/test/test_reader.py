from datetime import datetime
from typing import List

from assertpy import assert_that
from pyspark import Row

from test.builders import visit
from visits_generator.reader import select_raw_visits


def should_extract_struct_to_top_level_columns(generate_spark_session):
    spark_session = generate_spark_session[0]
    visit_1, visit_2, visit_3, visit_4 = (visit(visit_id='v1', event_time='2024-01-05T10:00:00.000Z', page='page 2'),
                                          visit(visit_id='v1', event_time='2024-01-05T10:01:00.000Z', page='page 3'),
                                          visit(visit_id='v1', event_time='2024-01-05T10:03:00.000Z', page='page 4'),
                                          visit(visit_id='v2', event_time='2024-01-05T10:00:00.000Z', user_id=None,
                                                page='home_page'))
    visits_to_test_raw = [visit_1.as_kafka_row(),
                          visit_2.as_kafka_row(),
                          visit_3.as_kafka_row(),
                          visit_4.as_kafka_row(),
                          Row(value='{...}', )
                          ]
    visits_to_test = spark_session.createDataFrame(visits_to_test_raw)

    raw_visits: List[Row] = select_raw_visits(visits_to_test).orderBy(['visit_id', 'event_time']).collect()

    assert_that(raw_visits).is_length(len(visits_to_test_raw))
    overrides = {'event_time': lambda event_time_str: datetime.strptime(event_time_str, '%Y-%m-%dT%H:%M:%S.%fZ')}
    ignored_fields = set(['context'])
    assert raw_visits[0] == Row(visit_id=None, event_time=None, user_id=None, page=None, device_type=None,
                                device_version=None)
    assert raw_visits[1] == visit_1.as_row(overrides=overrides, ignored_fields=ignored_fields,
                                           extra_fields={'device_type': visit_1.context.technical.device_type,
                                                         'device_version': visit_1.context.technical.device_version})
    assert raw_visits[2] == visit_2.as_row(overrides=overrides, ignored_fields=ignored_fields,
                                           extra_fields={'device_type': visit_2.context.technical.device_type,
                                                         'device_version': visit_2.context.technical.device_version})

    assert raw_visits[3] == visit_3.as_row(overrides=overrides, ignored_fields=ignored_fields,
                                           extra_fields={'device_type': visit_3.context.technical.device_type,
                                                         'device_version': visit_3.context.technical.device_version})

    assert raw_visits[4] == visit_4.as_row(overrides=overrides, ignored_fields=ignored_fields,
                                           extra_fields={'device_type': visit_4.context.technical.device_type,
                                                         'device_version': visit_4.context.technical.device_version})
