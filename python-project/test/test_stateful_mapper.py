from unittest import mock

import pandas as pd
from assertpy import assert_that
from pandas import Timestamp
from pyspark import Row

from test.builders import state_page_visit_row, groupstate_for_test, \
    visited_page_for_stateful_of_user_A
from visits_generator.stateful_mapper import map_visits_to_session
import logging

print('setting log')
logging.basicConfig() #level=logging.DEBUG)
logging.getLogger('root').setLevel(logging.DEBUG)
#logging.getLogger('map_visits_to_session').setLevel(logging.DEBUG)

@mock.patch('pyspark.sql.streaming.state.GroupState')
def oshould_emit_completed_session(current_state):
    visit_id = 'visit-abc'
    current_state.hasTimedOut = True
    current_state.get = ([state_page_visit_row('page 1', 1712725580000),
                          state_page_visit_row('page 2', 1712725700000),
                          state_page_visit_row('page 3', 1712725730000)], 'user A',
                         'pc', 'pc_v_1')

    # wrap to list(...) and evaluate the state
    output_visits = list(map_visits_to_session((visit_id,), [], current_state))

    assert_that(output_visits).is_length(1)
    assert output_visits[0].to_dict(orient='records')[0] == {
        'visit_id': 'visit-abc',
        'start_time': Timestamp('2024-04-10 05:06:20+0000', tz='UTC'),
        'end_time': Timestamp('2024-04-10 05:08:50+0000', tz='UTC'),
        'visited_pages': [Row(page='page 1', event_time_as_milliseconds=1712725580000),
                          Row(page='page 2', event_time_as_milliseconds=1712725700000),
                          Row(page='page 3', event_time_as_milliseconds=1712725730000)],
        'user_id': 'user A',
        'device_type': 'pc',
        'device_version': 'pc_v_1',
        'device_full_name': None
    }


def oshould_create_a_new_session_state_when_there_is_no_watermark():
    visit_id = 'visit-abc'
    current_state = groupstate_for_test(watermark_present=True)
    input_rows = pd.DataFrame.from_dict(data=[
        visited_page_for_stateful_of_user_A('page 1', '2024-01-05T10:00:00.000Z'),
        visited_page_for_stateful_of_user_A('page 2', '2024-01-05T10:01:00.000Z'),
        visited_page_for_stateful_of_user_A('page 3', '2024-01-05T10:01:15.000Z')
    ])

    # list(...) to evaluate
    output_visits = list(map_visits_to_session((visit_id,), [input_rows], current_state))

    assert_that(output_visits).is_empty()
    state_pages, user, device_type, device_version = current_state.get
    assert state_pages == [
        {'event_time_as_milliseconds': 1704448800000, 'page': 'page 1'},
        {'event_time_as_milliseconds': 1704448860000, 'page': 'page 2'},
        {'event_time_as_milliseconds': 1704448875000, 'page': 'page 3'}
    ]
    assert_that(user).is_equal_to('user A')
    assert_that(device_type).is_equal_to('pc')
    assert_that(device_version).is_equal_to('pc_v_1')
    # when there is no watermark, we add the 10 minutes to the last event time
    assert_that(current_state._timeout_timestamp).is_equal_to(1704449475000)


def oshould_create_a_new_session_state_when_there_is_a_watermark_of_2024_01_05___09_50_AM():
    visit_id = 'visit-abc'
    current_state = groupstate_for_test(watermark_present=True, event_time_watermark=1704448200000)
    input_rows = pd.DataFrame.from_dict(data=[
        visited_page_for_stateful_of_user_A('page 1', '2024-01-05T09:51:00.000Z'),
        visited_page_for_stateful_of_user_A('page 2', '2024-01-05T09:51:15.000Z'),
        visited_page_for_stateful_of_user_A('page 3', '2024-01-05T09:55:21.000Z')
    ])

    # list(...) to evaluate
    output_visits = list(map_visits_to_session((visit_id,), [input_rows], current_state))

    assert_that(output_visits).is_empty()
    state_pages, user, device_type, device_version = current_state.get
    assert state_pages == [
        {'event_time_as_milliseconds': 1704448260000, 'page': 'page 1'},
        {'event_time_as_milliseconds': 1704448275000, 'page': 'page 2'},
        {'event_time_as_milliseconds': 1704448521000, 'page': 'page 3'}
    ]
    assert_that(user).is_equal_to('user A')
    assert_that(device_type).is_equal_to('pc')
    assert_that(device_version).is_equal_to('pc_v_1')
    # when there is no watermark, we add the 10 minutes to the watermark (09:50:00 + 10' = 10:00:00)
    assert_that(current_state._timeout_timestamp).is_equal_to(1704448800000)


def should_create_a_new_session_and_restore_it_for_the_next_update_for_a_watermark_of_2024_01_05___09_50_AM():
    visit_id = 'visit-abc'
    current_state = groupstate_for_test(watermark_present=True, event_time_watermark=1704448200000)
    input_rows_1 = pd.DataFrame.from_dict(data=[
        visited_page_for_stateful_of_user_A('page 1', '2024-01-05T09:51:00.000Z'),
        visited_page_for_stateful_of_user_A('page 2', '2024-01-05T09:51:15.000Z'),
        visited_page_for_stateful_of_user_A('page 3', '2024-01-05T09:55:21.000Z')
    ])

    # list(...) to evaluate
    output_visits = list(map_visits_to_session((visit_id,), [input_rows_1], current_state))

    assert_that(output_visits).is_empty()
    state_pages, user, device_type, device_version = current_state.get
    assert state_pages == [{'event_time_as_milliseconds': 1704448260000, 'page': 'page 1'},
                           {'event_time_as_milliseconds': 1704448275000, 'page': 'page 2'},
                           {'event_time_as_milliseconds': 1704448521000, 'page': 'page 3'}]
    assert_that(user).is_equal_to('user A')

    assert_that(device_version).is_equal_to('pc_v_1')
    assert_that(current_state._timeout_timestamp).is_equal_to(1704448800000)

    # new data + evolved watermark to 09:55 AM
    current_state._event_time_watermark_ms = 1704448500000
    input_rows_2 = pd.DataFrame.from_dict(data=[
        visited_page_for_stateful_of_user_A('page 4', '2024-01-05T09:56:00.000Z'),
        visited_page_for_stateful_of_user_A('page 5', '2024-01-05T09:57:15.000Z'),
        visited_page_for_stateful_of_user_A('page 6', '2024-01-05T09:58:00.000Z')
    ])
    output_visits = list(map_visits_to_session((visit_id,), [input_rows_2], current_state))

    assert_that(output_visits).is_empty()
    state_pages, user, device_type, device_version = current_state.get
    assert state_pages == [
        {'event_time_as_milliseconds': 1704448260000, 'page': 'page 1'},
        {'event_time_as_milliseconds': 1704448275000, 'page': 'page 2'},
        {'event_time_as_milliseconds': 1704448521000, 'page': 'page 3'},
        {'event_time_as_milliseconds': 1704448560000, 'page': 'page 4'},
        {'event_time_as_milliseconds': 1704448635000, 'page': 'page 5'},
        {'event_time_as_milliseconds': 1704448680000, 'page': 'page 6'}
    ]
    assert_that(user).is_equal_to('user A')
    assert_that(device_type).is_equal_to('pc')
    assert_that(device_version).is_equal_to('pc_v_1')
    assert_that(current_state._timeout_timestamp).is_equal_to(1704449100000)
