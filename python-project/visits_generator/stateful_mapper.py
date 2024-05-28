import datetime
import logging
from typing import Any, Dict, Iterable, List, Tuple, Optional

import pandas
from pyspark.sql.streaming.state import GroupState
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, LongType, ArrayType


def get_visited_page_schema() -> ArrayType:
    return (ArrayType(StructType([
        StructField("page", StringType()),
        StructField("event_time_as_milliseconds", LongType())
    ])))


def get_session_output_schema() -> StructType:
    return (StructType([
        StructField("visit_id", StringType()),
        StructField("user_id", StringType()),
        StructField("start_time", TimestampType()),
        StructField("end_time", TimestampType()),
        StructField("visited_pages", get_visited_page_schema()),
        StructField("device_type", StringType()),
        StructField("device_version", StringType()),
        # Even though the device_full_name is not set here, it's required
        # Otherwise the join fails with a mysterious error:
        # Lost task 1.0 in stage 16.0 (TID 63) (192.168.1.55 executor driver): java.lang.ClassCastException: org.apache.spark.sql.vectorized.ColumnarBatchRow cannot be cast to org.apache.spark.sql.catalyst.expressions.UnsafeRow
        # 	at org.apache.spark.sql.execution.UnsafeRowSerializerInstance$$anon$1.writeValue(UnsafeRowSerializer.scala:64)
        # 	at org.apache.spark.storage.DiskBlockObjectWriter.write(DiskBlockObjectWriter.scala:312)
        # 	at org.apache.spark.shuffle.sort.BypassMergeSortShuffleWriter.write(BypassMergeSortShuffleWriter.java:171)
        StructField("device_full_name", StringType()),
    ]))


def get_session_state_schema() -> StructType:
    return (StructType([
        StructField("visits", get_visited_page_schema()),
        StructField("user_id", StringType()),
        StructField("device_type", StringType()),
        StructField("device_version", StringType()),
    ]))


def get_session_to_return(visit_id: str, visited_pages: List[Tuple[str, str]], user_id: str,
                          device_type: str, device_version: str) -> Dict[str, Any]:
    sorted_visits = sorted(visited_pages,
                           key=lambda event_time_with_page: event_time_with_page.event_time_as_milliseconds)
    first_event_time_as_milliseconds = sorted_visits[0].event_time_as_milliseconds
    last_event_time_as_milliseconds = sorted_visits[len(sorted_visits) - 1].event_time_as_milliseconds
    # Using the [...] is required to avoid
    # "ValueError: If using all scalar values, you must pass an index" error
    return {
        "visit_id": [visit_id],
        "start_time": [datetime.datetime.fromtimestamp(first_event_time_as_milliseconds / 1000.0,
                                                       tz=datetime.timezone.utc)],
        "end_time": [datetime.datetime.fromtimestamp(last_event_time_as_milliseconds / 1000.0,
                                                     tz=datetime.timezone.utc)],
        "visited_pages": [sorted_visits],
        "user_id": [user_id],
        "device_type": [device_type],
        "device_version": [device_version],
        "device_full_name": [None]
    }


def map_visits_to_session(visit_id_tuple: Any,
                          input_rows: Iterable[pandas.DataFrame],
                          current_state: GroupState) -> Iterable[pandas.DataFrame]:
    mapper_logger = logging.getLogger('map_visits_to_session')
    session_expiration_time_10min_as_ms = 10 * 60 * 1000
    visit_id = visit_id_tuple[0]

    visit_to_return = None
    if current_state.hasTimedOut:
        mapper_logger.info(f"Session ({current_state.get}) expired for {visit_id}; let's generate the final output here")
        visits, user_id, device_type, device_version, = current_state.get
        visit_to_return = get_session_to_return(visit_id, visits, user_id, device_type, device_version)
        current_state.remove()
    else:
        new_visits = []
        new_user_id: Optional[str] = None
        new_device_type: Optional[str] = None
        new_device_version: Optional[str] = None
        base_event_time_in_ms_for_state_expiration = current_state.getCurrentWatermarkMs()
        for input_df_for_group in input_rows:
            input_df_for_group['event_time_as_milliseconds'] = input_df_for_group['event_time'] \
                .apply(lambda x: int(pandas.Timestamp(x).timestamp()) * 1000)

            if current_state.getCurrentWatermarkMs() == 0:
                max_event_time_for_group = int(input_df_for_group['event_time_as_milliseconds'].max())
                base_event_time_in_ms_for_state_expiration = max(base_event_time_in_ms_for_state_expiration,
                                                                 max_event_time_for_group)

            new_visits = input_df_for_group[['event_time_as_milliseconds', 'page']].to_dict(orient='records')
            new_user_id = input_df_for_group.at[0, 'user_id']
            new_device_type = input_df_for_group.at[0, 'device_type']
            new_device_version = input_df_for_group.at[0, 'device_version']

        visits_so_far = []
        if current_state.exists:
            visits_so_far, _, _, _ = current_state.get

        visits_for_state = visits_so_far + new_visits
        current_state.update((visits_for_state, new_user_id, new_device_type, new_device_version, ))

        timeout_timestamp = base_event_time_in_ms_for_state_expiration + session_expiration_time_10min_as_ms
        current_state.setTimeoutTimestamp(timeout_timestamp)
        mapper_logger.info(f'Updating {visits_for_state} with expiration time = {timeout_timestamp}')

    if visit_to_return:
        yield pandas.DataFrame(visit_to_return)
