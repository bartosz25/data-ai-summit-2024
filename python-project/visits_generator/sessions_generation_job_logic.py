from typing import Dict

from pyspark.sql import functions as F, DataFrame
from pyspark.sql.streaming import DataStreamWriter

from visits_generator.enricher import enrich_sessions_with_devices
from visits_generator.reader import select_raw_visits
from visits_generator.stateful_mapper import map_visits_to_session, get_session_output_schema, get_session_state_schema
from visits_generator.writer import set_up_sessions_writer


def generate_sessions(visits_source: DataFrame, devices: DataFrame, trigger: Dict[str, str],
                      checkpoint_location: str) -> DataStreamWriter:
    raw_visits = select_raw_visits(visits_source)

    grouped_visits = (raw_visits
                      .withWatermark('event_time', '5 minutes')
                      .groupBy(F.col('visit_id')))

    sessions = grouped_visits.applyInPandasWithState(
        func=map_visits_to_session,
        outputStructType=get_session_output_schema(),
        stateStructType=get_session_state_schema(),
        outputMode="append",
        timeoutConf="EventTimeTimeout"
    )

    enriched_sessions = enrich_sessions_with_devices(sessions, devices)

    sessions_to_write = (enriched_sessions
                         .withColumn('value', F.to_json(F.struct('*')))
                         .selectExpr('visit_id AS key', 'value'))

    return set_up_sessions_writer(sessions_to_write, trigger).option('checkpointLocation', checkpoint_location)
