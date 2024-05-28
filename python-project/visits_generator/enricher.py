from pyspark.sql import DataFrame, functions


def enrich_sessions_with_devices(sessions: DataFrame, devices: DataFrame) -> DataFrame:
    enriched_visits = ((sessions.join(
        devices,
        [sessions.device_type == devices.type,
         sessions.device_version == devices.version],
        'left_outer')).drop('type', 'version', 'device_full_name')
                       .withColumnRenamed('full_name', 'device_full_name'))

    return enriched_visits
