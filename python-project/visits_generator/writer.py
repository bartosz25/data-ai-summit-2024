from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql.streaming import DataStreamWriter


def set_up_sessions_writer(sessions_to_write: DataFrame, trigger: Dict[str, str]) -> DataStreamWriter:
    return (sessions_to_write.writeStream
            .outputMode("append")
            .trigger(**trigger))
