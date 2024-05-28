import shutil
from os import path
from typing import List

from pyspark import Row
from pyspark.sql import SparkSession


class DatasetWriter:

    def __init__(self, output_dir: str, spark_session: SparkSession):
        print(f'>>> Initializing {output_dir}')
        self._output_dir = output_dir
        self.spark_session = spark_session

        if path.isdir(self.output_dir):
            shutil.rmtree(self.output_dir)

    @property
    def output_dir(self):
        return self._output_dir

    @property
    def checkpoint_dir(self):
        return f'{self.output_dir}/checkpoint'

    def write_dataframe(self, data: List[Row]):
        self.spark_session.createDataFrame(data).write.mode('append').json(self.output_dir)
