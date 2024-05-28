import json
import logging
from typing import List, Any, Dict

from pyspark.sql import DataFrame, SparkSession


class DataToAssertWriterReader:

    def __init__(self, spark_session: SparkSession):
        self.spark_session = spark_session
        self._results_per_micro_batch: Dict[int, List[Dict[str, Any]]] = {}

    def write_results_to_batch_id_partitioned_storage(self):
        def write_to_test_output_dir(dataframe: DataFrame, batch_number: int):
            rows = dataframe.collect()
            results = []
            for row in rows:
                results.append(json.loads(row.value))
            logging.info(f'Setting {len(results)} for {batch_number}')
            self._results_per_micro_batch[batch_number] = results

        return write_to_test_output_dir

    def get_results_to_assert_for_micro_batch(self, batch_number: int) -> List[Dict[str, Any]]:
        return self._results_per_micro_batch[batch_number]

    def get_results_to_assert_for_the_last_micro_batch(self) -> List[Dict[str, Any]]:
        """
        :return: Results for the last micro-batch. We're relying on it because sometimes Spark executed two queries
                 instead of one, where only the last one does the cleaning. Hence, instead of getting
                 two queries after each processAllAvailable call, we get three.
                 It looks like a weird race condition between the processAllAvailable() and the state cleaning process.

                 As a result, using the direct number-based fetch, leads to flaky tests.
        """
        return self.get_results_to_assert_for_micro_batch(len(self._results_per_micro_batch) - 1)
