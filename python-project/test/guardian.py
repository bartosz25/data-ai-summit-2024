import os
from typing import List

import great_expectations as gx
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.data_context import FileDataContext

from test.models import Visit, Device, WithRowConverter
from test.spark_session_builder import spark_session_for_tests


class DataQualityValidator:

    def __init__(self):
        self.context: FileDataContext = gx.get_context(
            context_root_dir=f'{os.getcwd()}/great_expectations_spec')
        self.spark_session = spark_session_for_tests()

    def validate_visits(self, visits_to_validate: List[Visit]) -> CheckpointResult:
        return self._validate_dataset('visits_expectations', visits_to_validate)

    def validate_devices(self, devices_to_validate: List[Device]) -> CheckpointResult:
        return self._validate_dataset('devices_expectations', devices_to_validate)

    def _validate_dataset(self, expectations: str, items_to_validate: List[WithRowConverter]) -> CheckpointResult:
        items_as_rows = list(map(lambda item: item.as_row(), items_to_validate))
        dataframe_to_test = self.spark_session.createDataFrame(items_as_rows)
        datasource = self.context.sources.add_or_update_spark(name=f'{expectations}_data_source')
        data_asset = datasource.add_dataframe_asset(name=f'{expectations}_data_asset')

        batch_request = data_asset.build_batch_request(dataframe=dataframe_to_test)

        checkpoint = self.context.add_or_update_checkpoint(
            name=f'{expectations}_checkpoint',
            validations=[
                {
                    "batch_request": batch_request,
                    "expectation_suite_name": expectations,
                },
            ],
        )
        checkpoint_result: CheckpointResult = checkpoint.run()

        return checkpoint_result
