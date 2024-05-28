import sys

from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.core import ExpectationSuite, ExpectationConfiguration
from great_expectations.core.batch import BatchRequest
import pandas as pd

import great_expectations as gx

context_root_dir = '/gx-2'

context = gx.data_context.FileDataContext.create(context_root_dir)


devices_expectations_name = 'devices_expectations'
devices_suite: ExpectationSuite = context.add_or_update_expectation_suite(
    devices_expectations_name)

expectation_configuration = ExpectationConfiguration(
    expectation_type="expect_table_columns_to_match_set",
    kwargs={"column_set": ["type", "version", "full_name"], "exact_match": True}
)
devices_suite.add_expectation(expectation_configuration=expectation_configuration)

for column in ["type", "full_name", "version"]:
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": column},
    )
    devices_suite.add_expectation(expectation_configuration=expectation_configuration)

expectation_configuration = ExpectationConfiguration(
    expectation_type="expect_column_values_to_be_in_set",
    kwargs={"column": "type", "value_set": set(["lenovo", "galaxy", "htc", "lg", "iphone", "mac"])},
)
devices_suite.add_expectation(expectation_configuration=expectation_configuration)

expectation_configuration = ExpectationConfiguration(
    expectation_type="expect_column_value_lengths_to_be_between",
    kwargs={"column": "full_name", "min_value": 5, "max_value": 20},
)
devices_suite.add_expectation(expectation_configuration=expectation_configuration)

context.save_expectation_suite(devices_suite, devices_expectations_name)

########################################################################################################################

visits_expectations_name = 'visits_expectations'
visits_suite: ExpectationSuite = context.add_or_update_expectation_suite(
    visits_expectations_name)

expectation_configuration = ExpectationConfiguration(
    expectation_type="expect_table_columns_to_match_ordered_list",
    kwargs={"column_list": ["visit_id", "event_time", "user_id", "keep_private", "page", "context"]}
)
visits_suite.add_expectation(expectation_configuration=expectation_configuration)

for column in ["visit_id", "event_time", "user_id", "keep_private", "page", "context"]:
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": column},
    )
    visits_suite.add_expectation(expectation_configuration=expectation_configuration)

expectation_configuration = ExpectationConfiguration(
    expectation_type="expect_column_values_to_match_strftime_format",
    kwargs={"column": "event_time", "strftime_format": "%Y-%m-%dT%H:%M:%S.%fZ"},
)
visits_suite.add_expectation(expectation_configuration=expectation_configuration)

for column in ["visit_id", "event_time", "user_id", "page"]:
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_value_lengths_to_be_between",
        kwargs={"column": column, "min_value": 2, "max_value": 50},
    )
    visits_suite.add_expectation(expectation_configuration=expectation_configuration)

context.save_expectation_suite(visits_suite, visits_expectations_name)






sys.exit(1)
context = gx.get_context(context_root_dir=context_root_dir)
context = gx.get_context()
datasource = context.sources.add_or_update_pandas(name="my_pandas_dataframe")
data_asset = datasource.add_dataframe_asset(name="asset")
batch_request = data_asset.build_batch_request(
    dataframe=pd.read_json('/tmp/dedp/ch02/data-readiness/marker/input/dataset.json', lines=True))

context.add_or_update_expectation_suite("my_expectation_suite")
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name="my_expectation_suite",
)
validator.head()

validator.expect_column_values_to_not_be_null(column="type")
validator.expect_column_values_to_not_be_null(column="full_name")
validator.expect_column_values_to_not_be_null(column="version")
validator.save_expectation_suite(discard_failed_expectations=False)

checkpoint = context.add_or_update_checkpoint(
    name="my_checkpoint",
    validations=[
        {
            "id": "validation_test_results",
            "batch_request": batch_request,
            "expectation_suite_name": "my_expectation_suite",
        },
    ],
)
checkpoint_result: CheckpointResult = checkpoint.run()
context.build_data_docs()

retrieved_checkpoint = context.get_checkpoint(name="my_checkpoint")
#print(retrieved_checkpoint)

print(checkpoint_result.list_validation_results)

"""
validator = context.sources.pandas_default.read_csv(
    "/tmp/dedp/ch02/data-readiness/marker/input/dataset.json"
)
"""
