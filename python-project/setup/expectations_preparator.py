import os

import great_expectations as gx
from great_expectations.core import ExpectationSuite, ExpectationConfiguration

context_root_dir = f'{os.getcwd()}/great_expectations_spec'

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

