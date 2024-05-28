import json
import logging
import os
import time
import textwrap

from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult

import pytest
from pyspark.sql import SparkSession

from test.dataset_holder import DataGeneratorWrapper
from test.guardian import DataQualityValidator
from test.spark_session_builder import spark_session_for_tests


# These tests run after executing all tests and assert whether the used datasets are relevant
# to the real data
# Eventually, you can define this check as a hook in `def pytest_sessionfinish(session, exitstatus):`
# but then any failure will not mark the unit test suite as failed
@pytest.fixture(scope="session", autouse=True)
def session_test_finish_handler(request):
    def validate_test_datasets():
        visits_to_validate = DataGeneratorWrapper.visits()
        devices_to_validate = DataGeneratorWrapper.devices()
        data_quality_validator = DataQualityValidator()
        logging.debug(f'visits_to_validate={visits_to_validate}')
        logging.debug(f'devices_to_validate={devices_to_validate}')

        visits_validation_result: CheckpointResult = data_quality_validator.validate_visits(visits_to_validate)
        logging.info(f'Validation result for visits={visits_validation_result}')
        devices_validation_result: CheckpointResult = data_quality_validator.validate_devices(devices_to_validate)
        logging.info(f'Validation result for devices={devices_validation_result}')

        def _get_validation_errors(validation_result: CheckpointResult):
            errors = ''
            for expectation_validation_result in validation_result.list_validation_results()[0].results:
                if not expectation_validation_result.success:
                    expectation_dict = expectation_validation_result.to_json_dict()
                    title = f"➡️ {expectation_dict['expectation_config']['expectation_type']}"
                    errors += textwrap.dedent(f'''
                        * {title}: {json.dumps(expectation_dict)}
                    ''')
            return errors

        errors_per_dataset = ''
        if not visits_validation_result.success:
            errors_per_dataset += textwrap.dedent(f'''
                📌 Visits:
                  {_get_validation_errors(visits_validation_result)}''')

        if not devices_validation_result.success:
            errors_per_dataset += textwrap.dedent(f'''
                📌 Devices:
                  {_get_validation_errors(devices_validation_result)}''')

        if errors_per_dataset:
            error_message = textwrap.dedent(f'''
            ⚠️ Data quality validation failure. Errors for:  
            {errors_per_dataset}''')

            raise Exception(error_message)
    # It's disabled but you can uncomment to see how the data quality guard might run
    #request.addfinalizer(validate_test_datasets)


@pytest.fixture(scope='session', autouse=False)
def generate_spark_session() -> SparkSession:
    # change Python timezone to avoid time-zones issues (Spark JVM vs. Python local) during the .collect()
    os.environ['TZ'] = 'UTC'
    time.tzset()
    session = spark_session_for_tests()
    yield [session]

    session.stop()
