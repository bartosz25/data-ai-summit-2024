from pyspark.sql import SparkSession


def convert_devices_to_delta_table(spark_session: SparkSession, devices_input_location: str,
                                   devices_output_location: str):
    input_devices = spark_session.read.schema('type STRING, full_name STRING, version STRING').json(devices_input_location)

    input_devices.write.mode('overwrite').format('delta').save(devices_output_location)
