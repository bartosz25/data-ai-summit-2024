import argparse

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, DataFrame

from visits_generator.devices_to_delta_converter import convert_devices_to_delta_table
from visits_generator.sessions_generation_job_logic import generate_sessions

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='Sessionization pipeline example for DAIS 2024')
    parser.add_argument('--kafka_bootstrap_servers', required=True)
    parser.add_argument('--kafka_input_topic', required=True)
    parser.add_argument('--kafka_output_topic', required=True)
    parser.add_argument('--devices_table_location', required=True)
    parser.add_argument('--checkpoint_location', required=True)
    args = parser.parse_args()

    spark_session = configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                   .config("spark.sql.shuffle.partitions", 2)
                                                   .config("spark.sql.extensions",
                                                           "io.delta.sql.DeltaSparkSessionExtension")
                                                   .config("spark.sql.catalog.spark_catalog",
                                                           "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
                                                   extra_packages=['org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0']
                                                   ).getOrCreate()

    input_data_stream = (spark_session.readStream
                         .option('kafka.bootstrap.servers', args.kafka_bootstrap_servers)
                         .option('subscribe', args.kafka_input_topic)
                         .option('startingOffsets', 'LATEST')
                         .option('maxOffsetsPerTrigger', '1000')
                         .format('kafka').load())

    devices_delta_location = f'{args.devices_table_location}/delta'
    convert_devices_to_delta_table(spark_session, args.devices_table_location, devices_delta_location)
    devices_table: DataFrame = spark_session.read.format('delta').load(devices_delta_location)

    sessions_writer = generate_sessions(input_data_stream, devices_table, {'processingTime': '10 seconds'},
                                        args.checkpoint_location)

    write_query = (sessions_writer
                   .format("kafka")
                   .option('kafka.bootstrap.servers', args.kafka_bootstrap_servers)
                   .option('topic', args.kafka_output_topic)
                   .start())

    write_query.awaitTermination()
