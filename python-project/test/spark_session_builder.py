from pyspark.sql import SparkSession


def spark_session_for_tests() -> SparkSession:
    spark = (SparkSession.builder.master("local[2]")
             .config('spark.driver.extraJavaOptions', '-Duser.timezone=UTC')
             .config('spark.executor.extraJavaOptions', '-Duser.timezone=UTC')
             .config('spark.sql.session.timeZone', 'UTC')
             .config('spark.ui.enabled', False)
             .config('spark.sql.shuffle.partitions', 2)
             .getOrCreate())

    return spark
