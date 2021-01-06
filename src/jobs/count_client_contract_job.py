import json
from dependencies.spark import spark_env
from jobs.schema import schema_json

from dependencies.sparkInstrumentedJob import read_csv, load_data



def transform_dataframe (input_data):
    pass


def main() :
    # start Spark application and get Spark session, logger and config
    spark, log, config = spark_env(
    app_name='count_client_contract_job',
    files=['conf/configs.json'])
    exercice="exercice2"

    # log that main ETL job is starting
    log.warn('count_client_contract_job is up-and-running')


    # execute ETL pipeline
    data = read_csv(spark,config)
    data_transformed = transform_dataframe(data)
    load_data(data_transformed, config, exercice)

    # log the success and terminate Spark application
    log.warn('count_client_contract_job is finished')
    spark.stop()
    return None


if __name__ == '__main__':
    main()