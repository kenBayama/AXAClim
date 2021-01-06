from pyspark.sql.functions import row_number, col, explode, array, lit, udf, rand, monotonically_increasing_id
from pyspark.sql.types import ArrayType, IntegerType
from pyspark.sql import Window

from dependencies.spark import spark_env
from dependencies.utils import read_parquet_and_createStore, load_data


def mock_input_prep (input_data) : 
    n_to_array = udf(lambda n : [n] * n, ArrayType(IntegerType()))
    mock_input_data = input_data.withColumn('n', n_to_array(input_data.TOTAL_CLIENTS))
    mock_input_data2 = mock_input_data.withColumn("mock", explode(mock_input_data.n))
    mock_input_data3 = ( mock_input_data2.withColumn
                ("TOTAL_CLIENTS", 
                (mock_input_data2["TOTAL_CLIENTS"] * monotonically_increasing_id()*3)
                .cast("int")))

    mock_input_data4 = mock_input_data3.drop("mock").drop("n")
    
    mock_input_data4.show()
    mock_input_data4.printSchema()

    return mock_input_data4

def transform_dataframe (input_data) : 
    
    w =   Window.partitionBy("NMPTF").orderBy(col("TOTAL_CLIENTS"))
    transformed_df1 = input_data.select("*",row_number().over(w).alias('row')).where(col("row")<=3)
    transformed_df_final = transformed_df1.drop("row")

    transformed_df_final.show()
    transformed_df_final.printSchema()
    

    return transformed_df_final
    




def main() : 
    # start Spark application and get Spark session, logger and config
    spark, log, config = spark_env(
    app_name='top_client_per_portofolio_job',
    files=['conf/configs.json'])
    exercice="exercice3"

    # log that main ETL job is starting
    log.warn('top_client_per_portofolio_job is up-and-running')

    path = config["path_processed_data_folder"] + "/" + config["input_file_parquet2"]


    # execute ETL pipeline
    input_data = spark.read.parquet(path)
    prep_input_data = mock_input_prep (input_data)
    data_transformed = transform_dataframe(prep_input_data)
    load_data(data_transformed, config, exercice)

    # log the success and terminate Spark application
    log.warn('top_client_per_portofolio_job is finished')
    spark.stop()
    return None


if __name__ == '__main__':
    main()