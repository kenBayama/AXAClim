import json

from pyspark.sql.functions import regexp_replace, from_json, col, to_timestamp

from dependencies.spark import spark_env
from dependencies.utils import read_csv, load_data
from dependencies.schema import schema_json



def transform_dataframe (input_data):

    """
    apply a set of transformation to the entry spark.dataframe.
    clean the phone company client data

    Transformations : 

        CLEAN JSON column data
        EXPLODE the JSON column into two distinct guid and poi column
        CLEAN CLI_COEFF column
        CLEAN CLI_TEL column 
        CAST data types to the columns that requires it 
        

    Parameters : 
    input_data : spark.DataFrame

    |-- AGENT_ID1: long (nullable = true)
    |-- AGENT_ID2: long (nullable = true)
    |-- ADRESSE_PDV2: string (nullable = true)
    |-- CLI_TEL: string (nullable = true)
    |-- CLI_COEFF: string (nullable = true)
    |-- ANCCLI: integer (nullable = true)
    |-- CDPROENT: string (nullable = true)
    |-- CDREGAXA: integer (nullable = true)
    |-- DATE_LAST_RESIL: timestamp (nullable = true)
    |-- DATE_LAST_SOUS: timestamp (nullable = true)
    |-- DATE_NAISS: string (nullable = true)
    |-- JSON: string (nullable = true)
    
    return :
    transformed_df_final : spark.DataFrame

    |-- AGENT_ID1: long (nullable = true)
    |-- AGENT_ID2: long (nullable = true)
    |-- ADRESSE_PDV2: string (nullable = true)
    |-- CLI_TEL: string (nullable = true)
    |-- CLI_COEFF: double (nullable = true)
    |-- ANCCLI: integer (nullable = true)
    |-- CDPROENT: string (nullable = true)
    |-- CDREGAXA: integer (nullable = true)
    |-- DATE_LAST_RESIL: timestamp (nullable = true)
    |-- DATE_LAST_SOUS: timestamp (nullable = true)
    |-- DATE_NAISS: timestamp (nullable = true)
    |-- guid: string (nullable = true)
    |-- poi: string (nullable = true)

    """

    # CLEAN JSON column data
    transformed_df1 = input_data.withColumn("JSON", regexp_replace(input_data["JSON"], "^\"+|\"+$",""))

    # EXPLODE the JSON column into two distinct guid and poi column
    transformed_df2 = ( transformed_df1.withColumn("JSON", from_json(transformed_df1["JSON"],schema_json))
                            .select("*",col("JSON.*"))
                            .drop("JSON"))
    # CLEAN columns
    transformed_df3 = transformed_df2.withColumn("CLI_COEFF", regexp_replace(input_data["CLI_COEFF"], ",","."))
    transformed_df4 = transformed_df3.withColumn("CLI_TEL", regexp_replace(transformed_df3["CLI_TEL"], "[/.]",""))

    # CAST data types 
    transformed_df5 = transformed_df4.withColumn("CLI_COEFF", transformed_df4["CLI_COEFF"].cast("double"))
    transformed_df6 = transformed_df5.withColumn("DATE_NAISS", to_timestamp(transformed_df5["DATE_NAISS"], 'dd/MM/yyyy'))
    transformed_df7 = transformed_df6.withColumn("DATE_LAST_SOUS", to_timestamp(transformed_df6["DATE_LAST_SOUS"], 'yyyy-MM-dd HH:mm:ss'))
    transformed_df8 = transformed_df7.withColumn("DATE_LAST_RESIL", to_timestamp(transformed_df7["DATE_LAST_RESIL"], 'yyyy-MM-dd HH:mm:ss'))
    transformed_df9 = transformed_df8.withColumn("AGENT_ID1", transformed_df8["AGENT_ID1"].cast("long"))
    transformed_df10 = transformed_df9.withColumn("AGENT_ID2", transformed_df9["AGENT_ID2"].cast("long"))
    transformed_df11 = transformed_df10.withColumn("CDREGAXA", transformed_df10["CDREGAXA"].cast("int"))
    transformed_df_final = transformed_df11.withColumn("ANCCLI", transformed_df11["ANCCLI"].cast("int"))


    return transformed_df_final
    

          
def main() :
    # start Spark application and get Spark session, logger and config
    spark, log, config = spark_env(
    app_name = 'csv_to_dataframe_job',
    files = ['conf/configs.json'])
    exercice = "exercice1"
    path = config["path_raw_data_folder"] + "/" + config["input_file_csv"]

    # log that main ETL job is starting
    log.warn('csv_to_dataframe_job is up-and-running')


    # execute ETL pipeline
    
    data = read_csv(spark, path)
    data_transformed = transform_dataframe(data)
    load_data(data_transformed,config,exercice)

    # log the success and terminate Spark application
    log.warn('csv_to_dataframe_job is finished')
    spark.stop()
    return None


if __name__ == '__main__':
    main()