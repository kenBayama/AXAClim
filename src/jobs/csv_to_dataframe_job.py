import json

from pyspark.sql.functions import regexp_replace, from_json, col, to_timestamp

from dependencies.spark import spark_env
from dependencies.utils import read_data, load_data
from dependencies.schema import schema_json



def transform_dataframe (input_data):

    """
    apply a set of transformation to the entry spark.dataframe.
    clean the phone company client data

    Transformations : 

        CLEANING JSON column data by removing useless double quote 
            at the beggining and at the end to the string
        EXPLODING the JSON column into two distinct guid and poi columns
        DROPING the JSON column
        CLEANING CLI_COEFF column by replacing commas by dots
        CLEANING CLI_TEL column BY removing the dots and the slashes
        CASTING appropriate data types to the columns that requires it 
        

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
    # EXPLODE the JSON column into two distinct guid and poi column 
    # CLEAN columns  
    # CAST data types 

    transformed_df_final = ( 
        input_data
            .withColumn("JSON", regexp_replace(col("JSON"), "^\"+|\"+$",""))
            .withColumn("JSON", from_json(col("JSON"),schema_json))
            .select("*",col("JSON.*"))
            .drop("JSON")

            .withColumn("CLI_COEFF", regexp_replace(col("CLI_COEFF"), ",","."))
            .withColumn("CLI_TEL", regexp_replace(col("CLI_TEL"), "[/.]",""))

            .withColumn("CLI_COEFF", col("CLI_COEFF").cast("float"))
            .withColumn("DATE_NAISS", to_timestamp(col("DATE_NAISS"), 
                                                'dd/MM/yyyy'))                               
            .withColumn("DATE_LAST_SOUS", to_timestamp(col("DATE_LAST_SOUS"), 
                                                'yyyy-MM-dd HH:mm:ss'))
            .withColumn("DATE_LAST_RESIL", to_timestamp(col("DATE_LAST_RESIL"), 
                                                'yyyy-MM-dd HH:mm:ss'))
            .withColumn("AGENT_ID1", col("AGENT_ID1").cast("long"))
            .withColumn("AGENT_ID2", col("AGENT_ID2").cast("long"))
            .withColumn("CDREGAXA", col("CDREGAXA").cast("int"))
            .withColumn("ANCCLI", col("ANCCLI").cast("int")))      


    return transformed_df_final
    

          
def main() :
    # start Spark application and get Spark session, logger and config

    spark, log, config = spark_env(app_name = 'csv_to_dataframe_job',
                                    files = ['conf/configs.json'])

    spark.conf.set('spark.sql.avro.compression.codec', 'deflate')
    
    exercice = "exercice1"
    path = config["path_raw_data_folder"] + "/" + config["input_file_csv"]

    # log that main ETL job is starting
    log.warn('csv_to_dataframe_job is up-and-running')


    # execute ETL pipeline
    
    data = read_data(spark, path,"csv")
    data_transformed = transform_dataframe(data)
    load_data(data_transformed,config,exercice,"avro")

    # log the success and terminate Spark application
    log.warn('csv_to_dataframe_job is finished')
    spark.stop()
    return None


if __name__ == '__main__':
    main()