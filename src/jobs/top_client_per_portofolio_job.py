from pyspark.sql.functions import ( row_number, col, 
                                    explode, array, lit, udf, 
                                    rand, monotonically_increasing_id )
from pyspark.sql.types import ArrayType, IntegerType
from pyspark.sql import Window

from dependencies.spark import spark_env
from dependencies.utils import read_data, load_data


def mock_input_prep (input_data) : 

    """
    apply a set of transformation to the entry spark.dataframe.
    create a dataset from the result of exercice 2 which can be use
    to answer the exercice 3  

    Transformations : 

        CREATING an UDF that creat an array of int 
            based on the data from an other column

        CREATING a column N on which the udf n_to_array is applied

        EXPLODING the data in the n column and stored it in the MOCK column
            The rows are duplicated

        MULTIPLYING the number of client (TOTAL_CLIENTS) 
            by an ascending value of integer 

        DROPING the MOCK and N column  
        

    Parameters : 
    input_data : spark.DataFrame

    root
    |-- NMPTF: string (nullable = true)
    |-- TOTAL_CLIENTS: long (nullable = true)
    |-- TOTAL_CONTRATS: long (nullable = true)
    |-- ECH_1: long (nullable = true)
    |-- ECH_2: long (nullable = true)
    |-- ECH_3: long (nullable = true)
    |-- ECH_4: long (nullable = true)
    |-- ECH_6: long (nullable = true)
    |-- ECH_8: long (nullable = true)
    |-- ECH_9: long (nullable = true)
    |-- ECH_10: long (nullable = true)
    |-- ECH_11: long (nullable = true)
    |-- ECH_12: long (nullable = true)

    return :
    mock_input_data_final : spark.DataFrame

    root
    |-- NMPTF: string (nullable = true)
    |-- TOTAL_CLIENTS: long (nullable = true)
    |-- TOTAL_CONTRATS: long (nullable = true)
    |-- ECH_1: long (nullable = true)
    |-- ECH_2: long (nullable = true)
    |-- ECH_3: long (nullable = true)
    |-- ECH_4: long (nullable = true)
    |-- ECH_6: long (nullable = true)
    |-- ECH_8: long (nullable = true)
    |-- ECH_9: long (nullable = true)
    |-- ECH_10: long (nullable = true)
    |-- ECH_11: long (nullable = true)
    |-- ECH_12: long (nullable = true)


    """ 

    n_to_array = udf(lambda n : [n] * n, ArrayType(IntegerType()))

    mock_input_data = ( 
        input_data.withColumn('N', n_to_array(input_data.TOTAL_CLIENTS)))

    mock_input_data2 = ( 
        mock_input_data.withColumn("MOCK", explode(mock_input_data.N)))

    mock_input_data3 = ( 
        mock_input_data2.withColumn("TOTAL_CLIENTS", 
        (mock_input_data2["TOTAL_CLIENTS"] * monotonically_increasing_id()*3)
            .cast("int")))

    mock_input_data_final = mock_input_data3.drop("MOCK").drop("N")
    
    return mock_input_data_final

def transform_dataframe (input_data) :
    """
    apply a set of transformation to the entry spark.dataframe.
    
    Filter the data to keep for each category of portofolio the top three elements
    with the greater number of clients

    Transformations : 

        PARTITIONING data based on portofolio (NMPTF)
        ORDERING by the number of client (TOTAL_CLIENTS) 
            in descending order
        CREATE a column to number each row (NUMBER)
        FILTERING the rows based on their numbering (NUMBER) 
            lesser than or equal 3 
        DROP the column used for numbering (NUMBER)   
        

    Parameters : 
    input_data : spark.DataFrame

    root
    |-- NMPTF: string (nullable = true)
    |-- TOTAL_CLIENTS: long (nullable = true)
    |-- TOTAL_CONTRATS: long (nullable = true)
    |-- ECH_1: long (nullable = true)
    |-- ECH_2: long (nullable = true)
    |-- ECH_3: long (nullable = true)
    |-- ECH_4: long (nullable = true)
    |-- ECH_6: long (nullable = true)
    |-- ECH_8: long (nullable = true)
    |-- ECH_9: long (nullable = true)
    |-- ECH_10: long (nullable = true)
    |-- ECH_11: long (nullable = true)
    |-- ECH_12: long (nullable = true)

    return :
    transformed_df_final : spark.DataFrame

    root
    |-- NMPTF: string (nullable = true)
    |-- TOTAL_CLIENTS: long (nullable = true)
    |-- TOTAL_CONTRATS: long (nullable = true)
    |-- ECH_1: long (nullable = true)
    |-- ECH_2: long (nullable = true)
    |-- ECH_3: long (nullable = true)
    |-- ECH_4: long (nullable = true)
    |-- ECH_6: long (nullable = true)
    |-- ECH_8: long (nullable = true)
    |-- ECH_9: long (nullable = true)
    |-- ECH_10: long (nullable = true)
    |-- ECH_11: long (nullable = true)
    |-- ECH_12: long (nullable = true)


    """ 
    # PARTITIONING data based on portofolio value and 
    # ORDERING the number of client in descending order 
    # Then FILTERING based on their numbering 

    w = Window.partitionBy("NMPTF").orderBy(col("TOTAL_CLIENTS").desc())
    transformed_df1 = ( input_data.select("*",row_number()
        .over(w).alias('NUMBER'))
        .where(col("NUMBER")<=3) )
    transformed_df_final = transformed_df1.drop("NUMBER")
    

    return transformed_df_final
    




def main() : 
    # start Spark application and get Spark session, logger and config
    spark, log, config = spark_env(app_name='top_client_per_portofolio_job',
                                    files=['conf/configs.json'])
    spark.conf.set('spark.sql.avro.compression.codec', 'deflate')

    exercice="exercice3"

    # log that main ETL job is starting
    log.warn('top_client_per_portofolio_job is up-and-running')

    path = ( config["path_processed_data_folder"] + "/" 
            + config["input_file_parquet2"] )


    # execute ETL pipeline
    input_data = read_data(spark,path,"avro")

    #preprocessing phase
    prep_input_data = mock_input_prep (input_data)

    #processing phase
    data_transformed = transform_dataframe(prep_input_data)
    load_data(data_transformed, config, exercice,"avro")


    # log the success and terminate Spark application
    log.warn('top_client_per_portofolio_job is finished')
    spark.stop()
    return None


if __name__ == '__main__':
    main()