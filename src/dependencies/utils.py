import os

def read_data (spark, path, format): 

    """
    this function allow you to read data from csv, avro or parquet file 
    and return those data in a spark.dataFrame

    Parameters : 
        spark : spark.SparkSession
        path : str
        format : str


    return :
        input_data : spark.DataFrame

    """

    input_data = None

    if format == "csv" : 
        input_data = (spark.read.format("csv") 
            .option("header", True) 
            .option("delimiter",";")
            .load(path))
    elif format == "avro" : 
        input_data = (spark.read.format("com.databricks.spark.avro") 
            .load(path))
    
    else : 
        input_data = spark.read.parquet(path)

    return input_data




def load_data (input_data, config, exercice, format):
    
    """
    this function allow you to read data from a spark.dataFrame
    and load those data in a parquet or an avro file based on the 
    format parameters. 

    Parameters :
        input_data : spark.DataFrame 
        config : dict of str
        exercice : str
        format : str


    return :
        None

    """

    if format == "parquet" : 
        (input_data
            .write
            .mode("overwrite")
            .parquet(config["path_processed_data_folder"] + "/" + exercice))
    else :
        (input_data
            .write
            .mode("overwrite")
            .format("com.databricks.spark.avro")
            .save(config["path_processed_data_folder"] + "/" + exercice))




def read_parquet_and_createStore (spark, path, name_store, usefull_col) :

    """
    this function allow you to read data from parquet files,
    store a memory reference of the read spark.dataFrame
    and return a subset of this spark.dataFrame 
    based on the usefull_col parameter 

    Parameters :
        input_data : spark.DataFrame 
        path : str
        name_store : str
        usefull_col : str


    return :
        None

    """

    input_data = ( 
        spark.read.format("parquet")
        .load(path)
        .createOrReplaceTempView(name_store))

    return spark.sql(f"select {usefull_col} from {name_store}")