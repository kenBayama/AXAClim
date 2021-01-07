
def read_data (spark, path,format): 

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




def load_data (input_data,config,exercice,format):
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


def read_parquet_and_createStore (spark,path,name_store,usefull_col) : 

    input_data = ( 
        spark.read.format("parquet")
        .load(path)
        .createOrReplaceTempView(name_store))

    return spark.sql(f"select {usefull_col} from Campagne")