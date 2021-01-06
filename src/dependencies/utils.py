
def read_csv (spark, path): 

    input_data = (spark.read.format("csv") 
    .option("header", True) 
    .option("delimiter",";")
    .load(path))
    return input_data




def load_data (input_data,config,exercice):

    input_data.write.mode("overwrite").parquet(config["path_processed_data_folder"] + "/" + exercice)
    input_data.write.mode("overwrite").csv(config["path_processed_data_folder"] + "/"+ exercice + "_csv/")

def read_parquet_and_createStore (spark,path,name_store,usefull_col) : 

    input_data = ( 
        spark.read.format("parquet")
        .load(path)
        .createOrReplaceTempView(name_store))

    return spark.sql(f"select {usefull_col} from Campagne")