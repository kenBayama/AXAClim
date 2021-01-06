
def read_csv (spark,config): 

    input_data = (spark.read.format("csv") 
    .option("header", True) 
    .option("delimiter",";")
    .load(config["path_raw_data_folder"] + "/" + config["input_file_csv"]))
    return input_data


def load_data (input_data,config,exercice):

    input_data.write.mode("overwrite").parquet(config["path_processed_data_folder"] + "/" + exercice)
    input_data.write.mode("overwrite").csv(config["path_processed_data_folder"] + "/"+ exercice + "_csv/")
