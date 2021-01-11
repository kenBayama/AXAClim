import json
from pyspark.sql.functions  import countDistinct, when, first
from functools  import reduce

from dependencies.spark import spark_env
from dependencies.schema import schema_json
from dependencies.utils import (read_data, 
                                load_data, 
                                read_parquet_and_select)


def transform_dataframe (input_data):
    """
    apply a set of transformation to the entry spark.dataframe.
    
    Restructure the company client-contracts portofolio data in order to have
    for each portofolio the number of contract which arrive at its term 
    for january, february, november and december (1/2/11/12)

    Transformations : 

        AGGREGATING the total number of client (00021_NUMCLIA) 
            per portofolio (NMPTF)

        AGGREGATING the total number of contracts (00004_NUMCLE) 
            per portofolio (NMPTF)

        AGGREGATING number of contracts per portofolio (NMPTF) 
            and per term (01255_MOISAN)

        PIVOTING the term (01255_MOISAN) for each portofolio (NMPTF) 
            and store the number of contract per portofolio (NMPTF) 
            and per term (01255_MOISAN) in the newly created columns(ECH)

        DROPING the useless columns
        
        RENAMING the newly created column

        JOINING all the resulting in a dataFrames
        
    Parameters : 

        input_data : spark.DataFrame

        |-- NMPTF: string (nullable = true)
        |-- 00021_NUMCLIA: string (nullable = true)
        |-- 00004_NUMCLE: string (nullable = true)
        |-- 01255_MOISAN: integer (nullable = true)
        
    return :

        transformed_df_final : spark.DataFrame

        |-- NMPTF: string (nullable = true)
        |-- TOTAL_CLIENTS: long (nullable = false)
        |-- TOTAL_CONTRATS: long (nullable = true)
        |-- ECH_1: long (nullable = true)
        |-- ECH_2: long (nullable = true)
        |-- ECH_3: long (nullable = true)
        |-- ECH_4: long (nullable = true)


    """

    # AGGREGATE the total number of client and contracts
    total_clients = ( input_data.groupBy("NMPTF")
                                .agg(countDistinct("00021_NUMCLIA")
                                .alias("TOTAL_CLIENTS")) )

    total_contracts = ( input_data.groupBy("NMPTF")
                                .agg(countDistinct("00004_NUMCLE")
                                .alias("TOTAL_CONTRATS")) )


    # AGGREGATE the number of contracts per portofolio and per term
    # PIVOT the term for each portofolio
    echeance = ( input_data.groupBy("NMPTF","01255_MOISAN")
                                .agg(countDistinct("00004_NUMCLE")
                                .alias("ECH"))
                                .groupBy("NMPTF")
                                .pivot("01255_MOISAN")
                                .agg(first("ECH")) )

    # DROPING the useless columns 
    # RENAME the newly created column
    pivoted_new_col = echeance.columns
    pivoted_new_col.remove("NMPTF")
    col_to_keep = ["1","2","11","12"] 
    to_drop = set(pivoted_new_col) - set(col_to_keep)

    """for i in pivoted_new_col :
        if i not in  col_to_keep : 
            echeance.drop(i)"""
    
    echeance_final = (
        reduce((
            lambda df, col_name: 
                df.drop(col_name)),
                to_drop, 
                echeance)
        .withColumnRenamed("1","ECH_1")
        .withColumnRenamed("2","ECH_2")
        .withColumnRenamed("11","ECH_3")
        .withColumnRenamed("12","ECH_4") )
            

    # JOIN all the resulting dataFrames   
    transformed_df_final = ( total_clients
        .join(total_contracts,on="NMPTF",how="left")
        .join(echeance_final, on="NMPTF",how="left"))

    return transformed_df_final


def transform_dataframe_alternative (input_data):
    """
    apply a set of transformation to the entry spark.dataframe.
    
    Restructure the company client-contracts portofolio data in order to have
    for each portofolio the number of contract which arrive at its term 
    for term for january, february, november and december (1/2/11/12)

    Transformations : 

        AGGREGATING the total number of client (00021_NUMCLIA) 
            per portofolio (NMPTF) and term of contract (01255_MOISAN)

        AGGREGATING the total number of contracts (00004_NUMCLE) 
            per portofolio (NMPTF) and term of contract (01255_MOISAN)

        AGGREGATING number of contracts per portofolio (NMPTF) 
            and per term (01255_MOISAN)

        PIVOTING the term (01255_MOISAN) for each portofolio (NMPTF) 
            and store the number of contract per portofolio (NMPTF) 
            and per term (01255_MOISAN) in the newly created columns(ECH)

        DROPING the useless columns

        RENAMING the newly created column

        JOINING all the resulting in a dataFrames
        
    Parameters : 

        input_data : spark.DataFrame

        |-- NMPTF: string (nullable = true)
        |-- 00021_NUMCLIA: string (nullable = true)
        |-- 00004_NUMCLE: string (nullable = true)
        |-- 01255_MOISAN: integer (nullable = true)
        
    return :

        transformed_df_final : spark.DataFrame

        |-- NMPTF: string (nullable = true)
        |-- TOTAL_CLIENTS: long (nullable = false)
        |-- TOTAL_CONTRATS: long (nullable = true)
        |-- ECH_1: long (nullable = true)
        |-- ECH_2: long (nullable = true)
        |-- ECH_3: long (nullable = true)
        |-- ECH_4: long (nullable = true)


    """
    # AGGREGATE the total number of client and contracts
    total_clients = ( input_data.groupBy("NMPTF","01255_MOISAN")
                                .agg(countDistinct("00021_NUMCLIA")
                                .alias("TOTAL_CLIENTS"))
                                .drop("01255_MOISAN") )

    total_contracts = ( input_data.groupBy("NMPTF")
                                .agg(countDistinct("00004_NUMCLE")
                                .alias("TOTAL_CONTRATS")) )


    # AGGREGATE the number of contracts per portofolio and per term
    # PIVOT the term for each portofolio
    echeance = ( input_data.groupBy("NMPTF","01255_MOISAN")
                                .agg(countDistinct("00004_NUMCLE")
                                .alias("ECH"))
                                .groupBy("NMPTF")
                                .pivot("01255_MOISAN")
                                .agg(first("ECH")
                                .alias("_ECH")) )

    # DROPING the useless columns
    # RENAME the newly created column
    pivoted_new_col = echeance.columns
    pivoted_new_col.remove("NMPTF")
    col_to_keep = ["1","2","11","12"] 
    to_drop = set(pivoted_new_col) - set(col_to_keep)

    """for i in pivoted_new_col :
        if i not in  col_to_keep : 
            echeance.drop(i)"""
    
    echeance_final = (
        reduce((
            lambda df, col_name: 
                df.drop(col_name)),
                to_drop, 
                echeance)
        .withColumnRenamed("1","ECH_1")
        .withColumnRenamed("2","ECH_2")
        .withColumnRenamed("11","ECH_3")
        .withColumnRenamed("12","ECH_4") )
            

    # JOIN all the resulting dataFrames   
    transformed_df_final = ( total_clients
        .join(total_contracts,on="NMPTF",how="left")
        .join(echeance_final, on="NMPTF",how="left"))

    return transformed_df_final


def main() :

    # start Spark application and get Spark session, logger and config
    spark, log, config = spark_env(app_name='count_client_contract_job',
                                    files=['conf/configs.json'])
    spark.conf.set('spark.sql.avro.compression.codec', 'snappy')
    
    exercice="exercice2"
    exercice_alt="exercice2_alt"


    # log that main ETL job is starting
    log.warn('count_client_contract_job is up-and-running')

    usefull_col = ["NMPTF","00021_NUMCLIA","00004_NUMCLE","01255_MOISAN"]
    path = config["path_raw_data_folder"] + "/" + config["input_file_parquet"]


    # execute ETL pipeline
    input_data = read_parquet_and_select(spark, 
                                        path,
                                        usefull_col)

    data_transformed = transform_dataframe(input_data)
    data_transformed_alternative = transform_dataframe_alternative(input_data)

    load_data(data_transformed, config, exercice,"avro")
    load_data(data_transformed_alternative, config, exercice_alt,"avro")



    # log the success and terminate Spark application
    log.warn('count_client_contract_job is finished')
    spark.stop()
    return None


if __name__ == '__main__':
    main()