from pyspark.sql.types import IntegerType,StringType,StructType, StructField, FloatType, TimestampType,LongType, DoubleType


schema_csv_raw = StructType([
    StructField('AGENT_ID1', LongType(), True),
    StructField('AGENT_ID2', LongType(), True),
    StructField('ADRESSE_PDV2', StringType(), True),
    StructField('CLI_TEL', StringType(), True),
    StructField('CLI_COEFF', FloatType(), True),
    StructField('ANCCLI', IntegerType(), True),
    StructField('CDPROENT', StringType(), True),
    StructField('CDREGAXA', IntegerType(), True),
    StructField('DATE_LAST_RESIL', TimestampType(), True),
    StructField('DATE_LAST_SOUS', TimestampType(), True),
    StructField('DATE_NAISS', TimestampType(), True),
    StructField('JSON',StringType(), True)
    ])

schema_json =  StructType([
                StructField('guid',StringType(),True),
                StructField('poi',StringType(),True)

    ])



schema_csv_expected = StructType([
    StructField('NMPTF', StringType(), True),
    StructField('TOTAL_CLIENTS', IntegerType(), True),
    StructField('TOTAL_CONTRATS', IntegerType(), True),
    StructField('ECH_1', IntegerType(), True),
    StructField('ECH_2', IntegerType(), True),
    StructField('ECH_3', IntegerType(), True),
    StructField('ECH_4', IntegerType(), True)
    
    ])