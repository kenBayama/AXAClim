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
    StructField('AGENT_ID1', LongType(), True),
    StructField('AGENT_ID2', LongType(), True),
    StructField('ADRESSE_PDV2', StringType(), True),
    StructField('CLI_TEL', StringType(), True),
    StructField('CLI_COEFF', DoubleType(), True),
    StructField('ANCCLI', IntegerType(), True),
    StructField('CDPROENT', StringType(), True),
    StructField('CDREGAXA', IntegerType(), True),
    StructField('DATE_LAST_RESIL', TimestampType(), True),
    StructField('DATE_LAST_SOUS', TimestampType(), True),
    StructField('DATE_NAISS', TimestampType(), True),
    StructField('guid',StringType(),True),
    StructField('poi',StringType(),True)
    ])