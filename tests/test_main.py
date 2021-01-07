import unittest
import json
from pandas.testing import assert_frame_equal

from dependencies.spark import spark_env


class SparkJobTests(unittest.TestCase):
    """Test suite for transformation in etl_job.py
    """

    def setUp(self):
        """Start Spark, define config and path to test data
        """

        self.spark, self.logger, *_ = spark_env(jar_packages=["org.apache.spark:spark-avro_2.11:2.4.7"])

        self.test_data_path = 'tests/test_data/'
        self.filename = '/01_csv_to_dataframe.csv'
        self.expected_filename = '/exercice1'


    def tearDown(self):
        """Stop Spark
        """
        self.spark.stop()


    def are_dfs_equal(self, df1, df2,first):
        df1_schema = df1.schema.fields
        df2_schema = df2.schema.fields
        df1_tuples = [(x.name, x.dataType) for x in df1_schema]
        df2_tuples = [(x.name, x.dataType) for x in df2_schema]

        diff = set(df1_tuples).difference
        if df1_tuples != df2_tuples:
            return False
        if first :
            if df1.take(1) != df2.take(1):
                return False
        else : 
            if df1.collect() != df2.collect():
                return False
        return True


    def get_path_test (self, folder, cat, filename) : 
        return folder + cat + filename

