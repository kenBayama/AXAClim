import unittest
import json
from pyspark.sql.types import IntegerType,StringType,StructType, StructField, FloatType, TimestampType

from jobs.csv_to_dataframe_job import transform_dataframe
from dependencies.spark import spark_env
from jobs.schema import schema_csv_raw, schema_csv_expected
from tests.test_main import SparkJobTests

class count_client_contracts_Tests(SparkJobTests):
    """Test suite for transformation in etl_job.py
    """


    def test_count_client_contract_per_portofolio(self):
        """Test data transformer.
        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        """
        
        self.test_data_path = 'tests/test_data/'
        self.filename = '/02_campagne.parquet'
        self.expected_filename = '/exercice2'


        # assemble
        """input_data = (
            self.spark.read.format("csv") 
                .option("header", True) 
                .schema(schema) 
                .load(self.test_data_path + self.filename ))"""

        filename_raw = self.get_path_test(self.test_data_path,"raw",self.filename)
        #filename_expected = self.get_path_test(self.test_data_path,"expected",self.expected_filename)
  
        input_data = self.spark.read.parquet(filename_raw)
                

        #expected_data = self.spark.read.parquet(filename_expected)

        
        input_data.show()
        input_data.printSchema()

        # act
        data_transformed = transform_dataframe(input_data)
        data_transformed.show()
        data_transformed.printSchema()
        
        # assert
        #self.assertTrue(self.are_dfs_equal(expected_data,data_transformed))
        


if __name__ == '__main__':
    unittest.main()