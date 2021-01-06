import unittest
import json
import pandas as pd


from jobs.csv_to_dataframe_job import transform_dataframe
from dependencies.spark import spark_env
from tests.test_main import SparkJobTests

class csv_to_dataframe_Tests(SparkJobTests):

    """
    Test suite for transformation in csv_to_dataframe_job.py

    """


    def test_csv_to_dataframe(self):

        """
        Test transform_dataframe function 
        
        """
      
        self.test_data_path = 'tests/test_data/'
        self.filename = '/01_csv_to_dataframe.csv'
        self.expected_filename = '/exercice1'

        # extract
        self.logger.warn('extract step : extracting the data')
        filename_raw = self.get_path_test(self.test_data_path,"raw",self.filename)
        filename_expected = self.get_path_test(self.test_data_path,"expected",self.expected_filename)
  
        input_data = (
            self.spark.read.format("csv")
                .option("header", True) 
                .option("delimiter",";")
                .option("inferSchema",True)
                .load(filename_raw))
        
        expected_data = self.spark.read.parquet(filename_expected)

        # transform
        self.logger.warn('transform step : transfoming the data')
        data_transformed = transform_dataframe(input_data)
        
        # assert
        self.logger.warn('assert step :  asserting the result')
        #self.assertTrue(self.assert_frame_equal_with_sort(expected_data.toPandas(),data_transformed.toPandas(),"AGENT_ID1"))
        self.assertTrue(self.are_dfs_equal(expected_data,data_transformed))
        
        


if __name__ == '__main__':
    unittest.main()