import unittest
import json
import pandas as pd


from jobs.csv_to_dataframe_job import transform_dataframe
from dependencies.spark import spark_env
from dependencies.utils import read_data
from tests.test_main import SparkJobTests

class csv_to_dataframe_Tests(SparkJobTests):

    """
    Test suite for transformation in csv_to_dataframe_job.py

    """


    def test_csv_to_dataframe_job(self):

        """
        Test transform_dataframe function 
        
        """
        self.logger.warn('Testing csv_to_dataframe')
        # parameter
        self.test_data_path = 'tests/test_data/'
        self.filename = '/01_csv_to_dataframe.csv'
        self.expected_filename = '/exercice1'
        filename_raw = (self.get_path_test(self.test_data_path,
                                            "raw",
                                            self.filename))

        filename_expected = (self.get_path_test(self.test_data_path,
                                                "expected",
                                                self.expected_filename))
 
        # extract
        self.logger.warn('extract step : extracting the data')

        input_data = read_data(self.spark,filename_raw,"csv")
        expected_data = read_data(self.spark,filename_expected,"avro")


    
        # transform
        self.logger.warn('transform step : transforming the data')
        data_transformed = transform_dataframe(input_data)
        

        # assert
        self.logger.warn('assert step :  asserting the result')
        (self.assertTrue(self.are_dfs_equal(expected_data,
                                            data_transformed,True)))
        
        


if __name__ == '__main__':
    unittest.main()