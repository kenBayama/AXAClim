import unittest
import json
import pandas as pd

from tests.test_main import SparkJobTests
from dependencies.utils import read_data
from jobs.top_client_per_portofolio_job import (transform_dataframe, 
                                                mock_input_prep)


class top_client_number_per_portofolio_Tests(SparkJobTests):



    def test_top_client_number_per_portofolio (self) : 
        """
        Test transform_dataframe function 
        
        """
        self.logger.warn('Testing top_client_number_per_portofolio')

        # parameter
        self.test_data_path = 'tests/test_data/'
        self.filename = '/exercice2'
        self.expected_filename = '/exercice3'
        filename_processed = (self.get_path_test(self.test_data_path,
                                                "expected",
                                                self.filename))
        filename_expected = (self.get_path_test(self.test_data_path,
                                                "expected",
                                                self.expected_filename))
 
        # extract
        self.logger.warn('extract step : extracting the data')
        input_data = read_data(self.spark,filename_processed,"avro")
        expected_data = read_data(self.spark,filename_expected,"avro")
      
        
    
        # transform
        self.logger.warn('transform step : transfoming the data')
        prep_input_data = mock_input_prep(input_data)
        data_transformed = transform_dataframe(prep_input_data)
        


        # assert
        self.logger.warn('assert step :  asserting the result')
        (self.assertTrue(self.are_dfs_equal(expected_data,
                                            data_transformed,
                                            False)))
        



if __name__ == '__main__':
    unittest.main()