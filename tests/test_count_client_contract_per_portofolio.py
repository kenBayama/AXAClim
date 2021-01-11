import unittest
import json

from jobs.count_client_contract_job import (transform_dataframe, 
                                            transform_dataframe_alternative)
from dependencies.spark import spark_env
from dependencies.schema import schema_csv_raw, schema_csv_expected
from dependencies.utils import read_parquet_and_select, read_data
from tests.test_main import SparkJobTests

class count_client_contracts_Tests(SparkJobTests):
    """
    Test suite for transformation in ount_client_contracts_job.py
    """


    def test_count_client_contract_per_portofolio_job(self):
        """
        Test transform_dataframe function 
        
        """

        self.logger.warn('Testing count_client_contract_per_portofolio')

        # parameter
        self.test_data_path = 'tests/test_data/'
        self.filename = '/02_campagne.parquet'
        self.expected_filename = '/exercice2'
        self.expected_filename_alt = '/exercice2_alt'


        filename_raw = (self.get_path_test(self.test_data_path,
                                            "raw",
                                            self.filename))

        filename_expected = (self.get_path_test(self.test_data_path,
                                                "expected",
                                                self.expected_filename))

        filename_expected_alt= (self.get_path_test(self.test_data_path,
                                                "expected",
                                                self.expected_filename_alt))
        usefull_col = ["NMPTF","00021_NUMCLIA","00004_NUMCLE","01255_MOISAN"]
        

        # extract
        self.logger.warn('extract step : extracting the data')
        input_data = (read_parquet_and_select(self.spark,
                                                    filename_raw,
                                                    usefull_col))

        expected_data = read_data(self.spark,filename_expected,"avro")
        expected_data_alt = read_data(self.spark,filename_expected_alt,"avro")

     

        # transform
        self.logger.warn('transform step : transfoming the data')
        data_transformed = transform_dataframe(input_data)
        data_transformed_alt = transform_dataframe_alternative(input_data)




        input_data.show()
        expected_data.show()
        expected_data_alt.sort("NMPTF","TOTAL_CLIENTS").show()
        data_transformed.show()
        data_transformed_alt.sort("NMPTF","TOTAL_CLIENTS").show()

        
        # assert
        self.logger.warn('assert step :  asserting the result')
        (self.assertTrue(self.are_dfs_equal(expected_data,
                                            data_transformed,
                                            False)))
        (self.assertTrue(self.are_dfs_equal(expected_data_alt.sort("NMPTF","TOTAL_CLIENTS"),
                                            data_transformed_alt.sort("NMPTF","TOTAL_CLIENTS"),
                                            False)))
        


if __name__ == '__main__':
    unittest.main()