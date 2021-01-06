import unittest
import json
from pandas.testing import assert_frame_equal

from jobs.csv_to_dataframe_job import transform_dataframe
from dependencies.spark import spark_env
from jobs.schema import schema_csv_raw, schema_csv_expected

class SparkJobTests(unittest.TestCase):
    """Test suite for transformation in etl_job.py
    """

    def setUp(self):
        """Start Spark, define config and path to test data
        """
        self.config = json.loads("""{"steps_per_floor": 21}""")

        self.spark, self.logger, *_ = spark_env()

        self.test_data_path = 'tests/test_data/'
        self.filename = '/01_csv_to_dataframe.csv'
        self.expected_filename = '/exercice1'


    def tearDown(self):
        """Stop Spark
        """
        self.spark.stop()


    def are_dfs_equal(self, df1, df2):
        if df1.schema != df2.schema:
            return False
        if df1.take(1) != df2.take(1):
            return False
        return True

    def get_path_test (self, folder, cat, filename) : 
        return folder + cat + filename

    def assert_frame_equal_with_sort(self,results, expected, keycolumns):
        results_sorted = results.sort_values(by=keycolumns).reset_index(drop=True)
        expected_sorted = expected.sort_values(by=keycolumns).reset_index(drop=True)
        assert_frame_equal(results_sorted, expected_sorted)