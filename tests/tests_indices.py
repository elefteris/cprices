# Import pyspark packages
from pyspark.sql import types as T
from epds_utils.testing.pyspark_test_case import PySparkTest

# Import python packages
import os
import unittest
import pandas as pd
from importlib import reload

#Import module to test
from cprices.cprices.steps import indices
reload(indices)

class TestIndices(PySparkTest):
    """Test class for indices."""

    def setUp(self):
        self.root_dir = '/home/cdsw/prices_webscraping/cpdt_core/'
        self.groupby_cols = ['group']

    def input_data_product_by_month_tables(self):
        """Create input data set for product_by_month_tables"""

        path = os.path.join(self.root_dir, 'tests', 'data', 'input_df.csv')
        input_df = self.spark.createDataFrame(pd.read_csv(path))

        return input_df

    def expected_output_product_by_month_tables(self):
        """Create expected output data set for carlifb"""

        path = os.path.join(self.root_dir, 'tests', 'data', 'df.csv')
        expected_df = self.spark.createDataFrame(pd.read_csv(path))

        path = os.path.join(self.root_dir, 'tests', 'data', 'dfp.csv')
        expected_dfp = self.spark.createDataFrame(pd.read_csv(path))
        expected_dfp = expected_dfp.replace(float('nan'), None)

        path = os.path.join(self.root_dir, 'tests', 'data', 'dfq.csv')
        expected_dfq = self.spark.createDataFrame(pd.read_csv(path))
        expected_dfq = expected_dfq.replace(float('nan'), None)

        return expected_df, expected_dfp, expected_dfq

    def test_product_by_month_tables(self):
        """Unit test for product_by_month_tables"""

        input_df = self.input_data_product_by_month_tables()
        expected = self.expected_output_product_by_month_tables()
        expected_df, expected_dfp, expected_dfq = expected
        result = indices.product_by_month_tables(input_df, self.groupby_cols)
        result_df, result_dfp, result_dfq = result

        self.assertDFEqualUnordered(result_df, expected_df)
        self.assertDFEqualUnordered(result_dfp, expected_dfp)
        self.assertDFEqualUnordered(result_dfq, expected_dfq)

if __name__ == "__main__":

    suite = unittest.TestSuite()
    #unittest.main(exit=False)

    suite.addTest(TestIndices("test_product_by_month_tables"))

    runner = unittest.TextTestRunner()
    runner.run(suite)
