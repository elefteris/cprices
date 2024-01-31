# Import pyspark packages
from pyspark.sql import functions as F
from pyspark.sql import types as T
from epds_utils.testing.pyspark_test_case import PySparkTest

# Import python packages
import unittest
from importlib import reload

#Import module to test
from cprices.cprices.steps import filtering
reload(filtering)

class TestFiltering(PySparkTest):
    """Test class for classification module."""

    def setUp(self):
        self.groupby_cols = ['group']
        self.mcs = 0.8
        self.rounding_scale = 3
        self.cols_to_round = [
            'price', 'sales', 'sum_sales', 'share', 'cumsum_share']

    def input_dat_get_expenditure_info(self):
        """Create unit test input data for get_expenditure_info"""

        input_data = [
            ('product_1', "supplier_1", 0.7, 1),
            ('product_1', "supplier_1", 1.0, 2),
            ('product_1', "supplier_1", 1.5, 1),
            ('product_1', "supplier_1", 1.5, 1),
            ('product_1', "supplier_1", 2.6, 3),
            ('product_1', "supplier_1", 2.7, 1),
            ('product_2', "supplier_2", 0.7, 1),
            ('product_2', "supplier_2", 1.0, 2),
            ('product_2', "supplier_2", 1.5, 1),
            ('product_2', "supplier_2", 1.5, 1),
            ('product_3', "supplier_2", 2.6, 3),
            ('product_3', "supplier_2", 2.7, 1)
        ]
        schema = T.StructType([
            T.StructField('product_id', T.StringType(), False),
            T.StructField('group', T.StringType(), False),
            T.StructField('price', T.DoubleType(), False),
            T.StructField('quantity', T.IntegerType(), False)])

        return self.spark.createDataFrame(input_data, schema)

    def expected_output_get_expenditure_info(self):
        """Create expected output data set for get_expenditure_info"""

        expected_output_data = [
            ("product_3","supplier_2",2.6,3,7.8,16.2,0.481,0.481,0),
            ("product_3","supplier_2",2.7,1,2.7,16.2,0.167,0.648,0),
            ("product_2","supplier_2",1.0,2,2.0,16.2,0.123,0.772,0),
            ("product_2","supplier_2",1.5,1,1.5,16.2,0.093,0.864,1),
            ("product_2","supplier_2",1.5,1,1.5,16.2,0.093,0.957,1),
            ("product_2","supplier_2",0.7,1,0.7,16.2,0.043,1.0,1),
            ("product_1","supplier_1",2.6,3,7.8,16.2,0.481,0.481,0),
            ("product_1","supplier_1",2.7,1,2.7,16.2,0.167,0.648,0),
            ("product_1","supplier_1",1.0,2,2.0,16.2,0.123,0.772,0),
            ("product_1","supplier_1",1.5,1,1.5,16.2,0.093,0.864,1),
            ("product_1","supplier_1",1.5,1,1.5,16.2,0.093,0.957,1),
            ("product_1","supplier_1",0.7,1,0.7,16.2,0.043,1.0,1)
        ]
        schema = T.StructType([
            T.StructField('product_id', T.StringType(), False),
            T.StructField('group', T.StringType(), False),
            T.StructField('price', T.DoubleType(), False),
            T.StructField('quantity', T.IntegerType(), False),
            T.StructField('sales', T.DoubleType(), False),
            T.StructField('sum_sales', T.DoubleType(), False),
            T.StructField('share', T.DoubleType(), False),
            T.StructField('cumsum_share', T.DoubleType(), False),
            T.StructField('filtered', T.IntegerType(), False)])

        return self.spark.createDataFrame(expected_output_data, schema)

    def test_get_expenditure_info(self):
        """Unit test for get_expenditure_info"""

        input_df = self.input_dat_get_expenditure_info()
        expected_df = self.expected_output_get_expenditure_info()
        result_df = filtering.get_expenditure_info(
            self.spark, input_df, self.groupby_cols, self.mcs)
        self.assertDFAlmostEqual(
            result_df, expected_df, self.cols_to_round, self.rounding_scale)


if __name__ == "__main__":

    suite = unittest.TestSuite()
    #unittest.main(exit=False)

    suite.addTest(TestFiltering("test_get_expenditure_info"))

    runner = unittest.TextTestRunner()
    runner.run(suite)
