# Import pyspark packages
from pyspark.sql import functions as F
from pyspark.sql import types as T
from epds_utils.testing.pyspark_test_case import PySparkTest

# Import python packages
import unittest
from importlib import reload

#Import module to test
from cprices.cprices.steps import averaging
reload(averaging)

class TestAveraging(PySparkTest):
    """Test class for averaging module."""

    def setUp(self):
        self.cols_to_round = ['price']
        self.rounding_scale = 3
        self.groupby_cols = ["product_id", "supplier", "month"]
        self.join_cols = ["product_id", "supplier", "date"]

    def input_data(self):
        """Create input data set for averaging unit tests"""

        input_data = [
            ('product_1', "supplier_1", "2019-01", 0.7, 1),
            ('product_1', "supplier_1", "2019-01", 1.0, 2),
            ('product_1', "supplier_1", "2019-01", 1.5, 1),
            ('product_1', "supplier_1", "2019-02", 1.5, 1),
            ('product_1', "supplier_1", "2019-02", 2.6, 3),
            ('product_1', "supplier_1", "2019-02", 2.7, 1),
            ('product_2', "supplier_2", "2019-01", 0.7, 1),
            ('product_2', "supplier_2", "2019-01", 1.0, 2),
            ('product_2', "supplier_2", "2019-02", 1.5, 1),
            ('product_2', "supplier_2", "2019-02", 1.5, 1),
            ('product_3', "supplier_2", "2019-01", 2.6, 3),
            ('product_3', "supplier_2", "2019-02", 2.7, 1)
        ]
        schema = T.StructType([
            T.StructField('product_id', T.StringType(), False),
            T.StructField('supplier', T.StringType(), False),
            T.StructField('date', T.StringType(), False),
            T.StructField('price', T.DoubleType(), False),
            T.StructField('quantity', T.IntegerType(), False)])

        df_input = self.spark.createDataFrame(input_data, schema)
        df_input = df_input.withColumn(
            'date', df_input['date'].cast(T.DateType()))
        df_input = df_input.withColumn('month', F.trunc('date', 'month'))

        return df_input

    def input_data_left_join(self):
        """Create input data set for testing the left_join() method"""

        input_data = [
            ('product_1', "supplier_1", "2019-01", 7),
            ('product_1', "supplier_1", "2019-01", 1),
            ('product_1', "supplier_1", "2019-01", 5),
            ('product_1', "supplier_1", "2019-02", 1),
            ('product_1', "supplier_1", "2019-02", 6),
            ('product_1', "supplier_1", "2019-02", 7),
            ('product_2', "supplier_2", "2019-01", 2),
            ('product_2', "supplier_2", "2019-01", 1),
            ('product_2', "supplier_2", "2019-02", 5),
            ('product_2', "supplier_2", "2019-02", 5),
            ('product_3', "supplier_2", "2019-01", 6),
            ('product_3', "supplier_2", "2019-02", 7)
        ]
        schema = T.StructType([
            T.StructField('product_id', T.StringType(), False),
            T.StructField('supplier', T.StringType(), False),
            T.StructField('date', T.StringType(), False),
            T.StructField('quantity', T.IntegerType(), False)])

        df1 = self.spark.createDataFrame(input_data, schema)
        df2 = df1.groupBy(self.join_cols).sum()

        return df1, df2

    def expected_output_waa(self):
        """Create expected output data set for weighted_arithmetic_averaging"""

        exp_output_data = [
            ('product_3', "supplier_2", "2019-02-01", 1, 2.7),
            ('product_1', "supplier_1", "2019-01-01", 4, 1.05),
            ('product_1', "supplier_1", "2019-02-01", 5, 2.4),
            ('product_2', "supplier_2", "2019-01-01", 3, 0.9),
            ('product_2', "supplier_2", "2019-02-01", 2, 1.5),
            ('product_3', "supplier_2", "2019-01-01", 3, 2.6),
        ]
        schema = T.StructType([
            T.StructField('product_id', T.StringType(), False),
            T.StructField('supplier', T.StringType(), False),
            T.StructField('month', T.StringType(), False),
            T.StructField('quantity', T.IntegerType(), False),
            T.StructField('price', T.DoubleType(), False)])

        return self.spark.createDataFrame(exp_output_data, schema)

    def expected_output_wga(self):
        """Create expected output data set for weighted_geometric_averaging"""

        exp_output_data = [
            ('product_3', "supplier_2", "2019-02-01", 1, 2.7),
            ('product_1', "supplier_1", "2019-01-01", 4, 1.012),
            ('product_1', "supplier_1", "2019-02-01", 5, 2.347),
            ('product_2', "supplier_2", "2019-01-01", 3, 0.888),
            ('product_2', "supplier_2", "2019-02-01", 2, 1.5),
            ('product_3', "supplier_2", "2019-01-01", 3, 2.6),
        ]
        schema = T.StructType([
            T.StructField('product_id', T.StringType(), False),
            T.StructField('supplier', T.StringType(), False),
            T.StructField('month', T.StringType(), False),
            T.StructField('quantity', T.IntegerType(), False),
            T.StructField('price', T.DoubleType(), False)])

        return self.spark.createDataFrame(exp_output_data, schema)

    def expected_output_uaa(self):
        """Create expected output data set for unweighted_arithmetic_averaging"""

        exp_output_data = [
            ('product_3', "supplier_2", "2019-02-01", 2.7, 1),
            ('product_1', "supplier_1", "2019-01-01", 1.067, 1),
            ('product_1', "supplier_1", "2019-02-01", 2.267, 1),
            ('product_2', "supplier_2", "2019-01-01", 0.85, 1),
            ('product_2', "supplier_2", "2019-02-01", 1.5, 1),
            ('product_3', "supplier_2", "2019-01-01", 2.6, 1),
        ]
        schema = T.StructType([
            T.StructField('product_id', T.StringType(), False),
            T.StructField('supplier', T.StringType(), False),
            T.StructField('month', T.StringType(), False),
            T.StructField('price', T.DoubleType(), False),
            T.StructField('quantity', T.IntegerType(), False)])

        return self.spark.createDataFrame(exp_output_data, schema)

    def expected_output_uga(self):
        """Create expected output data set for unweighted_geometric_averaging"""

        exp_output_data = [
            ('product_3', "supplier_2", "2019-02-01", 2.7, 1),
            ('product_1', "supplier_1", "2019-01-01", 1.016, 1),
            ('product_1', "supplier_1", "2019-02-01", 2.192, 1),
            ('product_2', "supplier_2", "2019-01-01", 0.837, 1),
            ('product_2', "supplier_2", "2019-02-01", 1.5, 1),
            ('product_3', "supplier_2", "2019-01-01", 2.6, 1),
        ]
        schema = T.StructType([
            T.StructField('product_id', T.StringType(), False),
            T.StructField('supplier', T.StringType(), False),
            T.StructField('month', T.StringType(), False),
            T.StructField('price', T.DoubleType(), False),
            T.StructField('quantity', T.IntegerType(), False)])

        return self.spark.createDataFrame(exp_output_data, schema)

    def expected_output_left_join(self):
        """Create expected output data set for left_join"""

        exp_output_data = [
            ('product_1', "supplier_1", "2019-02", 1, 14),
            ('product_1', "supplier_1", "2019-02", 6, 14),
            ('product_1', "supplier_1", "2019-02", 7, 14),
            ('product_2', "supplier_2", "2019-02", 5, 10),
            ('product_2', "supplier_2", "2019-02", 5, 10),
            ('product_2', "supplier_2", "2019-01", 2, 3),
            ('product_2', "supplier_2", "2019-01", 1, 3),
            ('product_1', "supplier_1", "2019-01", 7, 13),
            ('product_1', "supplier_1", "2019-01", 1, 13),
            ('product_1', "supplier_1", "2019-01", 5, 13),
            ('product_3', "supplier_2", "2019-02", 7, 7),
            ('product_3', "supplier_2", "2019-01", 6, 6),
        ]
        schema = T.StructType([
            T.StructField('product_id', T.StringType(), False),
            T.StructField('supplier', T.StringType(), False),
            T.StructField('date', T.StringType(), False),
            T.StructField('quantity', T.IntegerType(), False),
            T.StructField('sum(quantity)', T.IntegerType(), False)])

        return self.spark.createDataFrame(exp_output_data, schema)

    def test_weighted_arithmetic_average(self):
        """Unit test for weighted_arithmetic_average"""

        input_df = self.input_data()
        expected_df = self.expected_output_waa()
        result_df = averaging.weighted_arithmetic_average(
            self.spark, input_df, self.groupby_cols)
        self.assertDFAlmostEqual(
            result_df, expected_df, self.cols_to_round, self.rounding_scale)

    def test_weighted_geometric_average(self):
        """Unit test for weighted_arithmetic_average"""

        input_df = self.input_data()
        expected_df = self.expected_output_wga()
        result_df = averaging.weighted_geometric_average(
            self.spark, input_df, self.groupby_cols)
        self.assertDFAlmostEqual(
            result_df, expected_df, self.cols_to_round, self.rounding_scale)

    def test_unweighted_arithmetic_average(self):
        """Unit test for weighted_arithmetic_average"""

        input_df = self.input_data()
        expected_df = self.expected_output_uaa()
        result_df = averaging.unweighted_arithmetic_average(
            self.spark, input_df, self.groupby_cols)
        self.assertDFAlmostEqual(
            result_df, expected_df, self.cols_to_round, self.rounding_scale)

    def test_unweighted_geometric_average(self):
        """Unit test for weighted_geometric_average"""

        input_df = self.input_data()
        expected_df = self.expected_output_uga()
        result_df = averaging.unweighted_geometric_average(
            self.spark, input_df, self.groupby_cols)
        self.assertDFAlmostEqual(
            result_df, expected_df, self.cols_to_round, self.rounding_scale)

    def test_left_join(self):
        """Unit test for left_join()"""

        df1, df2 = self.input_data_left_join()
        expected_df = self.expected_output_left_join()
        result_df = averaging.left_join(self.spark, df1, df2, self.join_cols)
        self.assertDFEqual(result_df, expected_df)

if __name__ == "__main__":

    suite = unittest.TestSuite()
    #unittest.main(exit=False)

    suite.addTest(TestAveraging("test_weighted_arithmetic_average"))
    suite.addTest(TestAveraging("test_weighted_geometric_average"))
    suite.addTest(TestAveraging("test_unweighted_arithmetic_average"))
    suite.addTest(TestAveraging("test_unweighted_geometric_average"))
    suite.addTest(TestAveraging("test_left_join"))

    runner = unittest.TextTestRunner()
    runner.run(suite)
