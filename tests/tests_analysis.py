# import spark libraries
from pyspark.sql import functions as F
from pyspark.sql import types as T
from epds_utils.testing.pyspark_test_case import PySparkTest

# import python libraries
import unittest
from importlib import reload

# import module to test
from cprices.cprices.steps import analysis
reload(analysis)

class TestAnalysis(PySparkTest):
    """Test class for analysis."""

    def setUp(self):
        self.column = 'price'
        self.n_bins = 5
        self.groupby_cols = ['gcol']
        self.flag_col = 'flag'

    def input_data_get_binary_flag_stats(self):
        """Create input data set for get_binary_flag_stats"""

        input_data = [
            ('a', '2019-01-01', 0),
            ('a', '2019-01-01', 1),
            ('a', '2019-01-01', 1),
            ('a', '2019-01-01', 1),
            ('a', '2019-02-01', 0),
            ('a', '2019-02-01', 1),
            ('a', '2019-02-01', 1),
            ('a', '2019-02-01', 0),
            ('b', '2019-01-01', 0),
            ('b', '2019-01-01', 0),
            ('b', '2019-01-01', 1),
            ('b', '2019-01-01', 0),
            ('b', '2019-02-01', 0),
            ('b', '2019-02-01', 0),
            ('b', '2019-02-01', 1),
            ('b', '2019-02-01', 1),
        ]
        schema = T.StructType([
            T.StructField('gcol', T.StringType(), False),
            T.StructField('month', T.StringType(), False),
            T.StructField('flag', T.IntegerType(), False)
        ])
        return self.spark.createDataFrame(input_data, schema)

    def input_data_get_churn_stats(self):
        """Create input data set for get_churn_stats"""

        input_data = [
            ('a', '2019-01-01','lenovo tablet',100.0),
            ('a', '2019-01-01','lenovo laptop',500.0),
            ('a', '2019-01-01','hp desktop',800.0),
            ('a', '2019-01-01','hp laptop',550.0),
            ('a', '2019-01-01','dell desktop',600.0),
            ('a', '2019-01-01','dell laptop',350.0),
            ('a', '2019-01-01','acer laptop',425.0),
            ('a', '2019-01-01','razer gaming desktop',850.0),

            ('a', '2019-02-01','lenovo tablet',110.0),
            ('a', '2019-02-01','lenovo laptop',490.0),
            ('a', '2019-02-01','hp desktop',825.0),
            ('a', '2019-02-01','hp laptop',560.0),
            ('a', '2019-02-01','dell desktop',580.0),
            ('a', '2019-02-01','dell laptop',370.0),
            ('a', '2019-02-01','hp laptop2',415.0),
            ('a', '2019-02-01','razer gaming desktop',840.0),

            ('a', '2019-03-01','lenovo tablet',100.0),
            ('a', '2019-03-01','razer laptop',540.0),
            ('a', '2019-03-01','hp desktop',800.0),
            ('a', '2019-03-01','hp laptop',550.0),
            ('a', '2019-03-01','dell desktop',590.0),
            ('a', '2019-03-01','dell laptop',360.0),
            ('a', '2019-03-01','hp laptop2',405.0),
            ('a', '2019-03-01','razer gaming desktop',820.0),

            ('a', '2019-04-01','lenovo tablet',110.0),
            ('a', '2019-04-01','razer laptop',530.0),
            ('a', '2019-04-01','hp desktop',825.0),
            ('a', '2019-04-01','hp laptop',560.0),
            ('a', '2019-04-01','dell desktop',550.0),
            ('a', '2019-04-01','lenovo laptop',350.0),
            ('a', '2019-04-01','hp laptop2',395.0),
            ('a', '2019-04-01','razer gaming desktop',800.0),

            ('b', '2019-01-01','lenovo tablet',100.0),
            ('b', '2019-01-01','lenovo laptop',500.0),
            ('b', '2019-01-01','lenovo tablet2',150.0),
            ('b', '2019-01-01','lenovo laptop2',550.0),
            ('b', '2019-01-01','dell desktop',600.0),
            ('b', '2019-01-01','dell laptop',350.0),
            ('b', '2019-01-01','acer laptop',425.0),

            ('b', '2019-02-01','lenovo tablet',110.0),
            ('b', '2019-02-01','lenovo laptop',490.0),
            ('b', '2019-02-01','lenovo tablet2',145.0),
            ('b', '2019-02-01','lenovo laptop2',560.0),
            ('b', '2019-02-01','dell desktop',580.0),
            ('b', '2019-02-01','dell laptop',370.0),
            ('b', '2019-02-01','hp laptop',415.0),

            ('b', '2019-03-01','lenovo tablet',100.0),
            ('b', '2019-03-01','razer laptop',540.0),
            ('b', '2019-03-01','lenovo tablet2',140.0),
            ('b', '2019-03-01','lenovo laptop2',550.0),
            ('b', '2019-03-01','dell desktop',590.0),
            ('b', '2019-03-01','dell laptop',360.0),
            ('b', '2019-03-01','hp laptop',405.0),

            ('b', '2019-04-01','lenovo tablet',110.0),
            ('b', '2019-04-01','razer laptop',530.0),
            ('b', '2019-04-01','lenovo tablet2',135.0),
            ('b', '2019-04-01','lenovo laptop2',560.0),
            ('b', '2019-04-01','dell desktop',550.0),
            ('b', '2019-04-01','dell laptop',350.0),
            ('b', '2019-04-01','hp laptop',395.0),
        ]

        schema = T.StructType([
            T.StructField('gcol', T.StringType(), False),
            T.StructField('month', T.StringType(), False),
            T.StructField('product_id', T.StringType(), False),
            T.StructField('price', T.DoubleType(), False),
        ])
        return self.spark.createDataFrame(input_data, schema)

    def input_data_get_price_stats(self):
        """Create input data set for get_price_stats"""

        input_data = [
            ('a', '2019-01-01','lenovo tablet',100.0),
            ('a', '2019-01-01','lenovo laptop',500.0),
            ('a', '2019-01-01','hp desktop',800.0),
            ('a', '2019-01-01','hp laptop',550.0),
            ('a', '2019-02-01','lenovo tablet',110.0),
            ('a', '2019-02-01','lenovo laptop',490.0),
            ('a', '2019-02-01','hp desktop',825.0),
            ('a', '2019-02-01','hp laptop',560.0),
            ('b', '2019-01-01','lenovo tablet',120.0),
            ('b', '2019-01-01','lenovo laptop',400.0),
            ('b', '2019-01-01','hp desktop',650.0),
            ('b', '2019-01-01','hp laptop',300.0),
            ('b', '2019-02-01','lenovo tablet',90.0),
            ('b', '2019-02-01','lenovo laptop',410.0),
            ('b', '2019-02-01','hp desktop',655.0),
            ('b', '2019-02-01','hp laptop',310.0),
        ]
        schema = T.StructType([
            T.StructField('gcol', T.StringType(), False),
            T.StructField('month', T.StringType(), False),
            T.StructField('product_id', T.StringType(), False),
            T.StructField('price', T.DoubleType(), False),
        ])
        return self.spark.createDataFrame(input_data, schema)

    def input_data_get_sample_count(self):
        """Create input data set for get_churn_stats"""

        input_data = [
            ('a', '2019-01-01','lenovo tablet',100.0),
            ('a', '2019-01-01','lenovo laptop',500.0),
            ('a', '2019-01-01','hp desktop',800.0),
            ('a', '2019-01-01','hp laptop',550.0),
            ('a', '2019-01-01','dell desktop',600.0),
            ('a', '2019-01-01','dell laptop',350.0),
            ('a', '2019-01-01','acer laptop',425.0),
            ('a', '2019-01-01','razer gaming desktop',850.0),

            ('a', '2019-02-01','lenovo tablet',110.0),
            ('a', '2019-02-01','lenovo laptop',490.0),
            ('a', '2019-02-01','hp desktop',825.0),
            ('a', '2019-02-01','hp laptop',560.0),
            ('a', '2019-02-01','dell desktop',580.0),
            ('a', '2019-02-01','dell laptop',370.0),
            ('a', '2019-02-01','hp laptop2',415.0),
            ('a', '2019-02-01','razer gaming desktop',840.0),

            ('a', '2019-03-01','lenovo tablet',100.0),
            ('a', '2019-03-01','razer laptop',540.0),
            ('a', '2019-03-01','hp desktop',800.0),
            ('a', '2019-03-01','hp laptop',550.0),
            ('a', '2019-03-01','dell desktop',590.0),
            ('a', '2019-03-01','dell laptop',360.0),
            ('a', '2019-03-01','hp laptop2',405.0),
            ('a', '2019-03-01','razer gaming desktop',820.0),

            ('a', '2019-04-01','lenovo tablet',110.0),
            ('a', '2019-04-01','razer laptop',530.0),
            ('a', '2019-04-01','hp desktop',825.0),
            ('a', '2019-04-01','hp laptop',560.0),
            ('a', '2019-04-01','dell desktop',550.0),
            ('a', '2019-04-01','lenovo laptop',350.0),
            ('a', '2019-04-01','hp laptop2',395.0),
            ('a', '2019-04-01','razer gaming desktop',800.0),

            ('b', '2019-01-01','lenovo tablet',100.0),
            ('b', '2019-01-01','lenovo laptop',500.0),
            ('b', '2019-01-01','lenovo tablet2',150.0),
            ('b', '2019-01-01','lenovo laptop2',550.0),
            ('b', '2019-01-01','dell desktop',600.0),
            ('b', '2019-01-01','dell laptop',350.0),
            ('b', '2019-01-01','acer laptop',425.0),

            ('b', '2019-02-01','lenovo tablet',110.0),
            ('b', '2019-02-01','lenovo laptop',490.0),
            ('b', '2019-02-01','lenovo tablet2',145.0),
            ('b', '2019-02-01','lenovo laptop2',560.0),
            ('b', '2019-02-01','dell desktop',580.0),
            ('b', '2019-02-01','dell laptop',370.0),
            ('b', '2019-02-01','hp laptop',415.0),

            ('b', '2019-03-01','lenovo tablet',100.0),
            ('b', '2019-03-01','razer laptop',540.0),
            ('b', '2019-03-01','lenovo tablet2',140.0),
            ('b', '2019-03-01','lenovo laptop2',550.0),
            ('b', '2019-03-01','dell desktop',590.0),
            ('b', '2019-03-01','dell laptop',360.0),
            ('b', '2019-03-01','hp laptop',405.0),

            ('b', '2019-04-01','lenovo tablet',110.0),
            ('b', '2019-04-01','razer laptop',530.0),
            ('b', '2019-04-01','lenovo tablet2',135.0),
            ('b', '2019-04-01','lenovo laptop2',560.0),
            ('b', '2019-04-01','dell desktop',550.0),
            ('b', '2019-04-01','dell laptop',350.0),
            ('b', '2019-04-01','hp laptop',395.0),
        ]

        schema = T.StructType([
            T.StructField('gcol', T.StringType(), False),
            T.StructField('month', T.StringType(), False),
            T.StructField('product_id', T.StringType(), False),
            T.StructField('price', T.DoubleType(), False),
        ])
        return self.spark.createDataFrame(input_data, schema)

    def input_data_distributions(self):
        """Create input data set for distributions"""

        input_data = [
            ("a", 40.0),
            ("a", 50.0),
            ("a", 55.0),
            ("a", 110.0),
            ("a", 110.0),
            ("a", 120.0),
            ("b", 10.0),
            ("b", 20.0),
            ("b", 30.0),
            ("b", 40.0),
            ("b", 40.0),
            ("b", 60.0),
        ]
        schema = T.StructType([
            T.StructField('gcol', T.StringType(), False),
            T.StructField('price', T.DoubleType(), False),
        ])
        return self.spark.createDataFrame(input_data, schema)


    def expected_output_get_binary_flag_stats(self):
        """Create expected output data set for get_binary_flag_stats"""

        expout_data = [
            ("a", "2019-01-01", 4.0, "count"),
            ("b", "2019-01-01", 4.0, "count"),
            ("b", "2019-02-01", 4.0, "count"),
            ("a", "2019-02-01", 4.0, "count"),
            ("a", "2019-01-01", 3.0, "count1"),
            ("b", "2019-01-01", 1.0, "count1"),
            ("b", "2019-02-01", 2.0, "count1"),
            ("a", "2019-02-01", 2.0, "count1"),
            ("a", "2019-01-01", 1.0, "count0"),
            ("b", "2019-01-01", 3.0, "count0"),
            ("b", "2019-02-01", 2.0, "count0"),
            ("a", "2019-02-01", 2.0, "count0"),
            ("a", "2019-01-01", 0.75, "frac1"),
            ("b", "2019-01-01", 0.25, "frac1"),
            ("b", "2019-02-01", 0.5, "frac1"),
            ("a", "2019-02-01", 0.5, "frac1")
        ]
        schema = T.StructType([
            T.StructField('gcol', T.StringType(), False),
            T.StructField('month', T.StringType(), False),
            T.StructField('value', T.DoubleType(), False),
            T.StructField('metric', T.StringType(), False),
        ])
        return self.spark.createDataFrame(expout_data, schema)

    def expected_output_get_churn_stats(self):
        """Create expected output data set for get_churn_stats"""

        expout_data = [
            ("b",7.0,"2019-01-01","count"),
            ("b",7.0,"2019-02-01","count"),
            ("b",7.0,"2019-03-01","count"),
            ("b",7.0,"2019-04-01","count"),
            ("b",1.0,"2019-02-01","entered_count"),
            ("b",1.0,"2019-02-01","dropped_count"),
            ("b",1.0,"2019-03-01","entered_count"),
            ("b",1.0,"2019-03-01","dropped_count"),
            ("b",0.0,"2019-04-01","entered_count"),
            ("b",0.0,"2019-04-01","dropped_count"),
            ("b",0.14285714285714285,"2019-02-01","entered_frac"),
            ("b",0.14285714285714285,"2019-02-01","dropped_frac"),
            ("b",0.14285714285714285,"2019-03-01","entered_frac"),
            ("b",0.14285714285714285,"2019-03-01","dropped_frac"),
            ("b",0.0,"2019-04-01","entered_frac"),
            ("b",0.0,"2019-04-01","dropped_frac"),
            ("a",8.0,"2019-01-01","count"),
            ("a",8.0,"2019-02-01","count"),
            ("a",8.0,"2019-03-01","count"),
            ("a",8.0,"2019-04-01","count"),
            ("a",1.0,"2019-02-01","entered_count"),
            ("a",1.0,"2019-02-01","dropped_count"),
            ("a",1.0,"2019-03-01","entered_count"),
            ("a",1.0,"2019-03-01","dropped_count"),
            ("a",1.0,"2019-04-01","entered_count"),
            ("a",1.0,"2019-04-01","dropped_count"),
            ("a",0.125,"2019-02-01","entered_frac"),
            ("a",0.125,"2019-02-01","dropped_frac"),
            ("a",0.125,"2019-03-01","entered_frac"),
            ("a",0.125,"2019-03-01","dropped_frac"),
            ("a",0.125,"2019-04-01","entered_frac"),
            ("a",0.125,"2019-04-01","dropped_frac")
        ]
        schema = T.StructType([
            T.StructField('gcol', T.StringType(), False),
            T.StructField('value', T.DoubleType(), False),
            T.StructField('month', T.StringType(), False),
            T.StructField('metric', T.StringType(), False),
        ])
        return self.spark.createDataFrame(expout_data, schema)

    def expected_output_get_price_stats(self):
        """Create expected output data set for get_price_stats"""

        expout_data = [
            ("count",4.0,"a","2019-01-01"),
            ("count",4.0,"a","2019-02-01"),
            ("count",4.0,"b","2019-01-01"),
            ("count",4.0,"b","2019-02-01"),
            ("mean",487.5,"a","2019-01-01"),
            ("mean",496.25,"a","2019-02-01"),
            ("mean",367.5,"b","2019-01-01"),
            ("mean",366.25,"b","2019-02-01"),
            ("stddev",289.75564417856185,"a","2019-01-01"),
            ("stddev",295.1659137953884,"a","2019-02-01"),
            ("stddev",221.1146007541489,"b","2019-01-01"),
            ("stddev",234.35638814990017,"b","2019-02-01"),
            ("min",100.0,"a","2019-01-01"),
            ("min",110.0,"a","2019-02-01"),
            ("min",120.0,"b","2019-01-01"),
            ("min",90.0,"b","2019-02-01"),
            ("max",800.0,"a","2019-01-01"),
            ("max",825.0,"a","2019-02-01"),
            ("max",650.0,"b","2019-01-01"),
            ("max",655.0,"b","2019-02-01")
        ]
        schema = T.StructType([
            T.StructField('metric', T.StringType(), False),
            T.StructField('value', T.DoubleType(), False),
            T.StructField('gcol', T.StringType(), False),
            T.StructField('month', T.StringType(), False),
        ])
        return self.spark.createDataFrame(expout_data, schema)

    def expected_output_get_sample_count(self):
        """Create expected output data set for expected_output_get_sample_count"""

        expout_data = [
            ("a", "2019-02-01", "2019-01-01", 7.0),
            ("a", "2019-03-01", "2019-02-01", 7.0),
            ("a", "2019-04-01", "2019-03-01", 7.0),
            ("b", "2019-02-01", "2019-01-01", 6.0),
            ("b", "2019-03-01", "2019-02-01", 6.0),
            ("b", "2019-04-01", "2019-03-01", 7.0),
            ("a", "2019-03-01", "2019-01-01", 6.0),
            ("a", "2019-04-01", "2019-01-01", 6.0),
            ("b", "2019-03-01", "2019-01-01", 5.0),
            ("b", "2019-04-01", "2019-01-01", 5.0),
        ]
        schema = T.StructType([
            T.StructField('gcol', T.StringType(), False),
            T.StructField('month', T.StringType(), False),
            T.StructField('metric', T.StringType(), False),
            T.StructField('value', T.DoubleType(), False),
        ])
        return self.spark.createDataFrame(expout_data, schema)

    def expected_output_distributions(self):
        """Create expected output data set for distributions"""

        expout_data = [
            ("a", 56.0, 3),
            ("a", 72.0, 0),
            ("a", 88.0, 0),
            ("a", 104.0, 0),
            ("a", 120.0, 3),
            ("b", 20.0, 2),
            ("b", 30.0, 1),
            ("b", 40.0, 2),
            ("b", 50.0, 0),
            ("b", 60.0, 1),
        ]
        schema = T.StructType([
            T.StructField('gcol', T.StringType(), False),
            T.StructField('price', T.DoubleType(), False),
            T.StructField('count', T.IntegerType(), False),
        ])
        return self.spark.createDataFrame(expout_data, schema)

    def test_get_binary_flag_stats(self):
        """Unit test for get_binary_flag_stats"""
        input_df = self.input_data_get_binary_flag_stats()
        expected_df = self.expected_output_get_binary_flag_stats()
        result_df = analysis.get_binary_flag_stats(
            input_df, self.groupby_cols, self.flag_col)
        self.assertDFEqual(result_df, expected_df)

    def test_get_churn_stats(self):
        """Unit test for get_churn_stats"""
        input_df = self.input_data_get_churn_stats()
        expected_df = self.expected_output_get_churn_stats()
        result_df = analysis.get_churn_stats(input_df, self.groupby_cols)
        self.assertDFEqual(result_df, expected_df)

    def test_get_price_stats(self):
        """Unit test for get_price_stats"""
        input_df = self.input_data_get_price_stats()
        expected_df = self.expected_output_get_price_stats()
        result_df = analysis.get_price_stats(input_df, self.groupby_cols)
        self.assertDFEqualUnordered(result_df, expected_df)

    def test_get_sample_count(self):
        """Unit test for get_sample_count"""
        input_df = self.input_data_get_sample_count()
        expected_df = self.expected_output_get_sample_count()
        result_df = analysis.get_sample_count(input_df, self.groupby_cols)
        self.assertDFEqual(result_df, expected_df)

    def test_distributions(self):
        """Unit test for distributions"""
        input_df = self.input_data_distributions()
        expected_df = self.expected_output_distributions()
        result_df = analysis.distributions(input_df, self.groupby_cols, self.column, self.n_bins)
        self.assertDFEqual(result_df, expected_df)


if __name__ == "__main__":

    suite = unittest.TestSuite()
    #unittest.main(exit=False)

    suite.addTest(TestAnalysis("test_get_binary_flag_stats"))
    suite.addTest(TestAnalysis("test_get_churn_stats"))
    suite.addTest(TestAnalysis("test_get_price_stats"))
    suite.addTest(TestAnalysis("test_get_sample_count"))
    suite.addTest(TestAnalysis("test_distributions"))

    runner = unittest.TextTestRunner()
    runner.run(suite)
