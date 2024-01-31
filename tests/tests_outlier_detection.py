# Import pyspark packages
from pyspark.sql import functions as F
from pyspark.sql import types as T
from epds_utils.testing.pyspark_test_case import PySparkTest

# Import python packages
import unittest
from importlib import reload

#Import module to test
from cprices.cprices.steps import outlier_detection
reload(outlier_detection)

class TestOutliering(PySparkTest):
    """Test class for outlier detection methods."""

    def input_data(self):
        """Creates input dataframe."""

        self.groupby_cols = ['item']
        self.column = 'price'
        self.k = 2
        self.fences = {'item_1' : [1.5, 6.0]}

        input_data = [
            ('item_1', -1000.7,),
            ('item_1', 1.0,),
            ('item_1', 1.5,),
            ('item_1', 2.5,),
            ('item_1', 2.6,),
            ('item_1', 2.7,),
            ('item_1', 3.8,),
            ('item_1', 3.9,),
            ('item_1', 4.6,),
            ('item_1', 4.7,),
            ('item_1', 5.0,),
            ('item_1', 30.5,),
            ('item_1', 70.1,),
            ('item_1', 200.5,),
            ('item_1', None, ),
            ('item_1', None, ),
        ]
        schema = T.StructType([
            T.StructField('item', T.StringType(), False),
            T.StructField('price', T.DoubleType(), True)
        ])
        return self.spark.createDataFrame(input_data, schema)

    def expected_output_tukey(self):
        """Creates expected output dataframe for tukey."""

        tukey_expected = [
            ('item_1', -1000.7, 1),
            ('item_1', 1.0, 0),
            ('item_1', 1.5, 0),
            ('item_1', 2.5, 0),
            ('item_1', 2.6, 0),
            ('item_1', 2.7, 0),
            ('item_1', 3.8, 0),
            ('item_1', 3.9, 0),
            ('item_1', 4.6, 0),
            ('item_1', 4.7, 0),
            ('item_1', 5.0, 0),
            ('item_1', 30.5, 1),
            ('item_1', 70.1, 1),
            ('item_1', 200.5, 1),
            ('item_1', None, None),
            ('item_1', None, None),
        ]
        schema = T.StructType([
            T.StructField('item', T.StringType(), False),
            T.StructField('price', T.DoubleType(), True),
            T.StructField('outlier', T.IntegerType(), True),
        ])
        return self.spark.createDataFrame(tukey_expected, schema)

    def expected_output_ksigma(self):
        """Creates expected output dataframe for ksigma."""

        ksigma_expected = [
            ('item_1', -1000.7, 1),
            ('item_1', 1.0, 0),
            ('item_1', 1.5, 0),
            ('item_1', 2.5, 0),
            ('item_1', 2.6, 0),
            ('item_1', 2.7, 0),
            ('item_1', 3.8, 0),
            ('item_1', 3.9, 0),
            ('item_1', 4.6, 0),
            ('item_1', 4.7, 0),
            ('item_1', 5.0, 0),
            ('item_1', 30.5, 0),
            ('item_1', 70.1, 0),
            ('item_1', 200.5, 0),
            ('item_1', None, None),
            ('item_1', None, None),
        ]
        schema = T.StructType([
            T.StructField('item', T.StringType(), False),
            T.StructField('price', T.DoubleType(), True),
            T.StructField('outlier', T.IntegerType(), True),
        ])
        return self.spark.createDataFrame(ksigma_expected, schema)

    def expected_output_kimber(self):
        """Creates expected output dataframe for kimber."""

        kimber_expected = [
            ('item_1', -1000.7, 1),
            ('item_1', 1.0, 0),
            ('item_1', 1.5, 0),
            ('item_1', 2.5, 0),
            ('item_1', 2.6, 0),
            ('item_1', 2.7, 0),
            ('item_1', 3.8, 0),
            ('item_1', 3.9, 0),
            ('item_1', 4.6, 0),
            ('item_1', 4.7, 0),
            ('item_1', 5.0, 0),
            ('item_1', 30.5, 1),
            ('item_1', 70.1, 1),
            ('item_1', 200.5, 1),
            ('item_1', None, None),
            ('item_1', None, None),
        ]
        schema = T.StructType([
            T.StructField('item', T.StringType(), False),
            T.StructField('price', T.DoubleType(), True),
            T.StructField('outlier', T.IntegerType(), True),
        ])
        return self.spark.createDataFrame(kimber_expected, schema)

    def expected_output_udf_fences(self):
        """Creates expected output dataframe for udf_fences."""

        udf_fences_expected = [
            ('item_1', -1000.7, 1),
            ('item_1', 1.0, 1),
            ('item_1', 1.5, 0),
            ('item_1', 2.5, 0),
            ('item_1', 2.6, 0),
            ('item_1', 2.7, 0),
            ('item_1', 3.8, 0),
            ('item_1', 3.9, 0),
            ('item_1', 4.6, 0),
            ('item_1', 4.7, 0),
            ('item_1', 5.0, 0),
            ('item_1', 30.5, 1),
            ('item_1', 70.1, 1),
            ('item_1', 200.5, 1),
            ('item_1', None, None),
            ('item_1', None, None),
        ]
        schema = T.StructType([
            T.StructField('item', T.StringType(), False),
            T.StructField('price', T.DoubleType(), True),
            T.StructField('outlier', T.IntegerType(), True),
        ])
        return self.spark.createDataFrame(udf_fences_expected, schema)

    def test_tukey(self):
        """Unit test for tukey."""

        df_input = self.input_data()
        df_tukey_expected = self.expected_output_tukey()
        df_tukey_output = outlier_detection.tukey(
            df_input,
            self.groupby_cols,
            self.column,
            self.k,
            self.spark
        )
        self.assertDFEqual(df_tukey_output, df_tukey_expected)

    def test_ksigma(self):
        """Unit test for ksigma."""

        df_input = self.input_data()
        df_ksigma_expected = self.expected_output_ksigma()
        df_ksigma_output = outlier_detection.ksigma(
            df_input,
            self.groupby_cols,
            self.column,
            self.k
        )
        self.assertDFEqual(df_ksigma_output, df_ksigma_expected)

    def test_kimber(self):
        """Unit test for kimber."""

        df_input = self.input_data()
        df_kimber_expected = self.expected_output_kimber()
        df_kimber_output = outlier_detection.kimber(
            df_input,
            self.groupby_cols,
            self.column,
            self.k,
            self.spark
        )
        self.assertDFEqual(df_kimber_output, df_kimber_expected)

    def test_udf_fences(self):
        """Unit test for udf_fences."""

        df_input = self.input_data()
        df_udf_fences_expected = self.expected_output_udf_fences()
        df_udf_fences_output = outlier_detection.udf_fences(
            self.spark,
            df_input,
            self.column,
            self.fences
        )
        self.assertDFEqual(df_udf_fences_output, df_udf_fences_expected)

if __name__ == '__main__':

    suite = unittest.TestSuite()
    #unittest.main(exit=False)

    suite.addTest(TestOutliering("test_tukey"))
    suite.addTest(TestOutliering("test_ksigma"))
    suite.addTest(TestOutliering("test_kimber"))
    suite.addTest(TestOutliering("test_udf_fences"))

    runner = unittest.TextTestRunner()
    runner.run(suite)
