# Import pyspark packages
from pyspark.sql import functions as F
from pyspark.sql import types as T
from epds_utils.testing.pyspark_test_case import PySparkTest

# Import python packages
import unittest
from importlib import reload

#Import module to test
from cprices.cprices.steps import classification
reload(classification)

class TestClassification(PySparkTest):
    """Test class for classification module."""

    def setUp(self):
        self.keywords = ['tablet', 'refurbished', 'bag']

    def input_data(self):
        """Input data set for classify_name_using_keywords"""

        input_data = [
            ('lenovo tablet',),
            ('asus laptop refurbished',),
            ('dell laptop 13" screen',),
            (None,),
            ('laptop bag',),
            ('hp 15" laptop 2.4 GHz quad core',),
            ('   ',),
            (None,),
            ('razer gaming laptop 4.0 GHz 8 core 17" screen',)
        ]
        schema = T.StructType([
            T.StructField('product_name', T.StringType(), True),
        ])
        df_input = self.spark.createDataFrame(input_data, schema)

        return df_input

    def expected_output(self):
        """Expected output data set for classify_name_using_keywords"""

        input_data = [
            ('lenovo tablet', 1),
            ('asus laptop refurbished', 1),
            ('dell laptop 13" screen', 0),
            (None, 1),
            ('laptop bag', 1),
            ('hp 15" laptop 2.4 GHz quad core', 0),
            ('   ', 1),
            (None, 1),
            ('razer gaming laptop 4.0 GHz 8 core 17" screen', 0)
        ]
        schema = T.StructType([
            T.StructField('product_name', T.StringType(), True),
            T.StructField('classified', T.IntegerType(), False)
        ])

        return self.spark.createDataFrame(input_data, schema)

    def test_classify_name_using_keywords(self):
        """Unit test for classify_name_using_keywords"""

        input_df = self.input_data()
        expected_df = self.expected_output()
        result_df = classification.classify_name_using_keywords(
            input_df, self.keywords)
        self.assertDFEqual(result_df, expected_df)

if __name__ == "__main__":

    suite = unittest.TestSuite()
    #unittest.main(exit=False)

    suite.addTest(TestClassification("test_classify_name_using_keywords"))

    runner = unittest.TextTestRunner()
    runner.run(suite)
