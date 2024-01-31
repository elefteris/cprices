# Import pyspark packages
from pyspark.sql import types as T
from epds_utils.testing.pyspark_test_case import PySparkTest

# Import python packages
import pandas as pd
import os
import subprocess
import unittest
from importlib import reload

# import helper module
from epds_utils.hdfs import hdfs_utils

#import module to test
from cprices.cprices.steps import utils
reload(utils)



class TestUtils(PySparkTest):
    """Test class for utils functions: create_dataframe and find."""

    def setUp(self):
        base_dir = r"/dap/workspace_zone/prices_webscraping/pipeline_data/test/"
        filename = 'csv_dat'
        extension = '.csv'
        filedir = os.path.join(base_dir, filename)
        self.filepath = os.path.join(filedir, filename+extension)

        csv_data = [('a',1.99), ('b',2.99), ('c',3.99)]
        schema = T.StructType([
            T.StructField('col1', T.StringType(), False),
            T.StructField('col2', T.DoubleType(), False)
        ])
        df_csv = self.spark.createDataFrame(csv_data, schema)
        hdfs_utils.write_csv_rename(df_csv, filedir)

    def input_data_find(self):
        """Create unit test input data for find"""

        input_data = [(12345, 1), (23456, 2)]
        schema = T.StructType([
            T.StructField("col1", T.IntegerType(), False),
            T.StructField("col2", T.IntegerType(), False)
        ])
        df1 = self.spark.createDataFrame(input_data, schema)

        input_data = [(10000, 1), (20000, 2)]
        schema = T.StructType([
            T.StructField("col1", T.IntegerType(), False),
            T.StructField("col2", T.IntegerType(), False)
        ])
        df2 = self.spark.createDataFrame(input_data, schema)

        dictionary = {
            "key1" : "value1",
            "key2": {
                "subkey1" : df1,
                "subkey2" : {
                    "subkey3" : df2,
                    "subkey4" : "value2"
                }
            }
        }

        return df1, df2, dictionary

    def test_create_dataframe(self):
        """Unit test for create_dataframe"""

        #Create a pandas dataframe
        data = [["A", "B", "C"]]
        column_names = ["1", "2", "3"]
        pdf = utils.create_dataframe(data, column_names)

        # Create comparison pandas dataframe
        pdf_comp = pd.DataFrame({"1": ["A"], "2": ["B"], "3": ["C"]})

        self.assertEqual(pdf.equals(pdf_comp), True)


    def test_find(self):
        """Unit test for find"""

        df1, df2, dictionary = self.input_data_find()

        # Subtest 1
        key = "key1"
        dictionary = {"key1" : "value1"}
        value = "value1"
        generator = utils.find(key, dictionary)
        for v in generator:
            self.assertEqual(value, v)

        # Subtest 2
        key = "subkey1"
        generator = utils.find(key, dictionary)
        for v in generator:
            self.assertDFEqual(df1, v, rounding_scale = 3)

        # Subtest 3
        key = "subkey3"
        generator = utils.find(key, dictionary)
        for v in generator:
            self.assertDFEqual(df2, v, rounding_scale = 3)

    def test_hdfs_read_csv(self):
        """Unit test for hdfs_read_csv"""

        pdf = utils.hdfs_read_csv(self.filepath)

        # Create comparison pandas dataframe
        pdf_comp = pd.DataFrame({
            "col1": ['a', 'b', 'c'],
            "col2": ['1.99', '2.99', '3.99']
        })
        self.assertEqual(pdf.equals(pdf_comp), True)

    def tearDown(self):
        "Delete temporary folder with test data from hdfs"
        subprocess.call(["hadoop","fs","-rm","-r", self.filepath])

if __name__ == "__main__":

    suite = unittest.TestSuite()
    #unittest.main(exit=False)

    suite.addTest(TestUtils("test_create_dataframe"))
    suite.addTest(TestUtils("test_find"))
    suite.addTest(TestUtils("test_hdfs_read_csv"))

    runner = unittest.TextTestRunner()
    runner.run(suite)
