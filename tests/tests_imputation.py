# Import pyspark packages
from pyspark.sql import types as T
from epds_utils.testing.pyspark_test_case import PySparkTest

# Import python packages
import unittest
from importlib import reload

#Import module to test
from cprices.cprices.steps import imputation
reload(imputation)

class TestImputation(PySparkTest):
    """Test class for imputation."""

    def setUp(self):
        self.groupby_cols = ['gcol']

    def input_data_imputation(self):
        """Input data set for all unit tests
        Casts price and quantity to doubles"""

        input_data = [
            ("a","prod2","01/01/2019",12.99,100),
            ("a","prod1","01/03/2019",790.0,1),
            ("a","prod1","01/04/2019",787.0,1),
            ("a","prod1","01/06/2019",783.0,3),
            ("a","prod2","01/06/2019",11.29,102),
            ("b","prod1","01/01/2019",650.0,1),
            ("b","prod2","01/01/2019",20.99,107),
            ("b","prod2","01/02/2019",20.59,119),
            ("b","prod1","01/03/2019",635.0,3),
            ("b","prod1","01/05/2019",628.0,2),
            ("b","prod1","01/06/2019",625.0,2),
            ("b","prod2","01/06/2019",19.29,122)
        ]
        schema = T.StructType([
            T.StructField('gcol', T.StringType(), False),
            T.StructField('product_id', T.StringType(), False),
            T.StructField('month', T.StringType(), False),
            T.StructField('price', T.DoubleType(), False),
            T.StructField('quantity', T.IntegerType(), False),
        ])
        return self.spark.createDataFrame(input_data, schema)


    def expected_output_imputation(self):
        """Output data set for imputation 3 months imputed"""

        expout_data = [
            ("01/03/2019",790.0,"a","prod1",1,0),
            ("01/04/2019",787.0,"a","prod1",1,0),
            ("01/05/2019",787.0,"a","prod1",1,1),
            ("01/06/2019",783.0,"a","prod1",3,0),
            ("01/01/2019",12.99,"a","prod2",100,0),
            ("01/02/2019",12.99,"a","prod2",100,1),
            ("01/03/2019",12.99,"a","prod2",100,1),
            ("01/04/2019",12.99,"a","prod2",100,1),
            ("01/06/2019",11.29,"a","prod2",102,0),
            ("01/01/2019",650.0,"b","prod1",1,0),
            ("01/02/2019",650.0,"b","prod1",1,1),
            ("01/03/2019",635.0,"b","prod1",3,0),
            ("01/04/2019",635.0,"b","prod1",3,1),
            ("01/05/2019",628.0,"b","prod1",2,0),
            ("01/06/2019",625.0,"b","prod1",2,0),
            ("01/01/2019",20.99,"b","prod2",107,0),
            ("01/02/2019",20.59,"b","prod2",119,0),
            ("01/03/2019",20.59,"b","prod2",119,1),
            ("01/04/2019",20.59,"b","prod2",119,1),
            ("01/05/2019",20.59,"b","prod2",119,1),
            ("01/06/2019",19.29,"b","prod2",122,0)
        ]
        schema = T.StructType([
            T.StructField('month', T.StringType(), False),
            T.StructField('price', T.DoubleType(), False),
            T.StructField('gcol', T.StringType(), False),
            T.StructField('product_id', T.StringType(), False),
            T.StructField('quantity', T.IntegerType(), False),
            T.StructField('imputed', T.IntegerType(), False),
        ])
        return self.spark.createDataFrame(expout_data, schema)


    def expected_output_imputation_2months(self):
        """Output data set for imputation 2 months imputed"""

        expout_data = [
            ("01/03/2019",790.0,"a","prod1",1,0),
            ("01/04/2019",787.0,"a","prod1",1,0),
            ("01/05/2019",787.0,"a","prod1",1,1),
            ("01/06/2019",783.0,"a","prod1",3,0),
            ("01/01/2019",12.99,"a","prod2",100,0),
            ("01/02/2019",12.99,"a","prod2",100,1),
            ("01/03/2019",12.99,"a","prod2",100,1),
            ("01/06/2019",11.29,"a","prod2",102,0),
            ("01/01/2019",650.0,"b","prod1",1,0),
            ("01/02/2019",650.0,"b","prod1",1,1),
            ("01/03/2019",635.0,"b","prod1",3,0),
            ("01/04/2019",635.0,"b","prod1",3,1),
            ("01/05/2019",628.0,"b","prod1",2,0),
            ("01/06/2019",625.0,"b","prod1",2,0),
            ("01/01/2019",20.99,"b","prod2",107,0),
            ("01/02/2019",20.59,"b","prod2",119,0),
            ("01/03/2019",20.59,"b","prod2",119,1),
            ("01/04/2019",20.59,"b","prod2",119,1),
            ("01/06/2019",19.29,"b","prod2",122,0)
        ]
        schema = T.StructType([
            T.StructField('month', T.StringType(), False),
            T.StructField('price', T.DoubleType(), False),
            T.StructField('gcol', T.StringType(), False),
            T.StructField('product_id', T.StringType(), False),
            T.StructField('quantity', T.IntegerType(), False),
            T.StructField('imputed', T.IntegerType(), False),
        ])
        return self.spark.createDataFrame(expout_data, schema)

    def test_forward_filling(self):
        """Unit test for forward_filling 3 months ffill"""

        ffill_limit = 3

        input_df = self.input_data_imputation()
        expected_df = self.expected_output_imputation()
        result_df = imputation.forward_filling(
            input_df, ffill_limit, self.groupby_cols)
        self.assertDFEqualUnordered(result_df, expected_df)

    def test_forward_filling_2months(self):
        """Unit test for forward_filling 2 months ffill"""

        ffill_limit = 2

        input_df = self.input_data_imputation()
        expected_df = self.expected_output_imputation_2months()
        result_df = imputation.forward_filling(
            input_df, ffill_limit, self.groupby_cols)
        self.assertDFEqualUnordered(result_df, expected_df)


if __name__ == "__main__":

    suite = unittest.TestSuite()
    #unittest.main(exit=False)

    suite.addTest(TestImputation("test_forward_filling"))
    suite.addTest(TestImputation("test_forward_filling_2months"))

    runner = unittest.TextTestRunner()
    runner.run(suite)
