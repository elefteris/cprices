# Import pyspark packages
from pyspark.sql import functions as F
from pyspark.sql import types as T
from epds_utils.testing.pyspark_test_case import PySparkTest

# Import python packages
import os
import pandas as pd
import unittest
from importlib import reload

#Import module to test
from cprices.cprices.steps import indices
reload(indices)

from cprices.cprices.steps.index_methods import (
    carli,
    dutot,
    fisher,
    geary_khamis,
    geks,
    jevons,
    laspeyres,
    paasche,
    tornqvist
)
reload(carli)
reload(dutot)
reload(geary_khamis)
reload(geks)
reload(fisher)
reload(jevons)
reload(laspeyres)
reload(paasche)
reload(tornqvist)

class TestIndexMethods(PySparkTest):
    """Test class for index methods."""

    def setUp(self):
        self.root_dir = '/home/cdsw/cprices'
        self.cols_to_round = ['index_value']
        self.rounding_scale = 3
        self.groupby_cols = ['group']
        self.d = 13

    def input_data_index_methods(self):
        """Create input data set for all unit tests (all index methods)"""
        path = os.path.join(self.root_dir, 'tests', 'data', 'input_df.csv')
        input_df = self.spark.createDataFrame(pd.read_csv(path))
        df, dfp, dfq = indices.product_by_month_tables(
            input_df, self.groupby_cols)
        return df, dfp, dfq

    def input_data_geks_rygeks(self):
        """Create input data set for geks and rygeks"""
        path = os.path.join(self.root_dir, 'tests', 'data', 'geks_input.csv')
        input_df = self.spark.createDataFrame(pd.read_csv(path))
        return input_df

    def input_data_geary_khamis_subset_df(self):
        """Create input data set for subsetting geary khamis input"""
        path = os.path.join(self.root_dir, 'tests', 'data', 'df_gk_subset.csv')
        input_df = self.spark.createDataFrame(pd.read_csv(path))
        return input_df

    def expected_output_carlifb(self):
        """Create expected output data set for carlifb"""
        filename_method_data = "carli_fixed_base.csv"
        path = os.path.join(self.root_dir, 'tests', 'data', filename_method_data)
        expected_df = self.spark.createDataFrame(pd.read_csv(path))
        return expected_df

    def expected_output_carlic(self):
        """Create expected output data set for carlic"""
        filename_method_data = "carli_chained.csv"
        path = os.path.join(self.root_dir, 'tests', 'data', filename_method_data)
        expected_df = self.spark.createDataFrame(pd.read_csv(path))
        return expected_df

    def expected_output_dutotfb(self):
        """Create expected output data set for dutotfb"""
        filename_method_data = "dutot_fixed_base.csv"
        path = os.path.join(self.root_dir, 'tests', 'data', filename_method_data)
        expected_df = self.spark.createDataFrame(pd.read_csv(path))
        return expected_df

    def expected_output_dutotc(self):
        """Create expected output data set for dutotc"""
        filename_method_data = "dutot_chained.csv"
        path = os.path.join(self.root_dir, 'tests', 'data', filename_method_data)
        expected_df = self.spark.createDataFrame(pd.read_csv(path))
        return expected_df

    def expected_output_fisherbil(self):
        """Create expected data set for fisherbil"""
        filename_method_data = "fisher_bilateral.csv"
        path = os.path.join(self.root_dir, 'tests', 'data', filename_method_data)
        expected_df = self.spark.createDataFrame(pd.read_csv(path))
        return expected_df

    def expected_output_jevonsfb(self):
        """Create expected output data set for jevonsfb"""
        filename_method_data = "jevons_fixed_base.csv"
        path = os.path.join(self.root_dir, 'tests', 'data', filename_method_data)
        expected_df = self.spark.createDataFrame(pd.read_csv(path))
        return expected_df

    def expected_output_jevonsc(self):
        """Create expected output data set for jevonsc"""
        filename_method_data = "jevons_chained.csv"
        path = os.path.join(self.root_dir, 'tests', 'data', filename_method_data)
        expected_df = self.spark.createDataFrame(pd.read_csv(path))
        return expected_df

    def expected_output_jevonsbil(self):
        """Create expected output data set for jevonsbil
        (jevons bilateral indicies)"""
        filename_method_data = "jevons_bilateral.csv"
        path = os.path.join(self.root_dir, 'tests', 'data', filename_method_data)
        expected_df = self.spark.createDataFrame(pd.read_csv(path))
        return expected_df

    def expected_output_geks(self):
        """Create expected output data set for geks"""
        filename_method_data = "geks.csv"
        path = os.path.join(self.root_dir, 'tests', 'data', filename_method_data)
        expected_df = self.spark.createDataFrame(pd.read_csv(path))
        return expected_df

    def expected_output_rygeks(self):
        """Create expected output data set for rygeks"""
        filename_method_data = "rygeks.csv"
        path = os.path.join(self.root_dir, 'tests', 'data', filename_method_data)
        expected_df = self.spark.createDataFrame(pd.read_csv(path))
        return expected_df

    def expected_output_laspeyresfb(self):
        """Create expected data set for laspeyresfb"""
        filename_method_data = "laspeyres_fixed_base.csv"
        path = os.path.join(self.root_dir, 'tests', 'data', filename_method_data)
        expected_df = self.spark.createDataFrame(pd.read_csv(path))
        return expected_df

    def expected_output_laspeyresbil(self):
        """Create expected data set for laspeyresbil"""
        filename_method_data = "laspeyres_bilateral.csv"
        path = os.path.join(self.root_dir, 'tests', 'data', filename_method_data)
        expected_df = self.spark.createDataFrame(pd.read_csv(path))
        return expected_df

    def expected_output_paaschebil(self):
        """Create expected data set for paaschebil"""
        filename_method_data = "paasche_bilateral.csv"
        path = os.path.join(self.root_dir, 'tests', 'data', filename_method_data)
        expected_df = self.spark.createDataFrame(pd.read_csv(path))
        return expected_df

    def expected_output_tornqvistfb(self):
        """Create expected data set for tornqvistfb"""
        filename_method_data = "tornqvist_fixed_base.csv"
        path = os.path.join(self.root_dir, 'tests', 'data', filename_method_data)
        expected_df = self.spark.createDataFrame(pd.read_csv(path))
        return expected_df

    def expected_output_tornqvistbil(self):
        """Create expected data set for tornqvistbil"""
        filename_method_data = "tornqvist_bilateral.csv"
        path = os.path.join(self.root_dir, 'tests', 'data', filename_method_data)
        expected_df = self.spark.createDataFrame(pd.read_csv(path))
        return expected_df

    def expected_output_geary_khamis(self):
        """Create expected data set for geary_khamis"""
        filename_method_data = "geary_khamis.csv"
        path = os.path.join(self.root_dir, 'tests', 'data', filename_method_data)
        expected_df = self.spark.createDataFrame(pd.read_csv(path))
        return expected_df

    def test_carlifb(self):
        """Unit test for carlifb"""
        df, dfp, dfq = self.input_data_index_methods()
        expected_df = self.expected_output_carlifb()
        result_df = self.spark.createDataFrame(carli.carli_fixed_base(dfp))
        self.assertDFAlmostEqualUnordered(
            result_df, expected_df, self.cols_to_round, self.rounding_scale)

    def test_carlic(self):
        """Unit test for carlic"""
        df, dfp, dfq = self.input_data_index_methods()
        expected_df = self.expected_output_carlic()
        result_df = self.spark.createDataFrame(carli.carli_chained(dfp))
        self.assertDFAlmostEqualUnordered(
            result_df, expected_df, self.cols_to_round, self.rounding_scale)

    def test_dutotfb(self):
        """Unit test for dutotfb"""
        df, dfp, dfq = self.input_data_index_methods()
        expected_df = self.expected_output_dutotfb()
        result_df = self.spark.createDataFrame(dutot.dutot_fixed_base(dfp))
        self.assertDFAlmostEqualUnordered(
            result_df, expected_df, self.cols_to_round, self.rounding_scale)

    def test_dutotc(self):
        """Unit test for dutotc"""
        df, dfp, dfq = self.input_data_index_methods()
        expected_df = self.expected_output_dutotc()
        result_df = self.spark.createDataFrame(dutot.dutot_chained(dfp))
        self.assertDFAlmostEqualUnordered(
            result_df, expected_df, self.cols_to_round, self.rounding_scale)

    def test_fisherbil(self):
        """Unit test for fisherbil"""
        df, dfp, dfq = self.input_data_index_methods()
        expected_df = self.expected_output_fisherbil()
        result_df = fisher.fisher_bilateral_indices(df)
        self.assertDFAlmostEqualUnordered(
            result_df, expected_df, self.cols_to_round, self.rounding_scale)

    def test_jevonsfb(self):
        """Unit test for jevonsfb"""
        df, dfp, dfq = self.input_data_index_methods()
        expected_df = self.expected_output_jevonsfb()
        result_df = self.spark.createDataFrame(jevons.jevons_fixed_base(dfp))
        self.assertDFAlmostEqualUnordered(
            result_df, expected_df, self.cols_to_round, self.rounding_scale)

    def test_jevonsc(self):
        """Unit test for jevonsc"""
        df, dfp, dfq = self.input_data_index_methods()
        expected_df = self.expected_output_jevonsc()
        result_df = self.spark.createDataFrame(jevons.jevons_chained(dfp))
        self.assertDFAlmostEqualUnordered(
            result_df, expected_df, self.cols_to_round, self.rounding_scale)

    def test_jevonsbil(self):
        """Unit test for jevonsbil"""
        df, dfp, dfq = self.input_data_index_methods()
        expected_df = self.expected_output_jevonsbil()
        result_df = jevons.jevons_bilateral_indices(dfp)
        self.assertDFAlmostEqualUnordered(
            result_df, expected_df, self.cols_to_round, self.rounding_scale)

    def test_geks(self):
        """Unit test for geks"""
        dfgeks = self.input_data_geks_rygeks()
        expected_df = self.expected_output_geks()
        result_df = self.spark.createDataFrame(geks.geks(dfgeks.toPandas()))
        self.assertDFAlmostEqualUnordered(
            result_df, expected_df, self.cols_to_round, self.rounding_scale)

    def test_rygeks(self):
        """Unit test for rygeks"""
        dfgeks = self.input_data_geks_rygeks()
        expected_df = self.expected_output_rygeks()
        result_df = self.spark.createDataFrame(geks.rygeks(dfgeks.toPandas(), self.d))
        self.assertDFAlmostEqualUnordered(
            result_df, expected_df, self.cols_to_round, self.rounding_scale)

    def test_laspeyresfb(self):
        """Unit test for laspeyresfb"""
        df, dfp, dfq = self.input_data_index_methods()
        expected_df = self.expected_output_laspeyresfb()
        result_df = self.spark.createDataFrame(laspeyres.laspeyres_fixed_base(dfp, dfq))
        self.assertDFAlmostEqualUnordered(
            result_df, expected_df, self.cols_to_round, self.rounding_scale)

    def test_laspeyresbil(self):
        """Unit test for laspeyresbil"""
        df, dfp, dfq = self.input_data_index_methods()
        expected_df = self.expected_output_laspeyresbil()
        result_df = laspeyres.laspeyres_bilateral_indices(df)
        self.assertDFAlmostEqualUnordered(
            result_df, expected_df, self.cols_to_round, self.rounding_scale)

    def test_paaschebil(self):
        """Unit test for paaschebil"""
        df, dfp, dfq = self.input_data_index_methods()
        expected_df = self.expected_output_paaschebil()
        result_df = paasche.paasche_bilateral_indices(df)
        self.assertDFAlmostEqualUnordered(
            result_df, expected_df, self.cols_to_round, self.rounding_scale)

    def test_tornqvistfb(self):
        """Unit test for tornqvistfb"""
        df, dfp, dfq = self.input_data_index_methods()
        expected_df = self.expected_output_tornqvistfb()
        result_df = self.spark.createDataFrame(tornqvist.tornqvist_fixed_base(df))
        self.assertDFAlmostEqualUnordered(
            result_df, expected_df, self.cols_to_round, self.rounding_scale)

    def test_tornqvistbil(self):
        """Unit test for tornqvistbil"""
        df, dfp, dfq = self.input_data_index_methods()
        expected_df = self.expected_output_tornqvistbil()
        result_df = tornqvist.tornqvist_bilateral_indices(df)
        self.assertDFAlmostEqualUnordered(
            result_df, expected_df, self.cols_to_round, self.rounding_scale)

#    def test_geary_khamis_subset_df(self):
#        """Unit test for subsetting geary khamis input dataframe"""
#        df, dfp, dfq = self.input_data_index_methods()
#        months_subset = ['2017-11-01', '2017-12-01', '2018-01-01']
#        expected_df = self.input_data_geary_khamis_subset_df()
#        result_df = geary_khamis.subset_df(df, months_subset)
#        self.assertDFEqual(result_df, expected_df)
#
#    def test_geary_khamis_append_gk_index(self):
#        """Unit test for appending new geary khamis index to backseries"""
#
#    def test_geary_khamis_append_gk_index_overwrite(self):
#        """Unit test for overwritting geary khamis index in backseries"""
#
#    def test_geary_khamis_latest(self):
#        """Unit test for geary_khamis single month"""
#        df, dfp, dfq = self.input_data_index_methods()
#        expected_df = self.expected_output_geary_khamis()
#        result_df = self.spark.createDataFrame(geary_khamis.geary_khamis(df, run_all_periods=False))
#        self.assertDFAlmostEqualUnordered(
#            result_df, expected_df, self.cols_to_round, self.rounding_scale)
#
#    def test_geary_khamis_backseries(self):
#        """Unit test for geary_khamis backseries"""
#        df, dfp, dfq = self.input_data_index_methods()
#        expected_df = self.expected_output_geary_khamis()
#        result_df = self.spark.createDataFrame(
#            geary_khamis.geary_khamis(
#                df,
#                run_all_periods=True,
#                convergence_tolerance=1e-5,
#                GK_index_filepath='/home/cdsw/gk_test_backseries.csv'
#            )
#        )
#        self.assertDFAlmostEqualUnordered(
#            result_df, expected_df, self.cols_to_round, self.rounding_scale)

if __name__ == "__main__":

    suite = unittest.TestSuite()
    #unittest.main(exit=False)

    suite.addTest(TestIndexMethods("test_carlifb"))
    suite.addTest(TestIndexMethods("test_carlic"))
    suite.addTest(TestIndexMethods("test_dutotfb"))
    suite.addTest(TestIndexMethods("test_dutotc"))
    suite.addTest(TestIndexMethods("test_fisherbil"))
    suite.addTest(TestIndexMethods("test_jevonsfb"))
    suite.addTest(TestIndexMethods("test_jevonsc"))
    suite.addTest(TestIndexMethods("test_geks"))
    suite.addTest(TestIndexMethods("test_rygeks"))
    suite.addTest(TestIndexMethods("test_jevonsbil"))
    suite.addTest(TestIndexMethods("test_laspeyresfb"))
    suite.addTest(TestIndexMethods("test_laspeyresbil"))
    suite.addTest(TestIndexMethods("test_paaschebil"))
    suite.addTest(TestIndexMethods("test_tornqvistfb"))
    suite.addTest(TestIndexMethods("test_tornqvistbil"))
#    suite.addTest(TestIndexMethods("test_geary_khamis_subset_df"))
#    suite.addTest(TestIndexMethods("test_geary_khamis_append_gk_index"))
#    suite.addTest(TestIndexMethods("test_geary_khamis_append_gk_index_overwrite"))
#    suite.addTest(TestIndexMethods("test_geary_khamis_latest"))
#    suite.addTest(TestIndexMethods("test_geary_khamis_backseries"))

    runner = unittest.TextTestRunner()
    runner.run(suite)
