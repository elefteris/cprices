# import spark libraries
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# import python libraries
from functools import reduce
from importlib import reload
import os

from cprices.cprices.steps import averaging
reload(averaging)

def create_and_append_grouped_items(spark, df, groupby_cols, mapper, method):
    """
    Forms groups of products so that we don't keep track of the price of
    individual products but the average price of groups of products.

    Parameters
    ----------
    spark :
        spark session

    df : spark dataframe
        Includes products to be grouped.

    groupby_cols : list of strings
        User configuration. Includes parameter 'active' that shows whether
        the products of any item will be grouped.

    mapper : spark dataframe
        Includes the key column (product_id) to join df with mapper and
        another column showing the id of the group each product belongs to
        (group_id). This way, by joing the mapper to df the group_id will
        replace the product_id.

    method : string
        Shows the method to average prices within each group of products.

    Returns
    -------
    df : spark dataframe
        It is the union of the input dataframe with the "grouped" version of
        the input dataframe. The grouped version basically has grouped items
        that is all their products have been grouped and the product_id has
        been replaced by the group_id.

    Notes
    -----
    The products in a group (formed by the groupby columns) are replaced by
    one product/group_id with the average price and the sum of
    quantities of the products in the group.

    """

    # join mapper on product_id in new dfg to add group_id column
    dfg = df.join(mapper, ['product_id'], 'left')

    # drop null from group_id
    dfg = dfg.dropna(subset=['group_id'])

    # drop product_id and rename group_id to product_id
    dfg = dfg.drop('product_id')
    dfg = dfg.withColumnRenamed('group_id', 'product_id')

    # add suffix '_grouped' to all values of 'item' column
    dfg = dfg.withColumn(
        'item', F.concat(F.col('item'), F.lit('_grouped')))

    METHODS_DICT = {
        'unweighted_arithmetic' : averaging.unweighted_arithmetic_average,
        'unweighted_geometric'  : averaging.unweighted_geometric_average,
        'weighted_arithmetic'   : averaging.weighted_arithmetic_average,
        'weighted_geometric'    : averaging.weighted_geometric_average,
    }

    dfg = METHODS_DICT[method](spark, dfg, groupby_cols)

    # if any group doesn't have any products, the price and quantity
    # are missing
    dfg = dfg.dropna()

    # union original df with grouped dfg. df has the original items and
    # dfg has the items whose products have been grouped
    df = reduce(DataFrame.unionByName, [df, dfg])

    return df


def main(spark, df, config, dev_config):
    """
    Forms groups of products so that we don't keep track of the price of
    individual products but the average price of groups of products.

    Parameters
    ----------
    spark :
        spark session

    df : spark dataframe
        Includes products to be grouped.

    config : python file/module
        User configuration. Includes parameter 'active' that shows whether
        the products of any item will be grouped and the averaging method.

    dev_config : python file/module
        Developer's configuration. Includes HDFS path to mapper. That is
        the mapper for grouping maps product ids to product groups. It also
        includes the groupby columns.

    Returns
    -------
    df : spark dataframe
        If grouping is active then grouped items are added to the input
        dataframe. Otherwise the input dataframe is returned.

    """

    if config.params['grouping']['active']:

        # columns to form groups
        groupby_cols = dev_config.groupby_cols + ['month', 'product_id']

        # averaging method when aggregate to create group product
        method = config.params['grouping']['web_scraped']['method']

        # read mapper
        mapper_path = os.path.join(dev_config.mappers_dir, 'grouping')
        mapper = spark.read.parquet(mapper_path)
        mapper = mapper.select('product_id', 'group_id').dropna()

        df = create_and_append_grouped_items(
            spark, df, groupby_cols, mapper, method)

    return df
