# import spark libraries
from pyspark.sql import functions as F
from pyspark.sql import Window

# import python libraries
from importlib import reload
import sys

# import custom modules
from cprices.cprices.steps import averaging
reload(averaging)

def get_expenditure_info(spark, df, groupby_cols, mcs):

    """
    Creates additional column related to expenditure information.

    Parameters
    ----------
    spark :
        spark session

    df : spark dataframe
        Includes price and quantities for various products as well as the
        groupby columns to form groups of products.

    groupby_cols : list of strings
        Includes the columns to form groups of products, for example:
        data source, supplier, item, retailer and month.

    mcs : float
        The max_cumsum_share shows the maximum value of the total expenditure
        share of the most popular products. Any products above the limit are
        dropped. Accepted values range from 0 to 1. Default 0.8.


    Returns
    -------
    df : spark dataframe
        The input dataframe with extra columns related to expenditure:

        * sales : product of price*quantity
        * sum_sales : total sales of products that belong to group
        * share : fraction of products sales out of total group sales
        * cumsum_share : cumulative share of products starting from those
        with largest share
        * filtered : shows which products exceed the maximum value of the
        cumulative sum of the expenditure share
    """

    # create sales (expenditure share)
    df = df.withColumn('sales', df['price']*df['quantity'])

    # group sum of expenditure share
    cols = groupby_cols + ['sales']
    sum_sales = df.select(cols).groupBy(groupby_cols).sum()
    sum_sales = sum_sales.withColumnRenamed('sum(sales)', 'sum_sales')

    # join with original table
    df = averaging.left_join(
        spark, df1=df, df2=sum_sales, join_cols=groupby_cols)

    # divide to get % of expenditure share for each product in group
    df = df.withColumn('share', df['sales']/df['sum_sales'])

    # sort % of expenditure share (max to min) and cumsum by group
    w = (
        Window
        .partitionBy(groupby_cols)
        .orderBy(F.desc('share'))
        .rowsBetween(-sys.maxsize, 0)
    )
    df = df.withColumn('cumsum_share', F.sum(df['share']).over(w))

    # flag with for 1 products that exceed the maximum value of the cumulative
    # sum of the expenditure share
    df = df.withColumn(
        'filtered',
        F.when((df['cumsum_share']<=mcs)|(df['share']>=mcs),0)
        .otherwise(1)
    )

    return df


def main(spark, df, config, dev_config):
    """
    Filters out products with low expenditure share within each group of
    products defined by data source, supplier, item, retailer and month.

    Parameters
    ----------
    spark :
        spark session

    df : spark dataframe
        Includes price and quantities for various products as well as the
        groupby columns to form groups of products.

    config : python file/module
        Includes the active parameter that shows whether filtering will be used
        and the max_cumsum_share which is the upper limit of cumulative
        expenditure share. Any products above the limit are dropped.

    dev_config : python file/module
        Includes the groupby columns.

    Returns
    -------
    df : spark dataframe
        The input dataframe with extra columns sales, sum(sales), share,
        cumsum(share) related to expenditure that can be used for analysis.
        An extra gg column 'filtered' shows which products fall below the
        expenditure share limit (when the stage is active).

    filtered : spark dataframe
        The input dataframe without the products with low expenditure share
        and without any intermediate columns. This one will continue to the
        next stage. If the stage is not active, this is the same as the input
        dataframe.

    Notes
    -----
    Each product belongs to a group and has an expenditure share, i.e. the
    sales of the product as a fraction of the total sales of the group.
    Starting from the most popular product (largest expenditure share),
    the products' expenditure shares can be sorted in descending order.
    The max_cumsum_share shows the maximum value of the total expenditure
    of the most popular products. Any products above the limit are dropped.
    Accepted values range from 0 to 1. Default 0.8.

    If the stage is not chosen to be active by the user, all the expenditure
    info columns are still produced as this can be useful for further analysis
    on expenditure stats.

    The cols_to_drop are useful for expenditure stats analysis but do not
    continue to the next stage of the pipeline.

    """

    groupby_cols = dev_config.groupby_cols + ['month']
    cols_to_drop = ['sales', 'sum_sales', 'share', 'cumsum_share', 'filtered']
    mcs = config.params['filtering']['max_cumsum_share']

    df = get_expenditure_info(spark, df, groupby_cols, mcs)

    if config.params['filtering']['active']:
        # filter to keep only products with expenditure share above threshold
        # and drop intermediate columns
        filtered = df.filter(df['filtered']==0).drop(*cols_to_drop)
    else:
        # if not active then accept all products regardless from expenditure
        df = df.withColumn('filtered', F.lit(0))
        # drop intermediate columns
        filtered = df.drop(*cols_to_drop)

    return df, filtered
