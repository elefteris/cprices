# import spark libraries
from pyspark.sql import functions as F

# import python libraries
import pandas as pd
from importlib import reload

# import configuration files
from cprices.cprices.steps import utils
reload(utils)

def dutot_fixed_base(df):

    """
    Creates the dutot fixed base indices table.

    Parameters
    ----------
    df : spark dataframe
        The first column of df is 'id' which combines the group and the
        product_id so that group##product_id. Using the delimiter ##
        these two components can be split in two columns.

        The rest of the columns have the names of the months and each cell has
        the price of the corresponding id (row) in the corresponding month
        (column).

    Returns
    -------
    df : pandas dataframe
        The output dataframe has the columns group, month and index_value.

    Notes
    -----
    The mathematical theory behind this index method can be found here:
    <LINK>
    """

    # collect all months and sort
    months = sorted([c for c in df.columns if c != 'id'])
    months = [str(m) for m in months]

    # base month
    m0 = months[0]

    # it's a fixed base method so products that do not exist in the base month
    # are dropped
    df = df.filter(df[m0].isNotNull())

    # for each month create 2 additional columns:
    # 1. one with the prices in m0 of all the products that exist in all the months
    #    from m0 up to m
    # 2. one with the prices in m of all the products that exist in all the months
    #    from m0 up to m

#    # the following code is used if we require the price to exist in all
#    # months between m0 and m
#
#    for n, m in enumerate(months[1:]):
#
#        # column with products from m0 that appear in both m0 and m
#        # and all the months in between
#        m0_m0_m = m0+'_'+m0+'_'+m
#
#        # initialise column m0_m0_m with prices from m0
#        df = df.withColumn(m0_m0_m, df[m0])
#
#        # column with products from m that appear in both m0 and m
#        # and all the months in between
#        m_m0_m = m+'_'+m0+'_'+m
#
#        # initialise column m_m0_m with prices from m
#        df = df.withColumn(m_m0_m, df[m])
#
#        # turn prices in columns m0_m0_m, m_m0_m to null
#        # if any price from m1 up to m is missing
#
#        # months from m1 up to m
#        months_m0_m = months[1:n+1]
#
#        for mb in months_m0_m:
#            df = df.withColumn(m0_m0_m, df[mb]/df[mb] * df[m0_m0_m])
#            df = df.withColumn(m_m0_m, df[mb]/df[mb] * df[m_m0_m])

    # the following code is used if we require the price to exist only in the
    # base month and the month of interest

    for n, m in enumerate(months[1:]):

        # column with products from m0 that appear in both m0 and m
        m0_m0_m = m0+'_'+m0+'_'+m
        df = df.withColumn(m0_m0_m, df[m0] * df[m]/df[m])

        # column with products from m that appear in both m0 and m
        m_m0_m = m+'_'+m0+'_'+m
        df = df.withColumn(m_m0_m, df[m] * df[m0]/df[m0])

    df = df.drop(*months)

    # extract the group from the id column
    split_col = F.split(df['id'], '##')
    df = df.withColumn('group', split_col.getItem(0))

    # sum of prices of all products in the month that exist in
    # both months/pair of interest
    df = df.groupBy('group').sum()

    # PANDAS

    # bring aggregated data to the driver
    df = df.toPandas()
    cols_to_drop = []

    for m in months[1:]:

        sum_m0_m0_m = f"sum({m0}_{m0}_{m})"
        sum_m_m0_m = f"sum({m}_{m0}_{m})"

        df[m] = df[sum_m_m0_m] / df[sum_m0_m0_m]

        cols_to_drop.extend([sum_m_m0_m, sum_m0_m0_m])

    df = df.drop(cols_to_drop, 1)

    # for items and retailers, the index of the first month is 1
    df[m0] = 1

    # result from melt is df with 3 columns: group, month, index_value
    df = pd.melt(
        df,
        id_vars='group',
        value_vars=[c for c in df.columns if c!='group'],
        var_name='month',
        value_name='index_value'
    )

    return df[['group', 'month', 'index_value']]


def dutot_chained(df):

    """
    Creates the dutot chained indices table.

    Parameters
    ----------
    df : spark dataframe
        The first column of df is 'id' which combines the group and the
        product_id so that group##product_id. Using the delimiter ##
        these two components can be split in two columns.

        The rest of the columns have the names of the months and each cell has
        the price of the corresponding id (row) in the corresponding month
        (column).

    Returns
    -------
    df : pandas dataframe
        The output dataframe has the columns group, month and index_value.

    Notes
    -----
    The mathematical theory behind this index method can be found here:
    <LINK>
    """

    # collect all months and sort
    months = sorted([c for c in df.columns if c != 'id'])

    # check whether each product exists in each month within each pair of
    # consecutive months
    for m1, m2 in zip(months, months[1:]):

        # pair of months
        m12 = m1+'_'+m2

        df = (
          df
          .withColumn(m1+'_'+m12, df[m2]/df[m2] * df[m1])
          .withColumn(m2+'_'+m12, df[m1]/df[m1] * df[m2])
        )

    df = df.drop(*months)

    # extract the group from the id column
    split_col = F.split(df['id'], '##')
    df = df.withColumn('group', split_col.getItem(0))

    # sum of prices of all products in the month that exist in
    # both months/pair of interest
    df = df.groupBy('group').sum()

    # PANDAS

    # bring aggregated data to the driver
    df = df.toPandas()
    cols_to_drop = []

    # bilateral dutot indices: sum(prices in m2)/sum(prices in m1)
    # for all pairs of consecutive months
    for m1, m2 in zip(months, months[1:]):

        m12 = f"{m1}_{m2}"

        sum_m1_m1_m2 = f"sum({m1}_{m12})"
        sum_m2_m1_m2 = f"sum({m2}_{m12})"

        df[m12] = df[sum_m2_m1_m2] / df[sum_m1_m1_m2]

        cols_to_drop.extend([sum_m1_m1_m2, sum_m2_m1_m2])

    df = df.drop(cols_to_drop, 1)

    # the index of the first month is 1
    m0 = months[0]
    m0_m0 = m0+'_'+m0
    df[m0_m0] = 1

    # result is df with 3 columns: group, m1_m2, value
    df = pd.melt(
        df,
        id_vars='group',
        value_vars=[c for c in df.columns if c!='group'],
        var_name='m1_m2',
        value_name='value'
    )

    # separate the 2nd month from each pair of months
    # this is the month of interest, i.e. where we calculate the index
    _, df['month'] = df['m1_m2'].str.split('_', 1).str
    # df['month'] = pd.to_datetime(df['month'])

    df = df.sort_values(by=['group', 'month'], ascending=True)

    # dutot chained comes from the cumulative product of bilateral dutot indices
    # across all months
    df['index_value'] = df.groupby('group').cumprod()

    return df[['group', 'month', 'index_value']]
