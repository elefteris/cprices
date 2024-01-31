# import spark libraries
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# import python libraries
import pandas as pd
from importlib import reload
from functools import reduce
from itertools import combinations

# import custom libraries
from cprices.cprices.steps import utils
from cprices.cprices.steps.index_methods import geks
reload(utils)
reload(geks)

def jevons_fixed_base(df):

    """
    Creates the jevons fixed base indices table.

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

    for n, m in enumerate(months):

        # price relative between base month m0 and month of interest m
        m0_m = m0+'_'+m
        df = df.withColumn(m0_m, df[m]/df[m0])

#        # the following code is used if we require the price to exist in all
#        # months between m0 and m
#
#        # months between base month m0 and month of interest m
#        months_between_m0_m = months[:n]
#
#        # if price is missing in any month between m0 and m,
#        # then the price relative between m0, m should become null
#        for mb in months_between_m0_m:
#            df = df.withColumn(m0_m, df[mb]/df[mb] * df[m0_m])

    df = df.drop(*months)

    # melt the columns/pairs of months into one column
    df = utils.melt_df(
        df,
        id_vars = ['id'],
        value_vars = [c for c in df.columns if c != 'id'],
        var_name = 'm0_m',
        value_name = 'p/p0'
    )

    # choose columns of interest and drop any record that has a missing value
    df = df.select('id', 'm0_m', 'p/p0').dropna()

    # extract the group from the id column
    split_col = F.split(df['id'], '##')
    df = df.withColumn('group', split_col.getItem(0))

    # get index for each group and month by aggregation
    df = (
        df
        .withColumn('log(p/p0)', F.log(df['p/p0']))
        .groupBy(['group', 'm0_m'])
        .agg({"log(p/p0)":'mean'})
        .withColumn('index_value', F.exp(F.col('avg(log(p/p0))')))
    )

    df = df.drop('avg(log(p/p0))')

    # PANDAS

    # bring aggregated data to the driver
    df = df.toPandas()

    # separate the 2nd month from each pair of months
    # this is the month of interest, i.e. where we calculate the index
    _, df['month'] = df['m0_m'].str.split('_', 1).str

    return df[['group', 'month', 'index_value']]


def jevons_chained(df):

    """
    Creates the jevons chained indices table.

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

    # check whether each product exists in each month within each pair of
    # consecutive months
    for m1, m2 in zip(months, months[1:]):
        df = df.withColumn(m1+'_'+m2, df[m2]/df[m1])

    df = df.drop(*months)

    # melt the columns/pairs of months into one column
    df = utils.melt_df(
        df,
        id_vars = ['id'],
        value_vars = [c for c in df.columns if c != 'id'],
        var_name = 'pair_of_months',
        value_name = 'p2/p1'
    )

    # choose columns of interest and drop any record that has a missing value
    df = df.select('id', 'pair_of_months', 'p2/p1').dropna()

    # extract the group from the id column
    split_col = F.split(df['id'], '##')
    df = df.withColumn('group', split_col.getItem(0))

    # get jevons index for each group and month by aggregation
    df = (
        df
        .withColumn('log(p2/p1)', F.log(df['p2/p1']))
        .groupBy(['group', 'pair_of_months'])
        .agg({"log(p2/p1)":'mean'})
        .withColumn('index_value', F.exp(F.col('avg(log(p2/p1))')))
    )

    df = df.drop('avg(log(p2/p1))')

    # PANDAS

    # bring aggregated data to the driver
    df = df.toPandas()

    # separate the 2nd month from each pair of months
    # this is the month of interest, i.e. where we calculate the index
    _, df['month'] = df['pair_of_months'].str.split('_', 1).str
    # df['month'] = pd.to_datetime(df['month'])

    # jevons chained comes from the cumulative product of the geometric mean
    # of all chained price relatives
    df = df.sort_values(by=['group', 'month'], ascending=True)
    df['index_value'] = df.groupby('group').cumprod()

    # create dataframe for base month for all group_id combinations
    # the index value will be 1 everywhere because it's the base month
    df0 = pd.DataFrame({'group' : df['group'].unique()})
    df0['month'] = months[0]
    df0['index_value'] = 1

    # merge base month with the rest of the months
    df = pd.concat([df, df0])

    return df[['group', 'month', 'index_value']]


def jevons_bilateral_indices(df):

    """
    Creates the bilateral jevons indices for all possible pairs of months in
    the period of interest to be used in the geks index calculations.

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
    dfj : spark dataframe
        Has the following columns:

        * 'group' : concatenated groupby column names with '//'
        * 'pair_of_months' : concatenated months (eg 2019-01-01_2019-05-01).
        * 'index_value' : the bilateral index values (based on any index method).
    """

    # collect all months and sort
    months = sorted([c for c in df.columns if c != 'id'])

    # combinations will give back (m1, m2) with m1 < m2
    # the column added in each loop is the price relative between m1, m2
    for c in list(combinations(months, 2)):
        m1, m2 = str(c[0]), str(c[1])
        df = df.withColumn(m1+'_'+m2, df[m2]/df[m1])

    df = df.drop(*[str(m) for m in months])

    # Each column in the resulting df is a pair of months m1_m2
    # for all the possible combinations of months across the whole period

    # melt the columns/pairs of months into one column
    dfj = utils.melt_df(
        df,
        id_vars = ['id'],
        value_vars = [c for c in df.columns if c != 'id'],
        var_name = 'pair_of_months',
        value_name = 'p2/p1'
    )

    # we need all the pairwise permutations of the months
    # given that we if we change the order of months in the pair, the p2/p1
    # value is the inverse of the original one, we can simply
    # replace the column p2/p1 with its inverse
    # replace all the pairs of months m1_m2 with m2_m1 (change the order)
    dfj_inv = dfj.withColumn('p2/p1', 1/dfj['p2/p1'])
    split_col = F.split(dfj['pair_of_months'], '_')
    dfj_inv = dfj_inv.withColumn('m1', split_col.getItem(0))
    dfj_inv = dfj_inv.withColumn('m2', split_col.getItem(1))
    dfj_inv = dfj_inv.withColumn(
        'pair_of_months', F.concat_ws('_', *['m2', 'm1']))
    dfj_inv = dfj_inv.drop('m1', 'm2')

    # by appending the original dfj with its inverse version,
    # we get all the permutations of months
    dfj = reduce(DataFrame.unionByName, [dfj, dfj_inv])

    # choose columns of interest and drop any record that has a missing value
    dfj = dfj.select('id', 'pair_of_months', 'p2/p1').dropna()

    # extract the group from the id column
    split_col = F.split(dfj['id'], '##')
    dfj = dfj.withColumn('group', split_col.getItem(0))

    # get bilateral jevons index for each group and pair of months
    # by aggregation
    dfj = (
        dfj
        .withColumn('log(p2/p1)', F.log(dfj['p2/p1']))
        .groupBy(['group', 'pair_of_months'])
        .agg({"log(p2/p1)":'mean'})
        .withColumn('index_value', F.exp(F.col('avg(log(p2/p1))')))
    )

    dfj = dfj.drop('avg(log(p2/p1))')

    return dfj


def geks_jevons(df):

    """
    Creates the geks jevons indices table.

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

    # create bilateral indices table
    dfj = jevons_bilateral_indices(df)

    # bring dfj to the driver
    dfj = dfj.toPandas()

    # apply geks on the bilateral indices
    dfgj = geks.geks(dfj)

    return dfgj[['group', 'month', 'index_value']]


def rygeks_jevons(df, d):

    """
    Creates the rygeks jevons indices table.

    Parameters
    ----------
    df : spark dataframe
        The first column of df is 'id' which combines the group and the
        product_id so that group##product_id. Using the delimiter ##
        these two components can be split in two columns.

        The rest of the columns have the names of the months and each cell has
        the price of the corresponding id (row) in the corresponding month
        (column).

    d : integer
        Length of rolling window (e.g. 13 months from January to January)

    Returns
    -------
    df : pandas dataframe
        The output dataframe has the columns group, month and index_value.

    Notes
    -----
    The mathematical theory behind this index method can be found here:
    <LINK>
    """

    # create bilateral indices table
    dfj = jevons_bilateral_indices(df)

    # bring dfj to the driver
    dfj = dfj.toPandas()

    # apply rygeks on the bilateral indices
    dfrgj = geks.rygeks(dfj, d)

    return dfrgj[['group', 'month', 'index_value']]
