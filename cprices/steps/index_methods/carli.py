# import spark libraries
from pyspark.sql import functions as F

# import python libraries
import pandas as pd
from importlib import reload

# import configuration files
from cprices.cprices.steps import utils
reload(utils)

def carli_fixed_base(df):

    """
    Creates the carli fixed base indices table.

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
#        # START
#
#        # months between base month m0 and month of interest m
#        months_between_m0_m = months[:n]
#
#        # if price is missing in any month between m0 and m,
#        # then the price relative between m0, m should become null
#        for mb in months_between_m0_m:
#            df = df.withColumn(m0_m, df[mb]/df[mb] * df[m0_m])
#
#        # END

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

    df = df.groupBy(['group', 'm0_m']).mean()

    # PANDAS

    # bring aggregated data to the driver
    df = df.toPandas()

    df = df.rename(columns={'avg(p/p0)' : 'index_value'})

    # separate the 2nd month from each pair of months
    # this is the month of interest, i.e. where we calculate the index
    _, df['month'] = df['m0_m'].str.split('_', 1).str

    return df[['group', 'month', 'index_value']]


def carli_chained(df):

    """
    Creates the carli chained indices table.

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
        var_name = 'm1_m2',
        value_name = 'p2/p1'
    )

    # extract the group from the id column
    split_col = F.split(df['id'], '##')
    df = df.withColumn('group', split_col.getItem(0))

    # extract the m2 from the m1_m2 column
    split_col = F.split(df['m1_m2'], '_')
    df = df.withColumn('month', split_col.getItem(1))

    # choose columns of interest and drop any record that has a missing value
    df = df.select('group', 'month', 'p2/p1').dropna()

    # arithmetic mean of price relatives per item, retailer, month
    df = df.groupBy(['group', 'month']).mean()

    # PANDAS

    # bring aggregated data to the driver
    df = df.toPandas()

    # carli chained comes from the cumulative product of the arithmetic mean
    # of all chained price relatives
    df = df.sort_values(by=['group', 'month'], ascending=True)
    df['index_value'] = df.groupby('group').cumprod()

    # create dataframe for base month for all groups
    # the index value will be 1 everywhere because it's the base month
    df0 = pd.DataFrame({'group' : df['group'].unique()})
    df0['month'] = months[0]
    df0['index_value'] = 1

    # merge base month with the rest of the months
    df = pd.concat([df, df0])

    return df[['group', 'month', 'index_value']]
