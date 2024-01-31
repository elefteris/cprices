# import spark libraries
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql import Window

# import python libraries
import pandas as pd
from importlib import reload
from functools import reduce
from itertools import combinations
from itertools import product

# import custom libraries
from cprices.cprices.steps import utils
from cprices.cprices.steps.index_methods import geks
reload(utils)
reload(geks)


def laspeyres_fixed_base(dfp, dfq):

    """
    Creates the laspeyres fixed base indices table.

    Parameters
    ----------
    dfp : spark dataframe
        The first column of df is 'id' which combines the group and the
        product_id so that group##product_id. Using the delimiter ##
        these two components can be split in two columns.

        The rest of the columns have the names of the months and each cell has
        the price of the corresponding id (row) in the corresponding month
        (column).

    dfq : spark dataframe
        The first column of df is 'id' which combines the group and the
        product_id so that group##product_id. Using the delimiter ##
        these two components can be split in two columns.

        The rest of the columns have the names of the months and each cell has
        the quantity of the corresponding id (row) in the corresponding month
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
    months = sorted([c for c in dfp.columns if c != 'id'])
    months = [str(m) for m in months]

    # the first month across the whole period covered in the data
    m0 = months[0]

    # Laspeyres needs quantities from base period so only products that exist
    # in the base period can be used to get laspeyres index
    dfp = dfp.dropna(subset=[m0])

    # the quantities of products in base period
    dfq = dfq.select('id', m0).withColumnRenamed(m0, 'quantity')

    # multiply all product prices across all months by the corresponding
    # product quantities in the first month
    df = dfp.join(dfq, ['id'], 'left')
    for m in months:
        df = df.withColumn(m, df[m]*df['quantity'])
    df = df.drop('quantity')

    # create versions of the base month column for each of the following months
    # each version is going to have missing values where the corresponding
    # month column also has missing values
    for m in months[1:]:
        df = df.withColumn(m0+'_'+m, df[m]/df[m] * df[m0])
    df = df.drop(m0)

    # extract the group from the id column
    split_col = F.split(df['id'], '##')
    df = df.withColumn('group', split_col.getItem(0))

    # sum of product*price will give the total sales for each group in each month
    df = df.groupBy('group').sum()

    # PANDAS

    # bring aggregated data to the driver
    df = df.toPandas()

    for m in months[1:]:
        m0_sales = f'sum({m0}_{m})'
        m_sales = f'sum({m})'
        df[m] = df[m_sales]/df[m0_sales]

    # the index for the base month will be 1 for all groups
    df[m0] = 1

    # result is df with 3 columns: group, month, value
    df = pd.melt(
        df,
        id_vars='group',
        value_vars=months,
        var_name='month',
        value_name='index_value'
    )

    return df[['group', 'month', 'index_value']]


def laspeyres_bilateral_indices(input_df):
    """
    Creates the bilateral laspeyres indices for all possible pairs of months in
    the period of interest to be used in the geks index calculations.

    Parameters
    ----------
    input_df  : spark dataframe
        The first column of df is 'id' which combines the group_id and the
        product_id so that values in the 'id' column have the form
        'data_source//supplier//item//retailer##product_id'
        which can be split into the corresponding 5 columns later using the
        delimiter ## these two components can be split in two columns.

        The rest of the columns are the month, price and quantity values.

    Returns
    -------
    df : spark dataframe
        Has the following columns:

        * 'group' : concatenated groupby column names with '//'
        * 'pair_of_months' : concatenated months (eg 2019-01-01_2019-05-01).
        * 'index_value' : the bilateral index values (based on any index method).
    """

    # collect all months and sort into sequential order
    months = sorted([row.month for row in input_df.select('month').distinct().collect()])

    # Create dictionary for storing each combination of base month for bilateral indices
    bilat_dict = {}

    # Remove any null values from all over months
    input_df = input_df.dropna().cache()

    # Work through every option of base month m0 in available months
    for m0 in months:
        # Keep input_df pristine
        df = input_df

        # LAspeyres needs prices and quantities from base period so only products
        # that exist in the base period can be used to get Laspeyres index. We
        # achieve this by joining dataframe of unique entries in base month to full
        # dataframe
        df_base_month_id  = (
            df
            .where(df['month'] == m0)
            .select('id', 'price', 'quantity')
            .withColumnRenamed('price', 'price_0')
            .withColumnRenamed('quantity', 'quantity_0')
        )

        # Join the nonnull base month price/quantities onto main dataframe
        df = df.join(F.broadcast(df_base_month_id), 'id')

        # Calculate p_i^t*q_i^0
        df = df.withColumn('pt*q0', df['price']*df['quantity_0'])

        # Calculate p_i^0*q_i^0
        df = df.withColumn('p0*q0', df['price_0']*df['quantity_0'])

        # extract the group from the id column
        split_col = F.split(df['id'], '##')
        df = df.withColumn('group', split_col.getItem(0))

        # Perform group by of each 'group' and sum to get the
        # sum(p_i^t*q_i^t) and sum(p_i^0*q_i^t)
        df_sumpq = (
            df
            .groupBy('group', 'month')
            .sum()
            .select(['group', 'month', 'sum(pt*q0)', 'sum(p0*q0)'])
        )

        # Divide sum of (pt*q0) by sum(p0*q0) to obtain final index
        df_index = df_sumpq.withColumn(
            'index_value',
            F.col('sum(pt*q0)')/F.col('sum(p0*q0)')
        )

        # Append current base month choice to month column
        df_index = df_index.withColumn(
            'pair_of_months',
            F.concat(F.lit(m0), F.lit('_'), F.col('month'))
        )
        df_index = df_index.drop('months')

        # Assign current base month choice indices to dictionary
        bilat_dict[m0] = df_index


    # Create final dataframe of all combinations
    spark_df = reduce(DataFrame.unionByName, list(bilat_dict.values()))

    return spark_df.select('group' ,'pair_of_months', 'index_value')


def geks_laspeyres(df):
    """
    Creates the geks Laspeyres indices table.

    Parameters
    ----------
    df  : spark dataframe
        The first column of df is 'id' which combines the group_id and the
        product_id so that values in the 'id' column have the form
        'data_source//supplier//item//retailer##product_id'
        which can be split into the corresponding 5 columns later using the
        delimiter ## these two components can be split in two columns.

        The rest of the columns are the month, price and quantity values.

    Returns
    -------
    dfgt : pandas dataframe
        The output dataframe has the columns group, month and index_value.

    Notes
    -----
    The mathematical theory behind this index method can be found here:
    <LINK>
    """

    # create bilateral indices table
    dft = laspeyres_bilateral_indices(df)

    # bring dfj to the driver
    dft = dft.toPandas()

    # apply geks on the bilateral indices
    dfgt = geks.geks(dft)

    return dfgt


def rygeks_laspeyres(df, d):
    """
    Creates the rygeks Laspeyres indices table.

    Parameters
    ----------
    df  : spark dataframe
        The first column of df is 'id' which combines the group_id and the
        product_id so that values in the 'id' column have the form
        'data_source//supplier//item//retailer##product_id'
        which can be split into the corresponding 5 columns later using the
        delimiter ## these two components can be split in two columns.

        The rest of the columns are the month, price and quantity values.

    d : integer
        Length of rolling window (e.g. 13 months from January to January)

    Returns
    -------
    dfrgt : pandas dataframe
        The output dataframe has the columns group, month and index_value.

    Notes
    -----
    The mathematical theory behind this index method can be found here:
    <LINK>
    """

    # create bilateral indices table
    dft = laspeyres_bilateral_indices(df)

    # bring dfj to the driver
    dft = dft.toPandas()

    # apply rygeks on the bilateral indices
    dfrgt = geks.rygeks(dft, d)

    return dfrgt
