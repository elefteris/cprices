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

def paasche_bilateral_indices(input_df):
    """
    Creates the bilateral Paasche indices for all possible pairs of months in
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

        # Paasche needs prices and quantities from base period so only products
        # that exist in the base period can be used to get Paasche index. We
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

        # Calculate p_i^t*q_i^t
        df = df.withColumn('pt*qt', df['price']*df['quantity'])

        # Calculate p_i^0*q_i^t
        df = df.withColumn('p0*qt', df['price_0']*df['quantity'])

        # extract the group from the id column
        split_col = F.split(df['id'], '##')
        df = df.withColumn('group', split_col.getItem(0))

        # Perform group by of each 'group' and sum to get the
        # sum(p_i^t*q_i^t) and sum(p_i^0*q_i^t)
        df_sumpq = (
            df
            .groupBy('group', 'month')
            .sum()
            .select(['group', 'month', 'sum(pt*qt)', 'sum(p0*qt)'])
        )

        # Divide sum of (pt*qt) by sum(p0*qt) to obtain final index
        df_index = df_sumpq.withColumn(
            'index_value',
            F.col('sum(pt*qt)')/F.col('sum(p0*qt)')
        )

        # Append current base month choice to month column
        df_index = df_index.withColumn(
            'pair_of_months',
            F.concat(F.lit(m0), F.lit('_'), F.col('month'))
        )
        df_index = df_index.drop('months')

        # Assign current base month choice indices to dictionary
        bilat_dict[m0] = df_index.orderBy(['group', 'pair_of_months'])

    # Create final dataframe of all combinations
    spark_df = reduce(DataFrame.unionByName, list(bilat_dict.values()))


    return spark_df.select('group' ,'pair_of_months', 'index_value')


def geks_paasche(df):
    """
    Creates the geks Paasche indices table.

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
    dfp =  paasche_bilateral_indices(df)

    # bring dfj to the driver
    dfp = dfp.toPandas()

    # apply geks on the bilateral indices
    dfgp = geks.geks(dfp)

    return dfgp


def rygeks_paasche(df, d):
    """
    Creates the rygeks Paasche indices table.

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
    dfp = paasche_bilateral_indices(df)

    # bring dfj to the driver
    dfp = dfp.toPandas()

    # apply rygeks on the bilateral indices
    dfrgp = geks.rygeks(dfp, d)

    return dfrgp
