# import spark libraries
from pyspark.sql import functions as F

# import python libraries
import pandas as pd
from importlib import reload
from time import time

# import index methods
from cprices.cprices.steps.index_methods import (
    carli,
    dutot,
    fisher,
    geary_khamis,
    jevons,
    laspeyres,
    paasche,
    tornqvist
)
reload(carli)
reload(dutot)
reload(fisher)
reload(geary_khamis)
reload(jevons)
reload(laspeyres)
reload(paasche)
reload(tornqvist)

def product_by_month_tables(df, groupby_cols):

    """
    Creates two product_by_month tables: the first one carries the price
    information and the second one the quantity information.

    Parameters
    ----------
    df : spark dataframe
        A long format table with the groupby columns such as the
        data source, supplier, item, retailer as well as the product_id, month,
        price and quantity.

    groupby_cols : list of strings
        Includes the columns that can be used to form groups of products, for
        example data supplier. item, retailer, etc

    Returns
    -------
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

    df : spark dataframe

    Notes
    -----
    The reason that the 'id' column is created is to merge and maintain all
    the information for the group as defined by several groupby columns and the
    product_id in one column that we can later split for convenience.

    The pivoting provides a more convenient data structure to apply the index
    methods given how they are defined.

    """

    # concatenate the columns that form a group
    df = df.withColumn('group', F.concat_ws('//', *groupby_cols))

    # the id column concatenates the group column with the product_id column
    df = df.withColumn(
        'id',
        F.concat(F.col('group'), F.lit('##'), F.col('product_id'))
    )
    df = df.drop('group', 'product_id')

    # create products (rows) by all months (columns) dataframe with prices
    dfp = (
        df
        .select(['id', 'month', 'price'])
        .groupby(df['id'])
        .pivot('month')
        .sum('price')
    )

    # create products (rows) by all months (columns) dataframe with quantities
    dfq = (
        df
        .select(['id', 'month', 'quantity'])
        .groupby(df['id'])
        .pivot('month')
        .sum('quantity')
    )

    return df, dfp, dfq



def main(spark, df, config, dev_config):

    """
    Uses the prices data to produce consumer price indices for each group and
    month using a number of selected index methods.

    Parameters
    ----------
    spark :
        spark session

    df : spark dataframe
        A long format table with groupby columns as well as product_id, month,
        price, quantity.

    config : python file/module
        It includes the user-selected index methods to be used for the index
        calculations.

    dev_config : python file/module
        Includes the groupby columns.

    Returns
    -------
    df : pandas dataframe
        The dataframe with the indices has the groupby columns as well
        as the index_method, month and index_value columns.

    Notes
    -----
    The input price dataset goes through each user-selected index method to
    produce the corresponding index values for all groups and months.

    All the tables from the index methods are then concatenated into one table.
    The column 'index_method' in the final table specifies the name of the
    index method and can be used to split the table.

    """
    groupby_cols = dev_config.groupby_cols

    # methods selected by the user in the configuration
    selected_methods = config.params['indices']['methods']

    # length of rolling window for rygeks
    splice_window_length = config.params['indices']['splice_window_length']

    # determine whether calculating Geary Khamis index forevery period in range specified in config
    geary_khamis_all_periods = config.params['indices']['geary_khamis_all_periods']
    geary_khamis_convergence_tolerance = config.params['indices']['geary_khamis_convergence_tolerance']
    GK_index_filepath='/home/cdsw/GK_index_full.csv'

    # pivot by month to create prices and quantities dataframes
    df, dfp, dfq = product_by_month_tables(df, groupby_cols)

    # all available methods with their arguments
    METHODS_DICT = {
        # UNWEIGHTED
        'carli_fixed_base' : {
            'method' : carli.carli_fixed_base,
            'args' : {'df' : dfp}
        },
        'carli_chained' : {
            'method' : carli.carli_chained,
            'args' : {'df' : dfp}
        },
        'dutot_fixed_base' : {
            'method' : dutot.dutot_fixed_base,
            'args' : {'df' : dfp}
        },
        'dutot_chained' : {
            'method' : dutot.dutot_chained,
            'args' : {'df' : dfp}
        },
        'jevons_fixed_base' : {
            'method' : jevons.jevons_fixed_base,
            'args' : {'df' : dfp}
        },
        'jevons_chained' : {
            'method' : jevons.jevons_chained,
            'args' : {'df' : dfp}
        },
        'geks_jevons' : {
            'method' : jevons.geks_jevons,
            'args' : {'df' : dfp}
        },
        'rygeks_jevons' : {
            'method' : jevons.rygeks_jevons,
            'args' : {'df' : dfp, 'd' : splice_window_length}
        },
        # WEIGHTED
        'laspeyres_fixed_base' : {
            'method' : laspeyres.laspeyres_fixed_base,
            'args' : {'dfp' : dfp, 'dfq' : dfq}
        },
        'geks_laspeyres' : {
            'method' : laspeyres.geks_laspeyres,
            'args' : {'df' : df}
        },
        'rygeks_laspeyres' : {
            'method' : laspeyres.rygeks_laspeyres,
            'args' : {'df' : df, 'd' : splice_window_length}
        },
        'geks_paasche' : {
            'method' : paasche.geks_paasche,
            'args' : {'df' : df}
        },
        'rygeks_paasche' : {
            'method' : paasche.rygeks_paasche,
            'args' : {'df' : df, 'd' : splice_window_length}
        },
        'rygeks_fisher' : {
            'method' : fisher.rygeks_fisher,
            'args' : {'df' : df, 'd' : splice_window_length}
        },
        'tornqvist_fixed_base' : {
            'method' : tornqvist.tornqvist_fixed_base,
            'args' : {'df' : df}
        },
        'geks_tornqvist' : {
            'method' : tornqvist.geks_tornqvist,
            'args' : {'df' : df}
        },
        'rygeks_tornqvist' : {
            'method' : tornqvist.rygeks_tornqvist,
            'args' : {'df' : df, 'd' : splice_window_length}
        },
        'geary_khamis' : {
            'method' : geary_khamis.geary_khamis,
            'args' : {
                'df' : df,
                'geary_khamis_all_periods' : geary_khamis_all_periods,
                'geary_khamis_convergence_tolerance' : geary_khamis_convergence_tolerance,
                'GK_index_filepath' : GK_index_filepath
            }
        }
    }

    # initialize empty list to keep appending indices for each method
    # which will be concatenated into one pandas dataframe
    df = []

    # calculate indices for each method and append tables from all methods
    for name in selected_methods:
        print(f'* {name}...')
        start = time()
        # collect method and arguments
        method = METHODS_DICT[name]['method']
        args = METHODS_DICT[name]['args']
        # run method
        dfm = method(**args)
        dt = time()-start
        print(dt)
        dfm['index_method'] = name
        df.append(dfm)

    # append all dataframes from all index methods into one dataframe
    df = pd.concat(df).reset_index(drop=True)

    # split groupby columns
    pdcols = df['group'].str.split('//', len(groupby_cols)).str
    for i, c in enumerate(groupby_cols):
        df[c] = pdcols[i]

    # select and re-order columns
    cols = groupby_cols + ['index_method', 'month', 'index_value']

    return df[cols]
