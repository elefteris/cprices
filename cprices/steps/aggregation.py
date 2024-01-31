# import python libraries
import pandas as pd
import os
from importlib import reload

from cprices.cprices.steps import utils
reload(utils)


def main(df, config, dev_config):

    """
    Creates the item indices table.

    Parameters
    ----------
    df : pandas dataframe
        This table includes the index values for all groups

    config : python file/module
        Includes the staged_data parameter with the retailer weights for all
        web-scraped suppliers and items.

    dev_config : python file/module
        Includes the HDFS path to the mappers directory. There is a mapper
        retailer_weights.csv with the groupby columns and "weight"
        that has the weights to aggregate the low level indices (below
        the item level) in order to get the item indices.
        It also includes the groupby_cols.


    Returns
    -------
    df : pandas dataframe
        This table includes the index values for all the groups, months
        and index methods. These indices are on the item level.

    Notes
    -----
    This function multiplies each index value with a weight and then takes
    the sum after grouping by item, index_method, month.
    The result is the weighted average.

    The mapper with the weights is joined with the low level indices.
    The weights will be used to get the weighted average of the
    low level indices to produce the corresponding item index.

    """

    df = df.copy()

    if 'web_scraped' in df['data_source']:

        # dictionary with retailer weights for specific supplier and item
        weights = config.params['staged_data']['web_scraped']

        # create pandas dataframe with retailer weights for web scraped data
        rws = []
        for supplier in weights:
            for item in weights[supplier]:
                for retailer in weights[supplier][item]:
                    weight = weights[supplier][item][retailer]
                    rws.append([supplier, item, retailer, weight])
        rws = pd.DataFrame(rws, columns=['supplier', 'item', 'retailer', 'weight'])
        rws['data_source'] = 'web_scraped'

        # join df with weights to add weight column to the data
        join_cols = dev_config.groupby_cols
        df = pd.merge(df, rws, how='left', on=join_cols)
        # low level indices with no weights will get weight=1
        df['weight'] = df['weight'].fillna(1.0).astype(float)
    else:
        df['weight'] = 1.0

    # multiply index value by weight to get a weighted average index
    df['index_value'] = df['weight']*df['index_value']

    # get aggregated df
    df = (
        df
        .groupby(['item', 'index_method', 'month'])
        .sum()
        .reset_index()
        .drop('weight', 1)
    )

    return df
