# import spark libraries
from pyspark.sql import Window
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# import python libraries
import pandas as pd
from functools import reduce
from importlib import reload

# import custom libraries
from cprices.cprices.steps import utils
from cprices.cprices.steps.index_methods import geks
reload(utils)
reload(geks)

def tornqvist_fixed_base(df):

    """Creates the Tornqvist fixed base indices table using window functions.

    This function calculates the Tornqvist indices :math:\P_T^t\ for period :math:`t`
    using the following formula:

    .. math::
        P_T^t = \prod_{i=1}^n \left(\frac{p_i^t}{p_i^0}\right)^{\frac{w_i^0 + q_i^t}{2}},

    where
    .. math::
        w_i^k = \frac{p_i^k q_i^k}{\sum_{j=1}^n p_j^k q_j^k},\, k\in[0,t]

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
    pandas_df : pandas dataframe
        The output dataframe has the following columns:
        data source, supplier, item, retailer, index_method, month, index_value.
        It has the index value using tornqvist fixed base for all the combinations
        of data sources, suppliers, items, retailers and months.

    Notes
    -----
    The mathematical theory behind the Tornqvist fixed base index method can be
    found here: <LINK>

    To make the calculation of the Tornqvist index simpler, we take the natural
    logarithm of the formula, and then take the exponent in the final step to get
    back to the index value.
    """

    # collect all months and sort into sequential order
    months = sorted([row.month for row in df.select('month').distinct().collect()])

    # Record the first month across the whole period covered in the data
    m0 = months[0]

    # Remove any null values from all over months
    df = df.dropna()

    # Tornqvist needs prices and quantities from base period so only products
    # that exist in the base period can be used to get Tornqvist index. We
    # achieve this by joining dataframe of unique entries in base month to full
    # dataframe
    df_base_month_id  = (
        df
        .where(df['month'] == m0)
        .select('id')
    )

    # Join the nonnull base month price/quantities onto main dataframe
    df = df.join(F.broadcast(df_base_month_id), 'id')

    # if the join is removed the number of rows in the dataframe is wrong!
#    print(f'dataframe len = {df.count()}')

    # Calculate the price relative p_i^t/p_i^0 between base month m0 and month of interest m
    # and take the natural log of calculation
    df = (
        df
        .withColumn('ln(pt/p0)',
                    F.log(
                        F.col('price')
                        /F.first('price').over(Window
                                               .partitionBy('id')
                                               .orderBy('month')
                                              )
                    )
                   )
    )

    # Calculate p_i^t*q_i^t
    df = df.withColumn('pt*qt', df['price']*df['quantity'])

    # Calculate p_i^0*q_i^0 - unique base month weight for each period
    df = (
        df
        .withColumn('p0*q0',
                    F.first('price').over(Window
                                          .partitionBy('id')
                                          .orderBy('month')
                                         )
                    *F.first('quantity').over(Window
                                              .partitionBy('id')
                                              .orderBy('month')
                                             )
                   )
    )

    # extract the group from the id column
    split_col = F.split(df['id'], '##')
    df = df.withColumn('group', split_col.getItem(0))

    # Calculate the expenditure shares w_i^t
    df = (
        df
        .withColumn('wt',
                    df['pt*qt']
                    /F.sum(df['pt*qt']).over(Window
                                             .partitionBy([
                                                 'group',
                                                 'month'
                                             ])
                                            )
                   )
    )

    # Calculate the expenditure shares w_i^0
    df = (
        df
        .withColumn('w0',
                    df['p0*q0']
                    /F.sum(df['p0*q0']).over(Window
                                             .partitionBy([
                                                 'group',
                                                 'month'
                                             ])
                                            )
                   )
    )

    # Combine the two expenditure shares for each combination of base period
    # and other period
    df = df.withColumn('(w0+wt)/2', (df['w0']+df['wt'])*0.5)

    # Combine relevant w0, wt and ln(pt/p0) terms for each month
    df = df.withColumn('(w0+wt)/2*ln(pt/p0)',
                       df['(w0+wt)/2']*df['ln(pt/p0)']
                      )

    # Perform the sum of all individual items over each month
    df_index = (
        df
        .groupBy('group', 'month')
        .sum()
        .select(['group', 'month', 'sum((w0+wt)/2*ln(pt/p0))'])
    )

    # Take the natural exponent of values to obtain the Tornqvist index
    df_index = df_index.withColumn('index_value',
                                   F.exp('sum((w0+wt)/2*ln(pt/p0))')
                                  )

    # Drop unneeded column
    df_index = df_index.drop('sum((w0+wt)/2*ln(pt/p0))')

    # toPandas not working unless month is datetime or string
    df_index = df_index.withColumn('month', df_index['month'].cast(StringType()))


    # Bring aggregated data to the driver
    df = df_index.toPandas().reset_index(drop=True)

    return df[['group', 'month', 'index_value']]


def tornqvist_bilateral_indices(input_df):
    """Creates the Tornqvist bilateral indices table using window functions.

    This function calculates the Tornqvist indices :math:\P_T^t\ for period :math:`t`
    using the following formula:

    .. math::
        P_T^t = \prod_{i=1}^n \left(\frac{p_i^t}{p_i^0}\right)^{\frac{w_i^0 + q_i^t}{2}},

    where
    .. math::
        w_i^k = \frac{p_i^k q_i^k}{\sum_{j=1}^n p_j^k q_j^k},\, k\in[0,t]

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
    spark_df : spark dataframe
        The output dataframe has the following columns:
        data source, supplier, item, retailer, index_method, month, index_value.
        It has the index value using tornqvist fixed base for all the combinations
        of data sources, suppliers, items, retailers and months.

    Notes
    -----
    The mathematical theory behind the Tornqvist fixed base index method can be
    found here: <LINK>

    To make the calculation of the Tornqvist index simpler, we take the natural
    logarithm of the formula, and then take the exponent in the final step to get
    back to the index value.
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

        # Tornqvist needs prices and quantities from base period so only products
        # that exist in the base period can be used to get Tornqvist index. We
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

        # Calculate the price relative p_i^t/p_i^0 between base month m0 and month of interest m
        # and take the natural log of calculation
        df = df.withColumn('ln(pt/p0)', F.log( df['price']/df['price_0']))

        # Calculate p_i^t*q_i^t
        df = df.withColumn('pt*qt', df['price']*df['quantity'])

        # Calculate p_i^0*q_i^0 - unique base month weight for each period
        df = df.withColumn('p0*q0', df['price_0']*df['quantity_0'])

        # extract the group from the id column
        split_col = F.split(df['id'], '##')
        df = df.withColumn('group', split_col.getItem(0))

        # Perform group by of each 'group' and sum to get the
        # sum(p_i^t*q_i^t) and sum(p_i^0*q_i^0)

        df = (
            df
            .withColumn('sum(pt*qt)',
                        F.sum(df['price']*df['quantity'])
                        .over(Window
                              .partitionBy([
                                  'group',
                                  'month'
                              ])
                             )
                       )
        )

        df = (
            df
            .withColumn('sum(p0*q0)',
                        F.sum(df['price_0']*df['quantity_0'])
                        .over(Window
                              .partitionBy([
                                  'group',
                                  'month'
                              ])
                             )
                       )
        )

        # Calculate the expenditure shares w_i^t
        df = df.withColumn('wt', df['pt*qt']/df['sum(pt*qt)'])

        # Calculate the expenditure shares w_i^0
        df = df.withColumn('w0', df['p0*q0']/df['sum(p0*q0)'])

        # Combine the two expenditure shares for each combination of base period
        # and other period
        df = df.withColumn('(w0+wt)/2', (df['w0']+df['wt'])*0.5)

        # Combine relevant w0, wt and ln(pt/p0) terms for each item
        df = df.withColumn(
            '(w0+wt)/2*ln(pt/p0)',
            df['(w0+wt)/2']*df['ln(pt/p0)']
        )

        # Perform the sum of all individual items over each month
        df_index = (
            df
            .groupBy('group', 'month')
            .sum()
            .select(['group', 'month', 'sum((w0+wt)/2*ln(pt/p0))'])
        )

        # Take the natural exponent of values to obtain the Tornqvist index
        df_index = df_index.withColumn(
            'index_value',
            F.exp('sum((w0+wt)/2*ln(pt/p0))')
        )

        # Append current base month choice to month column
        df_index = df_index.withColumn(
            'pair_of_months',
            F.concat(F.lit(m0), F.lit('_'), F.col('month'))
        )

        # Drop unneeded column
        df_index = df_index.drop('sum((w0+wt)/2*ln(pt/p0))', 'month')

        # Assign current base month choice indices to dictionary
        bilat_dict[m0] = df_index.orderBy(['group', 'pair_of_months'])

    # Create final dataframe of all combinations
    spark_df = reduce(DataFrame.unionByName, list(bilat_dict.values()))

    return spark_df.select('group' ,'pair_of_months', 'index_value')


def geks_tornqvist(df):
    """
    Creates the geks Tornqvist indices table.

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
    dft = tornqvist_bilateral_indices(df)

    # bring dfj to the driver
    dft = dft.toPandas()

    # apply geks on the bilateral indices
    dfgt = geks.geks(dft)

    return dfgt


def rygeks_tornqvist(df, d):
    """
    Creates the rygeks Tornqvist indices table.

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
    dft = tornqvist_bilateral_indices(df)

    # bring dfj to the driver
    dft = dft.toPandas()

    # apply rygeks on the bilateral indices
    dfrgt = geks.rygeks(dft, d)

    return dfrgt
