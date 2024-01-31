# import spark libraries
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import IntegerType, StringType

# import python libraries
import pandas as pd
import os
#from importlib import reload

# import configuration files
#from cprices.cprices.steps import utils
#reload(utils)


def calculate_geary_khamis(df, convergence_tolerance):
    """Creates the Geary Khamis indices table.

    This function calculates the Geary Khamis indices :math:\P_{GK}^t\ for period :math:`t`
    using the following formula:

    Parameters
    ----------
    df  : spark dataframe
        The first column of df is 'id' which combines the group_id and the
        product_id so that values in the 'id' column have the form
        'data_source//supplier//item//retailer##product_id'
        which can be split into the corresponding 5 columns later using the
        delimiter ## these two components can be split in two columns.

        The rest of the columns are the month, price and quantity values.

    convergence_tolerance: float

    Returns
    -------
    pandas_df : pandas dataframe
        The output dataframe has the following columns:
        data source, supplier, item, retailer, index_method, month, index_value.
        It has the index value using Geary Khamis for all the combinations
        of data sources, suppliers, items, retailers and months.

    Notes
    -----
    The mathematical theory behind the Geary Khamis index method can be
    found here: <LINK>
    """

    df = df.dropna()

    # extract the group from the id column
    split_col = F.split(df['id'], '##')
    df = df.withColumn('group', split_col.getItem(0))

    ########
    #First calculate all static values for method

    # Calculate the turnover for individual items
    # Sum turnover (price*quantity) of all items for each month
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

    # Calculate the value index (VI) for each individual item using current month and base month
    df = (
        df
        .withColumn('sum(pt*qt)/sum(p0*q0)',
                    F.col('sum(pt*qt)')
                    /F.first('sum(pt*qt)').over(Window
                                                .partitionBy('group')
                                                .orderBy('month')
                                               )
                   )
    )

    # Calc phi_t for each item
    df = (
        df
        .withColumn('phi_i',
                    df['quantity']/(
                        F.sum('quantity')
                        .over(Window
                              .partitionBy('id')
                             )
                    )
        )
    )


    def flag_tolerence_threshold(x):
        '''
        Function flags rows depending on whether x exceeds threshold value.


        Parameter
        ---------
        x: float
            value for checking against threshold
        Returns
        -------
        0 or 1: int
            returns 1 if exceeds threshold, 0 otherwise

        Note
        ----
        Used as user defined function on spark dataframe
        '''
        if x > convergence_tolerance:
            return int(1)
        else:
            return int(0)

    # Assign flag function to spark UDF
    flag_udf = F.udf(flag_tolerence_threshold, IntegerType())

    # Set up initial P^GK and flag values for use in iterative solution
    df = df.withColumn('P^GK', F.lit(1.0))
    df = df.withColumn('flag', F.lit(1))

    # While the difference between P^GK_n and P^GK_n+1 exceed the given
    # threshold we continue to iterate solution
    i = 0
    while df.select(F.sum('flag')).collect()[0][0] > 0:

        # Increment iteration count
        i += 1

        # Replace previous P^GK value with current P^GK for next iteration
        df = df.drop('P^GK_previous')
        df = df.withColumnRenamed('P^GK', 'P^GK_previous')

        # calculate phi_i
        df = (
            df
            .withColumn('v_i',
                        F.sum(df['phi_i']*df['price']/df['P^GK_previous'])
                        .over(Window
                              .partitionBy('id')
                             )
                       )
        )

        df = df.withColumn('QAV_t', df['v_i']*df['quantity'])

        # Calculate the QAV
        df = (
            df
            .withColumn('sum(QAV_t)',
                        F.sum(df['v_i']*df['quantity'])
                        .over(Window
                              .partitionBy([
                                  'group',
                                  'month'
                              ])
                             )
                       )
        )

        # Calculate the quality index (QI) for each individual item using current month and base month
        df = (
            df
            .withColumn('sum(QAV_t)/sum(QAV_0)',
                        F.col('sum(QAV_t)')
                        /F.first('sum(QAV_t)').over(Window
                                                    .partitionBy('group')
                                                    .orderBy('month')
                                                   )
                       )
        )

        # Calculate new GK index
        df = df.withColumn('P^GK', df['sum(pt*qt)/sum(p0*q0)']/df['sum(QAV_t)/sum(QAV_0)'])

        # Calculate whether P^GK differences have fallen below threshold and flag appropriately
        df = df.withColumn('flag', flag_udf(df['P^GK']-df['P^GK_previous']))

        df = df.withColumn('Iteration #', F.lit(i))

        # Caching computations - better performance here than outside of loop
        df.cache()
#        df.cache().orderBy(['id','month']).show(50)
#        df.cache().orderBy(['id','month']).show(300)

#    df.show()
    print(f'computed GK using {i} iterations ')

    # get index for each group and month by finding distinct rows
    df = df.select('group' ,'month', 'P^GK').distinct()


    # PANDAS
     # toPandas not working unless month is datetime or string
    df = df.withColumn('month', df['month'].cast(StringType()))

    # bring index values to the driver
    df_final = df.toPandas()
    df_final = df_final.rename(columns={'P^GK' : 'index_value'})

    return df_final[['group', 'month', 'index_value']]


def subset_df(df, months_subset):
    """Subsets dataframe to only include the months provided

    Parameter
    ---------
    df: spark dataframe

    months_subset: list

    Returns
    -------
    df_subset: spark dataframe

    Notes
    -----
    DOES NOT ENSURE CONTINUOUS SET OF MONTHS
    """

    df_subset = df.filter(F.col('month').isin(months_subset))

    return df_subset


def append_gk_index(df, GK_index_filepath):
    """Appends the most recent months GK index from df to the backseries.

    If the backseries doesn't exist a new backseries is started. If the new GK
    index value already exists in the backseries the new value overwrites the
    old.

    Parameter
    ---------
    df: pandas dataframe

    GK_index_filepath: string

    Parameter
    ---------
    df_backseries: pandas dataframe
    """


    # We are only concerned with the most recent value from the calculated GK index series
    df_indices_new = df.sort_values('month').groupby('group').last().reset_index()

    #If other GK values exist we load them in, else we initalise an empty df for previous values
    if not os.path.exists(GK_index_filepath):
        df_indices_prev = pd.DataFrame(None, columns=['group', 'month', 'index_value'])
    else:
        df_indices_prev = pd.read_csv(GK_index_filepath)

    # Append new GK indices to previous entries, replacing any duplicates from previous with new
    # values
    df_backseries =  (
        pd.concat([df_indices_prev, df_indices_new])
        .drop_duplicates(
            subset=['group', 'month'],
            keep='last'
        )
        .reset_index(drop=True)
    )

    df_backseries.to_csv(GK_index_filepath, index=False)


    return df_backseries


def geary_khamis(
    df,
    run_all_periods,
    convergence_tolerance,
    GK_index_filepath
):
    """ Computes the Geary Khamis index.

    This function will compute the Geary Khamis index from the input dataframe
    for the most recent month in the dataframe (and will optionally produce the
    GK indices for all months in the input if specified by the user).

    Parameter
    ---------
    df: spark dataframe

    run_all_periods: boolean

    convergence_tolerance: float

    GK_index_filepath: string

    Returns
    -------
    df_indices_backseries : pandas dataframe
        The output dataframe has the columns group, month and index_value.

    """

    if run_all_periods:
        # Compute the Geary Khamis index for every period in range specified in scenario

        # collect all months and sort into sequential order
        months = sorted([row.month for row in df.select('month').distinct().collect()])

        # Loop over all
        for i, month in enumerate(months):
            months_subset = months[:i+1]
            print(months_subset)
            df_subset = subset_df(df, months_subset)

            df_indices_current = calculate_geary_khamis(df_subset, convergence_tolerance)

            df_indices_backseries = append_gk_index(df_indices_current, GK_index_filepath)
            print(df_indices_backseries.tail())

    else:
        # Only computing Geary Khamis for most recent period
        df_indices_current = calculate_geary_khamis(df, convergence_tolerance)

        df_indices_backseries = append_gk_index(df_indices_current, GK_index_filepath)


    return df_indices_backseries[['group', 'month', 'index_value']]
