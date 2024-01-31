# import spark libraries
from pyspark.sql import functions as F
from pyspark.ml.feature import Bucketizer
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import DoubleType

# import python libraries
from functools import reduce
from importlib import reload
import pandas as pd
import numpy as np
from time import time

# import custom functions
from cprices.cprices.steps import utils
reload(utils)

# import configuration file
from cprices.config import scenarios
reload(scenarios)


def get_sample_count(df, groupby_cols):

    # Create window functions
    w = Window.partitionBy
    w1 = w(groupby_cols + ['product_id'])
    w2 = w(groupby_cols + ['month'])
    w1_m = w(groupby_cols + ['product_id']).orderBy('month')
    w2_m = w(groupby_cols + ['month']).orderBy('month')

    # Getting the date range of the entire dataset
    dt_range = sorted([row.month for row in df.select('month').distinct().collect()])
    m1 = dt_range[0]
    m1_2 = dt_range[0:2]
    cols = groupby_cols + ['month', 'metric', 'value']

    def fixed_base(df, groupby_cols):
        """
        Loop over dt_range to count products in common in
        the first month + the next month in dt_range

        """
        cond = (~F.col('month').isin(m1_2)) & (F.min('month').over(w1) == m1)
        df = (
            df
            .withColumn('flag', F.when(cond,1.0))
            .withColumn('value', F.sum('flag').over(w2))
            .where(F.col('value').isNotNull())
            .drop_duplicates(groupby_cols + ['month'])
            .withColumn('metric', F.lit(m1))
        ).select(cols)
        return df

    # Count products which two months have in common (over a rolling window)
    def bilateral(df, groupby_cols):
        cond = F.lag('month').over(w1_m) == F.col('metric')
        df = (
            df
            .withColumn('metric', F.add_months(F.col('month'), -1))
            .withColumn('flag', F.when(cond, 1.0))
            .withColumn('value', F.sum('flag').over(w2_m))
            .drop_duplicates(groupby_cols + ['month'])
            .filter(F.col('value').isNotNull())
        ).select(cols)
        return df

    sample_count = bilateral(df,groupby_cols).union(fixed_base(df,groupby_cols))

    return sample_count

def get_churn_stats(df, groupby_cols):

    """
    Creates the churn table that shows how many products enter and drop from
    the basket every month for every group.

    Parameters
    ----------
    df : spark dataframe
        Includes the groupby columns and product ids.

    groupby_cols : list of strings
        Includes the columns that can be used to form groups of products, for
        example data supplier. item, retailer, etc

    Returns
    -------
    churn : spark dataframe
        Includes the groupby columns plus metric, month, value.

    Notes
    -----
    The metrics considered in the churn table are:

    * count         : total count of products
    * entered_count : count of products that entered compared to previous month
    * entered_frac  : fraction of products that entered compared to count of previous month
    * dropped_count : count of products that dropped compared to previous month
    * dropped_frac  : fraction of products that dropped compared to count of previous month

    """

    w = Window.partitionBy(groupby_cols + ["product_id"])
    w1 = Window.partitionBy(groupby_cols + ["month"])
    w2 = Window.partitionBy()
    w3 = w.orderBy("month")

    # Partitionied by month count products & create row numbers over later
    df = df.withColumn("count", (F.sum(F.lit(1)).over(w1)).cast("double")).withColumn(
        "row_w1", F.row_number().over(w1.orderBy(groupby_cols))
    )

    # Dropped count:
    # Flag if (latest entry) for the product is earlier than the
    # max date of the dataset or if product drops between months by checking
    # that the next entry has not increased by more than 1 month.
    # Then sum flags for total per month
    df = (
        df.withColumn("row", F.row_number().over(w.orderBy(F.desc("month"))))
        .withColumn(
            "dropped_count",
            (
                F.when(
                    (F.col("row") != 1)
                    & (F.lag("month", -1).over(w3) != F.add_months(F.col("month"), 1)),
                    1,
                )
                .when(
                    (F.col("row") == 1) & (F.max("month").over(w2) > F.col("month")), 1
                )
                .otherwise(0)
            ).cast("double"),
        )
        .withColumn(
            "dropped_count",
            F.sum("dropped_count").over(w1.orderBy(groupby_cols + ["month"])),
        )
    )

    # Entered count: sames as above with minor tweaks
    # Flag if (earliest entry) for the product is later than the
    # min date of the dataset or if product drops between months by checking
    # that the next entry has not decreased by more than 1 month.
    # Then sum flags for total per month
    df = (
        df.withColumn("row", F.row_number().over(w3)).withColumn(
            "entered_count",
            F.when(
                (F.col("row") != 1)
                & (F.lag("month", 1).over(w3) != F.add_months(F.col("month"), -1)),
                1,
            )
            .when((F.col("row") == 1) & (F.min("month").over(w2) < F.col("month")), 1)
            .otherwise(0),
        )
    ).withColumn(
        "entered_count",
        F.sum("entered_count").over(w1.orderBy(groupby_cols + ["month"])),
    )

    # Only need to keep 1 row from each month as the flags are summed now
    # Lag dates for dropped_count e.g. so the count for Jan shows as Feb
    # Add nulls to first entry in dropped and entered_count, so it can be
    # dropped after melt e.g. drop the entries for Jan (start of the time series)
    df = (
        df.where(F.col("row_w1") == 1)
        .withColumn(
            "dropped_count",
            F.lag("dropped_count", 1).over(
                Window.partitionBy(groupby_cols).orderBy("month")
            ),
        )
        .withColumn(
            "entered_count",
            F.when(F.col("dropped_count").isNull(), F.col("dropped_count")).otherwise(
                F.col("entered_count")
            ),
        )
    )

    # get fractions of products that dropped entered
    df = df.withColumn(
        "dropped_frac", F.col("dropped_count") / F.col("count")
    ).withColumn("entered_frac", F.col("entered_count") / F.col("count"))

    # melt the metric columns into one column
    df = utils.melt_df(
        df,
        id_vars=groupby_cols + ["month"],
        value_vars=[
            "count",
            "dropped_count",
            "entered_count",
            "dropped_frac",
            "entered_frac",
        ],
        var_name="metric",
        value_name="value",
    )
    # drop nulls (This drops the first entry/first month
    # for dropped & entered count as there's no value to show)
    cols = groupby_cols + ["value", "month", "metric"]
    churn = df.where(F.col("value").isNotNull()).select(cols)

    return churn

def get_price_stats(df, groupby_cols):

    """
    Creates price_stats table that shows price distribution summary statistics
    for each group.

    Parameters
    ----------
    df : spark dataframe
        Includes the groupby columns and product ids.

    groupby_cols : list of strings
        Includes the columns that can be used to form groups of products, for
        example data supplier. item, retailer, etc

    Returns
    -------
    df : spark dataframe
        Includes the groupby columns plus metric, month, value.

    Notes
    -----
    The metrics considered in the price_stats table are:

    * count of products
    * price average
    * price stddev
    * price min
    * price max

    """
    groupby_cols = groupby_cols + ['month']

    dfg = df.drop('product_id', 'quantity').groupBy(groupby_cols)

    mins = (
        dfg
        .min()
        .withColumnRenamed('min(price)', 'value')
        .withColumn('metric', F.lit('min'))
    )
    maxs = (
        dfg
        .max()
        .withColumnRenamed('max(price)', 'value')
        .withColumn('metric', F.lit('max'))
    )
    avgs = (
        dfg
        .mean()
        .withColumnRenamed('avg(price)', 'value')
        .withColumn('metric', F.lit('mean'))
    )
    stds = (
        dfg
        .agg(F.stddev("price"))
        .withColumnRenamed('stddev_samp(price)', 'value')
        .withColumn('metric', F.lit('stddev'))
    )
    counts = (
        dfg
        .count()
        .withColumnRenamed('count', 'value')
        .withColumn('metric', F.lit('count'))
    )

    return reduce(DataFrame.unionByName, [mins, maxs, avgs, stds, counts])


def get_binary_flag_stats(df, groupby_cols, flag_col):

    """
    Creates dataframe with metrics for each group based on a binary flag
    including counts and fractions of records within each group.

    Parameters
    ----------
    df : spark dataframe
        The classified table from the classification stage of the core
        pipeline. Includes groupby columns as well as 'classified' column.

    groupby_cols : list of strings
        Includes the columns that can be used to form groups of products, for
        example data supplier. item, retailer, etc

    flag_col : string
        Name of binary flag column.

    Returns
    -------
    df : spark dataframe
        Includes the following metrics for each group:

        * total count
        * count of records where flag=1
        * count of records where flag=0
        * fraction of records where flag=1

    """

    groupby_cols = groupby_cols + ['month']

    df.cache().count()

    # TOTAL COUNT

    df1 = df.groupBy(groupby_cols).count()
    df1 = df1.withColumn('metric', F.lit('count'))
    df1 = df1.withColumnRenamed('count', 'value')

    # COUNT OF PRODUCTS WHERE FLAG = 1

    df2 = df.filter(df[flag_col]==1).groupBy(groupby_cols).count()
    df2 = df2.withColumnRenamed('count', 'value')
    df2 = df2.withColumn('metric', F.lit('count1'))

    # COUNT OF PRODUCTS WHERE FLAG = 0

    df3 = df.filter(df[flag_col]==0).groupBy(groupby_cols).count()
    df3 = df3.withColumnRenamed('count', 'value')
    df3 = df3.withColumn('metric', F.lit('count0'))

    # FRACTION OF PRODUCTS WHERE FLAG = 1

    df4 = df.select(groupby_cols + [flag_col])
    df4 = df4.groupBy(groupby_cols).mean()
    df4 = df4.withColumnRenamed(f'avg({flag_col})', 'value')
    df4 = df4.withColumn('metric', F.lit('frac1'))

    # join all dataframes into one table
    df = reduce(DataFrame.unionByName, [df1, df2, df3, df4])

    return df


def distributions(df, groupby_cols, column, n_bins):

    """
    Creates dataframe with price bins and their frequencies for to plot the
    frequency polygons of the price distributions for all groups.

    Parameters
    ----------
    df : spark dataframe
        Includes the groupby columns and product ids.

    groupby_cols : list of strings
        Includes the columns that can be used to form groups of products, for
        example data supplier. item, retailer, etc

    n_bins : integer
        Number of bins to be used for each price distribution.

    column : str
        Column which distribution is calculated on.

    Returns
    -------
    dfb : spark dataframe
        Includes the binned version of the price data where prices
        have been grouped into bins and their frequencies.

    """
    #Specify windows
    w = Window.partitionBy
    cols2 = groupby_cols + ['buckets']
    w1 = w(groupby_cols)
    w2 = w(cols2)

    #Create the values needed to calculate the increment of each bucket
    dfb = (
        df
        .withColumn('min', F.min(column).over(w1))
        .withColumn('max', F.max(column).over(w1))
        .withColumn('step', (F.col('max') - F.col('min')) / n_bins)
        #Empty column to loop over in the next step
        .withColumn('buckets', F.lit(None))
    )

    #Assign bucket no. based on the below condition, when 'price' col is less
    #than 'less_t' ( which is max value - step / bucket no.) insert bucket no.
    cond_1 = F.col(column) <= F.col('less_t')
    for bucket in range(0,n_bins):
        dfb = (
            dfb
            .withColumn('less_t', F.col('max') - (F.col('step') * bucket))
            .withColumn('buckets', F.when(cond_1, bucket).otherwise(F.col('buckets')))
        )

    #Count products in in each bucket and month, then only keep one row per bucket & month
    dfb = (
        dfb
        .withColumn('count', F.count('buckets').over(w2))
        .dropDuplicates(cols2)
    )

    #To add buckets with zero counts I do the below:
    #List with every bucket
    col = ''.join(str(x) for x in [str(r)+',' for r in range(0, n_bins)])[:-1]

    #Select columns for left join, reduce to one entry per group
    #To give every group a complete range of buckets, explode list of bucket for every row/group
    join_cols = groupby_cols + ['max','step']
    df_left =(
        dfb
        .select(join_cols).distinct()
        .withColumn('buckets', F.explode(F.split(F.lit(col),',')))
    )

    #Left join with original dataset, now nulls are the missing buckets with count zero.
    #Fill nulls with 0
    dfb = (
        df_left
        .join(dfb,on = join_cols + ['buckets'], how='left')
        .na.fill(0, 'count')
    )

    #Add column info to new buckets & select def distributions(df, groupby_cols, column, n_bins):
    dfb = (
        dfb
        .withColumn(column, (F.col('max') - (F.col('step')) * F.col('buckets')))
        .select(*groupby_cols, column, F.col('count').cast('double'))
    )
    return dfb


def main(spark, dfs, dev_config):

    """
    Main function for statistics for each group.
    Includes low level indices, item indices, churn statistics, summary
    statistics for price distributions and binary flag statistics from
    classification, outlier detection, imputation and filtering.
    It also includes the binned version of the price data where prices
    have been grouped into bins and their frequencies.

    Parameters
    ----------
    spark :
        spark session

    dfs : dictionary of spark dataframes
        The output tables from the core pipeline to feed into analysis.

    dev_config : python file/module
        Includes groupby columns and config parameters for analysis pipeline.

    Returns
    -------
    stats : spark dataframe
        Output with all time series statistics for all groups and months.
        Has the groupby columns plus month, metric and value columns.
        It includes the following summary statistics:

        * churn stats
        * price distribution summary stats
        * binary flag stats for classification, outlier detection, imputation and filtering
        * low level indices and item indices
        * distributions (frequencies of bins for price frequency polygons)
    """

    params = dev_config.analysis_params
    groupby_cols = dev_config.groupby_cols + ['scenario']

    # initialise empty dictionary of all stats tables to be concatenated
    stats = {}

    # SAMPLE COUNT

    print('* sample count...')
    start = time()

    input_table = params['sample_count']['input_table']
    df = dfs[input_table]
    stats['sample_count'] = get_sample_count(df, groupby_cols)

    dt = time()-start
    print(dt)

    # CHURN STATS

    print('* churn stats...')
    start = time()

    input_table = params['churn']['input_table']
    df = dfs[input_table]
    stats['churn'] = get_churn_stats(df, groupby_cols)

    dt = time()-start
    print(dt)

    # PRICE DISTRIBUTION SUMMARY STATS

    print('* price distribution summary stats...')
    start = time()

    input_table = params['price_stats']['input_table']
    df = dfs[input_table]
    stats['price_stats'] = get_price_stats(df, groupby_cols)

    dt = time()-start
    print(dt)

    # BINARY FLAG STATS
    # CLASSIFICATION, OUTLIER DETECTION, IMPUTATION, FILTERING

    tables = ['classification', 'outlier_detection', 'imputation', 'filtering']
    for table in tables:
        print(f'* {table}...')
        start = time()
        input_table = params[table]['input_table']
        df = dfs[input_table]
        flag_col = params[table]['flag_col']
        metric1 = params[table]['metric1']
        metric2 = params[table]['metric2']
        metric3 = params[table]['metric3']

        # dataframe with metrics
        dfm = get_binary_flag_stats(df, groupby_cols, flag_col)
        dfm = dfm.withColumn(
            'metric',
            F.when(dfm['metric']=='count1', metric1)
            .when(dfm['metric']=='count0', metric2)
            .when(dfm['metric']=='frac1', metric3)
        )
        stats[table] = dfm

        dt = time()-start
        print(dt)

    # INDICES

    print('* indices...')
    start = time()

    stats['low_level_indices'] = (
        dfs['low_level_indices']
        .withColumnRenamed('index_method', 'metric')
        .withColumnRenamed('index_value', 'value')
    )

    stats['item_indices'] = (
        dfs['item_indices']
        .withColumnRenamed('index_method', 'metric')
        .withColumnRenamed('index_value', 'value')
        .withColumn('data_source', F.lit('all_data_sources'))
        .withColumn('supplier', F.lit('all_suppliers'))
        .withColumn('retailer', F.lit('all_retailers'))
    )

    dt = time()-start
    print(dt)

    # DISTRIBUTIONS

    print('* distributions...')
    start = time()

    n_bins = params['distributions']['n_bins']
    column = params['distributions']['column']
    input_table = params['distributions']['input_table']
    df = dfs[input_table]
    stats['distributions'] = distributions(df, groupby_cols+['month'], column, n_bins)
    stats['distributions'] = (
        stats['distributions']
        .withColumnRenamed('price', 'metric')
        .withColumnRenamed('count', 'value')
    )

    dt = time()-start
    print(dt)

    # ADD TABLE COLUMN IN EACH TABLE AND UNION ALL IN ONE

    print('* union analysis...')
    start = time()

    for table in stats:
        stats[table] = stats[table].withColumn('table', F.lit(table))
    stats = reduce(DataFrame.unionByName, list(stats.values()))

    dt = time()-start
    print(dt)

    return stats
