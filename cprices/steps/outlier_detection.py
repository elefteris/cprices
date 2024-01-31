# import spark libraries
from pyspark.sql import functions as F
from pyspark.sql import SQLContext, Window, DataFrame

# import python libraries
import pandas as pd
from functools import reduce

def ksigma(df, groupby_cols, column, k):

    """
    Adds outlier flag column to spark dataframe based on whether the values of
    the column of interest fall outside a lower and upper fence using ksigma
    rule.

    Parameters
    ----------
    df : spark dataframe
        Input dataset with column to check for outliers.

    groupby_cols : list of strings
        Column names by which df will be grouped.

    column : string
        Column of interest with numeric values, possibly Null.

    k : float
        Number of std around mean for lower and upper fences.

    Returns
    -------
    df : spark dataframe
        Output dataframe with additional column/binary flag for outliers.

    Notes
    -----
    The function groups df by the groupby_cols columns and then within each
    group it finds the mean and standard deviation. They are used to create
    the lower and upper fences as follows:

    * lower fence: lf = mean-k*std
    * upper fence: uf = mean+k*std

    Then each value in the column of interest is compared to the lower and upper
    fences within each group to create a binary flag/column showing whether each
    value is an outlier (1) or an inlier (0).

    If any value of the numerical column of interest is Null, the binary outlier
    flag will also be Null.

    """

    # add mean and std for each group
    window = Window.partitionBy(groupby_cols)

    df = df.withColumn('mean', F.mean(df[column]).over(window))
    df = df.withColumn('std', F.stddev(df[column]).over(window))

    # upper and lower outlier fences
    df = (
        df
        .withColumn('lf', df['mean']-k*df['std'])
        .withColumn('uf', df['mean']+k*df['std'])
    )

    # create outlier flag column
    df = df.withColumn(
        'outlier',
        F.when((df[column] <= df['uf']) & (df[column] >= df['lf']),0)
        .when((df[column] > df['uf']) | (df[column] < df['lf']),1)
        .otherwise(None)
    )

    # drop intermediate columns used to create the outlier flag
    df = df.drop(*['mean', 'std', 'lf', 'uf'])

    return df


def tukey(df, groupby_cols, column, k, spark):

    """
    Adds outlier flag column to spark dataframe based on whether the values of
    the column of interest fall outside a lower and upper fence using tukey
    fences.

    Parameters
    ----------
    df : spark dataframe
        Input dataset with column to check for outliers.

    groupby_cols : list of strings
        Column names by which df will be grouped.

    column : string
        Column of interest with numeric values, possibly Null.

    k : float
        Number of interquartile range (IQR) used to calculate lower and upper
        fences.

    Returns
    -------
    df : spark dataframe
        Output dataframe with additional column/binary flag for outliers.

    Notes
    -----
    The function groups df by the groupby_cols columns and then within each
    group it finds the first (q1) and third (q3) quartiles. They are used to
    create the lower and upper fences as follows:

    * interquartile range: iqr = q3-q1
    * lower fence: lf = q1-k*iqr
    * upper fence: uf = q3+k*iqr

    Then each value in the column of interest is compared to the lower and upper
    fences within each group to create a binary flag/column showing whether each
    value is an outlier (1) or an inlier (0).

    If any value of the numerical column of interest is Null, the binary outlier
    flag will also be Null.

    """

    # create dataframes with quartiles q1, q3 for each group
    sc = spark.sparkContext
    sqlContext = SQLContext(sc)

    df.registerTempTable("df")
    gcols = ', '.join(groupby_cols)

    command = (
        f"select {gcols}, " +
        f"percentile_approx({column},0.25) as q1, " +
        f"percentile_approx({column},0.75) as q3 " +
        f"from df group by {gcols}"
    )

    quartiles = sqlContext.sql(command)
    df = df.join(quartiles, groupby_cols, how='left')

    # interquartile range
    df = df.withColumn('iqr',df['q3']-df['q1'])

    # upper and lower Tukey fences
    df = (
        df
        .withColumn('lf', df['q1']-k*df['iqr'])
        .withColumn('uf', df['q3']+k*df['iqr'])
    )

    # create outlier flag column
    df = df.withColumn(
        'outlier',
        F.when((df[column] <= df['uf']) & (df[column] >= df['lf']),0)
        .when((df[column] > df['uf']) | (df[column] < df['lf']),1)
        .otherwise(None)
    )

    # drop intermediate columns used to create the outlier flag
    df = df.drop(*['q1', 'q3', 'iqr', 'lf', 'uf'])

    return df


def kimber(df, groupby_cols, column, k, spark):

    """
    Adds outlier flag column to spark dataframe based on whether the values of
    the column of interest fall outside a lower and upper fence using kimber
    fences.

    Parameters
    ----------
    df : spark dataframe
        Input dataset with column to check for outliers.

    groupby_cols : list of strings
        Column names by which df will be grouped.

    column : string
        Column of interest with numeric values, possibly Null.

    k : float
        Number of semi-interquartile ranges used to calculate the lower and
        upper fences.

    Returns
    -------
    df : spark dataframe
        Output dataframe with additional column/binary flag for outliers.

    Notes
    -----
    The function groups df by the groupby_cols columns and then within each
    group it finds the first quartile (q1) the second quartile or median (q2)
    and the third quartile (q3). They are used to create the lower and upper
    fences as follows:

    * lower semi-interquartle range: lsiqr = q2-q1
    * upper semi-interquartle range: usiqr = q3-q2
    * lower fence: lf = q1-k*lsiqr
    * upper fence: uf = q3+k*usiqr

    Then each value in the column of interest is compared to the lower and upper
    fences within each group to create a binary flag/column showing whether each
    value is an outlier (1) or an inlier (0).

    If any value of the numerical column of interest is Null, the binary outlier
    flag will also be Null.

    """

    sc = spark.sparkContext
    sqlContext = SQLContext(sc)

    df.registerTempTable("df")
    gcols = ', '.join(groupby_cols)

    command = (
        f"select {gcols}, " +
        f"percentile_approx({column},0.25) as q1, " +
        f"percentile_approx({column},0.5) as q2, " +
        f"percentile_approx({column},0.75) as q3 " +
        f"from df group by {gcols}"
    )

    quartiles = sqlContext.sql(command)
    df = df.join(quartiles, groupby_cols, how='left')

    # lower and upper semi-interquartile ranges
    df = (
        df
        .withColumn('lsiqr',df['q2']-df['q1'])
        .withColumn('usiqr',df['q3']-df['q2'])
    )

    # upper and lower kimber fences
    df = (
        df
        .withColumn('lf', df['q1']-k*df['lsiqr'])
        .withColumn('uf', df['q3']+k*df['usiqr'])
    )

    # create outlier flag column
    df = df.withColumn(
        'outlier',
        F.when((df[column] <= df['uf']) & (df[column] >= df['lf']),0)
        .when((df[column] > df['uf']) | (df[column] < df['lf']),1)
        .otherwise(None)
    )

    # drop intermediate columns used to create the outlier flag
    df = df.drop(*['q1','q2', 'q3', 'lsiqr', 'usiqr', 'lf', 'uf'])

    return df


def create_df_fences(spark, fences):

    """
    Creates a spark dataframe using the fences dictionary.

    Parameters
    ----------
    spark :
        spark session

    fences : python dictionary
        Each key corresponds to an item and it has a list with the lower and
        upper fences.

    Returns
    -------
    df_fences : spark dataframe
        It has 3 columns: 'item', 'lf', 'uf'

    Notes
    -----
    * The df_fences will later be joined with the prices dataset on ['item'].
    * Two columns are added for upper and lower fences ('lf', 'uf').
    * They are different across different items.

    """

    df_fences = []

    for item in fences:
        df_fences.append([item] + fences[item])

    df_fences = pd.DataFrame(df_fences, columns=['item', 'lf', 'uf'])
    df_fences = spark.createDataFrame(df_fences)

    return df_fences



def udf_fences(spark, df, column, fences):

    """
    Adds outlier flag column to spark dataframe based on whether the values of
    the column of interest fall outside the user-defined lower and upper fences.

    Parameters
    ----------
    spark :
        spark session

    df : spark dataframe
        Input dataset with column to check for outliers.

    column : string
        Column of interest with numeric values, possibly Null.

    fences : python dictionary
        Each key corresponds to an item and it has a list with the lower and
        upper fences.

    Returns
    -------
    df : spark dataframe
        Output dataframe with additional column/binary flag for outliers.

    Notes
    -----
    Each item has its own user-defined upper and lower price fences from the
    config file.

    """

    df_fences = create_df_fences(spark, fences)

    df = df.join(df_fences, ['item'], 'left')

    # create outlier flag column
    df = df.withColumn(
        'outlier',
        F.when((df[column] <= df['uf']) & (df[column] >= df['lf']),0)
        .when((df[column] > df['uf']) | (df[column] < df['lf']),1)
        .otherwise(None)
    )

    # drop intermediate columns used to create the outlier flag
    df = df.drop(*['lf', 'uf'])

    return df


def find_outliers(
    spark,
    df,
    column,
    groupby_cols,
    active,
    log_transform,
    method,
    k,
    fences
):

    """
    Adds outlier flag column for column of interest based on the user-specified
    outlier detection method.

    Parameters
    ----------
    spark :
        spark session

    df : spark dataframe
        Input dataset with column to check for outliers.

    column : string
        The name of column to detect outliers ('price')

    groupby_cols : list of strings
        These columns form subsets of dataframe to detect outliers on the group
        level.

    active : boolean
        Whether the outlier detection is on/off

    log_transform : boolean
        Whether to apply log transformation on column of interest.

    method : string
        The name of the outlier detection method to be used.
        Available options are: 'tukey', 'kimber', 'ksigma', 'udf_fences'.

    k : int or float
        The k parameter used in 'tukey', 'kimber', 'ksigma' methods.

    fences : dictionary of lists of int or float
        This dictionary includes all the names of the items in the data as keys.
        For each key (item) the corresponding value is a list with two elements.
        The first one is the lower fence and the second one is the upper fence.

    Returns
    -------
    df : spark dataframe
        The original inpit dataframe with an extra outlier flag column that is
        binary (0 for inliers, 1 for outliers)

    """

    # if outlier detection module is off, add column showing all values as inliers
    if not active:
        df = df.withColumn('outlier', F.lit(0))

    else:
        # log transform column of interest
        if log_transform and method != 'udf_fences':
            df = df.withColumn('log', F.log(df[column]))
            column = 'log'

        # apply outlier detection method
        if method == 'tukey':
            df = tukey(df, groupby_cols, column, k, spark)

        elif method == 'kimber':
            df = kimber(df, groupby_cols, column, k, spark)

        elif method == 'ksigma':
            df = ksigma(df, groupby_cols, column, k)

        elif method == 'udf_fences':
            df = udf_fences(spark, df, column, fences)

        else:
            raise Exception(
                f'{method} is not among the available outlier detection ' +
                'methods: ksigma, kimber, tukey, udf_fences'
            )

        # drop log column
        if log_transform and method != 'udf_fences':
            df = df.drop('log')

    return df


def main(spark, dfs, config, dev_config):

    """
    Main function for outlier detection across all data sources dataframes.

    Parameters
    ----------
    spark :
        spark session

    dfs : dictionary of spark dataframes
        Input dataframes with column to check for outliers.
        Each key/dataframe corresponds to a different data source
        (web-scraped, scanner, conventional)

    config : python file/module
        It has all the configuration parameters that may be needed for outlier
        detection.

    dev_config : python file/module
        Includes the groupby columns.

    Returns
    -------
    inliers_outliers : spark dataframe
        The original input dataframes (one for each data source) get an
        additional outlier flag column each showing whether the value in the
        column of interest is an outlier and then they are appended in
        the inliers_outliers table. This one will be stored in HDFS for
        analysis.

    inliers : dictionary of spark dataframes
        Same structure as dfs but for each dataframe (data source) the outliers
        have been removed as well as the outlier flag column. This dictionary
        will continue to the next stage.
    """

    # get parameters for outlier detection from config file
    params = config.params['outlier_detection']
    groupby_cols = dev_config.groupby_cols + ['month']

    inliers = {}

    for data_source in dfs:
        df = find_outliers(
            spark,
            df = dfs[data_source],
            column = 'price',
            groupby_cols = groupby_cols,
            active= params['active'],
            log_transform = params['log_transform'],
            method = params['method'],
            k = params['k'],
            fences = params['fences']
        )

        # replace dataframe in dfs with the one that has the outlier flag
        dfs[data_source] = df

        # separate the subset of inliers rows
        # they will continue to the next stage inside the inliers dictionary
        inliers[data_source] = df.filter(df['outlier']==0).drop('outlier')

    # union webscraped and scanner dataframes into one table
    inliers_outliers = reduce(DataFrame.unionByName, list(dfs.values()))

    return inliers_outliers, inliers
