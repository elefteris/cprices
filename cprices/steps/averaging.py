# import spark libraries
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SQLContext

# import python libraries
from functools import reduce
from importlib import reload


def left_join(spark, df1, df2, join_cols):

    """
    Left joins two tables on multiple columns.

    Parameters
    ----------
    spark :
        spark session

    df1 : spark dataframe
        The left table in the left join.

    df2 : spark dataframe
        The right table in the left join.

    join_cols : list of strings
        Colmn names to join on.

    Returns
    -------
    df : spark dataframe
        Left join of df2 to df1.

    Notes
    -----
    Due to spark jira issue 14948, when a dataframe df1 is derived from df
    and then we attempt to join df with df1, we get an error. We need to rename
    the columns and then include explicitly the columns to join on as:
    df.col==df1.col_renamed

    """

    sc = spark.sparkContext
    sqlContext = SQLContext(sc)

    cols_to_drop = []

    for c in join_cols:
        df2 = df2.withColumnRenamed(c, c+'_2')
        cols_to_drop.append(c+'_2')

    df1.registerTempTable("df1")
    df2.registerTempTable("df2")

    c = join_cols[0]
    query = f"SELECT * FROM df1 LEFT JOIN df2 ON df1.{c} = df2.{c+'_2'}"

    for c in join_cols[1:]:
        query += f" AND df1.{c} = df2.{c+'_2'}"

    df = sqlContext.sql(query)

    return df.drop(*cols_to_drop)


def weighted_arithmetic_average(spark, df, groupby_cols):
    """
    Calculates the weighted arithmetic average of price and the sum of product
    quantities in each month. The weights come from the product quantities in
    each month.

    Parameters
    ----------
    spark :
        spark session

    df : spark dataframe
        Has the products with price quotes to be averaged arithmetically and
        and quantities to be summed on a monthly basis. Includes also the
        groupby_cols to form groups.

    groupby_cols : list of strings
        Includes data_source, supplier, retailer, item, month, product_id

    Returns
    -------
    df : spark dataframe
        Has groupby_cols and weighted arithmetic average of price and sum of
        quantities for each product for each month.
    """

    # add column with sum of quantities of each product within each group
    cols = groupby_cols + ['quantity']
    sumq = df.select(cols).groupBy(groupby_cols).sum()
    df = left_join(spark, df1=df, df2=sumq, join_cols=groupby_cols)

    # within each group each price is weighted based on the fraction of
    # product sales in that price in that group
    df = df.withColumn('weight', df['quantity']/df['sum(quantity)'])
    df = df.withColumn('weighted_price', df['price']*df['weight'])

    # by summing the weighted price we get the weighted arithmetic average
    # of the price of each product within each group
    # by summing the quantities of sales of each product within each group
    # we get the total quantity sold for the corresponding product in the group
    df = df.groupBy(groupby_cols).sum()

    # rename columns to have the right names based on columns needed for
    # index calculations
    df = df.withColumnRenamed('sum(weighted_price)', 'price')
    df = df.withColumnRenamed('sum(quantity)', 'quantity')

    return df.drop('sum(price)', 'sum(sum(quantity))', 'sum(weight)')


def weighted_geometric_average(spark, df, groupby_cols):
    """
    Calculates the weighted geometric average of price and the sum of product
    quantities in each month. The weights come from the product quantities in
    each month.

    Parameters
    ----------
    spark :
        spark session

    df : spark dataframe
        Has the products with price quotes to be averaged geometrically and
        and quantities to be summed on a monthly basis. Includes also the
        groupby_cols to form groups.

    groupby_cols : list of strings
        Includes data_source, supplier, retailer, item, month, product_id

    Returns
    -------
    df : spark dataframe
        Has groupby_cols and weighted geometric average of price and sum of
        quantities for each product for each month.
    """

    df = df.withColumn('log(price)', F.log(df['price']))

    # add column with sum of quantities of each product within each group
    cols = groupby_cols + ['quantity']
    sumq = df.select(cols).groupBy(groupby_cols).sum()

    df = left_join(spark, df1=df, df2=sumq, join_cols=groupby_cols)

    # within each group each log price is weighted based on the fraction of
    # product sales in that price in that group
    df = df.withColumn('weight', df['quantity']/df['sum(quantity)'])
    df = df.withColumn('weighted_log(price)', df['log(price)']*df['weight'])

    # by summing the weighted price we get the weighted arithmetic average
    # of the price of each product within each group
    # by summing the quantities of sales of each product within each group
    # we get the total quantity sold for the corresponding product in the group
    df = df.groupBy(groupby_cols).sum()

    df = df.withColumn('price', F.exp(F.col('sum(weighted_log(price))')))
    df = df.withColumnRenamed('sum(quantity)', 'quantity')

    return df.drop(
        'sum(price)',
        'sum(weighted_log(price))',
        'sum(log(price))',
        'sum(sum(quantity))',
        'sum(weight)'
    )

def unweighted_arithmetic_average(spark, df, groupby_cols):
    """
    Calculates the arithmetic average of price and sets quantity for each
    product to 1 for each month.

    Parameters
    ----------
    spark :
        spark session

    df : spark dataframe
        Has the products with price quotes to be averaged arithmetically.
        Includes also the groupby_cols to form groups.

    groupby_cols : list of strings
        Includes data_source, supplier, retailer, item, month, product_id

    Returns
    -------
    df : spark dataframe
        Has groupby_cols and arithmetic average of price and quantity=1
        for each product for each month.
    """
    return (
        df
        .drop('quantity')
        .groupBy(groupby_cols).mean()
        .withColumnRenamed('avg(price)', 'price')
        .withColumn('quantity', F.lit(1))
    )

def unweighted_geometric_average(spark, df, groupby_cols):
    """
    Calculates the geometric average of price and sets quantity for each
    product to 1 for each month.

    Parameters
    ----------
    spark :
        spark session

    df : spark dataframe
        Has the products with price quotes to be averaged geometrically.
        Includes also the groupby_cols to form groups.

    groupby_cols : list of strings
        Includes data_source, supplier, retailer, item, month, product_id

    Returns
    -------
    df : spark dataframe
        Has groupby_cols and geometric average of price and quantity=1
        for each product for each month.
    """
    return (
        df
        .drop('quantity')
        .withColumn('log(price)', F.log(df['price']))
        .groupBy(groupby_cols)
        .agg({"log(price)":'mean'})
        .withColumn('price', F.exp(F.col('avg(log(price))')))
        .drop('avg(log(price))')
        .withColumn('quantity', F.lit(1))
    )

def main(spark, dfs, config, dev_config):

    """
    Main function for averaging webscraped and scanner data.

    Parameters
    ----------
    dfs : dictionary of spark dataframes
        Input dataframes to average price and sum product quantities.
        Each key/dataframe corresponds to a different data source
        (web-scraped, scanner, conventional )

    config : python file/module
        It includes the averaging method (weighted/unweighted,
        geometric/arithmetic) for each data source.

    dev_config : python file/module
        Includes the groupby columns.

    Returns
    -------
    averaged : spark dataframe
        Includes the monthly averaged price and the quantity column.
    """

    # columns to form groups
    groupby_cols = dev_config.groupby_cols + ['month', 'product_id']

    METHODS_DICT = {
        'unweighted_arithmetic' : unweighted_arithmetic_average,
        'unweighted_geometric'  : unweighted_geometric_average,
        'weighted_arithmetic'   : weighted_arithmetic_average,
        'weighted_geometric'    : weighted_geometric_average,
    }

    for data_source in dfs:
        method = config.params['averaging'][data_source]['method']
        dfs[data_source] = METHODS_DICT[method](
            spark, dfs[data_source], groupby_cols)

    # union webscraped and scanner averaged dataframes into one table
    averaged = reduce(DataFrame.unionByName, list(dfs.values()))

    return averaged
