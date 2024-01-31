# import spark libraries
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, IntegerType, DateType

def preprocess_supplier_1(df, item, start_date, end_date):

    """
    Not item-specific preprocessing for supplier_1 data.

    Parameters
    ----------
    df : spark dataframe
        Input dataframe to preprocess. It has the columns needed for index
        calculations (unique identifier, date, price, retailer) and the rest
        of the columns are the product features.

    item : string
        The name of the item dataset (eg 'laptop', 'clothing', etc).

    start_date : string
        The month from which the time series should start. Any records/products
        that were web-scraped will be dropped for this run. The convention is
        that a month is represented from the first day of the month
        (e.g. January 2019 is 2019-01-01).

    Returns
    -------
    df : spark dataframe
        Has the following columns:
        product_id, retailer, date, price, quantity, product_name

    Notes
    -----
    This function applies the following steps:

    * Creates product_id column using remotekey, item and retailer.
    * Select price_inc_offer as the price column
    * Cast doubletype on price and keep only records with positive price.
    * Creates quantity column with missing values.
    * Renames 'collection_date_of_price' column to 'date'.
    * Casts double type on prices columns.
    * Filters the data to keep only products collected after a user-specified date.
    * Select columns needed for following stages
    * Dropna and duplicates with respect to ['product_id', 'retailer', 'date', 'price']
    * Replace date with first date of corresponding month

    """

    # PRODUCT ID

    # create unique product id
    df = df.withColumn(
        'product_id',
        F.concat(F.col('remotekey'), F.lit('_'+item+'_'), F.col('retailer'))
    )

    # PRICE

    # use price including offer instead of price
    df = df.drop('price')
    df = df.withColumnRenamed('price_inc_offer', 'price')

    # price, should be non-missing and positive
    df = df.withColumn('price', df['price'].cast(DoubleType()))
    df = df.filter(df['price']>0)

    # QUANTITY
    df = df.withColumn('quantity', F.lit(1))

    # DATE

    df = df.withColumnRenamed('collection_date_of_price', 'date')
    df = df.withColumn('date', df['date'].cast(DateType()))

    df = df.select(
        'product_id',
        'retailer',
        'date',
        'price',
        'quantity',
        'product_name' # for classification
    )

    # drop missing values and duplicates
    cols = ['product_id', 'retailer', 'date', 'price']
    df = df.dropna(subset=cols)
    df = df.dropDuplicates(subset=cols)

    # MONTH

    # transform all price collection dates to first date of the month
    df = df.withColumn('month', F.trunc('date', 'month'))
    df = df.drop('date')

    # keep records that have been collected in the period of interest
    df = df.filter(
        (df['month']>=F.lit(start_date))&
        (df['month']<=F.lit(end_date))
    )

    return df


def preprocess_supplier_2(df, start_date, end_date):
    """
    Preprocess supplier_2 data.

    Parameters
    ----------
    df : spark dataframe

    start_date : string
        Date (first day of the month) from which the time series begins.
        The period of interest is: start_date/month with most recent data

    Returns
    -------
    df : spark dataframe
        Has the following columns:
        product_id, retailer, date, price, quantity, retailer_category_id

    Notes
    -----
    The transformations that take place are the following:

    * Create retailer column
    * retailer_category_id is created by concatenating department_code,
    section_code and sub_section_code
    * product_id is created by concatenating consumer_unit_id,
    retailer_category_id and retailer
    * Price is created as sales/quantity
    * Keeps only records with positive prices
    * Number_Sold column is renamed to quantity
    * date column is created (first date of week sold)
    * Filters dates after user-specified start_date to keep only sales in
    period of interest
    * Select columns needed for next stages
    * Drops rows with missing value
    * Replace date with first date of corresponding month

    The date column:

    * The date that the product was sold is not given.
    * Instead we have the dates of the first date of the week that
    the product was sold in.
    * The last date of the week is found by adding 7 days to the first date
    of the week.
    * If the first and last dates of week fall into two different (consecutive)
    months, then we cannot "assign" these product sales to any of the two
    months and the correposnding record is dropped.
    * In the end, the date value assigned to product sales is the first date of
    the week it was sold.

    """

    # RETAILER

    df = df.withColumn('retailer', F.lit('retailer_2'))

    # RETAILER'S CLASSIFICATION

    # unique identifiers for the categories of the retailer's classification
    df = df.withColumn(
        'retailer_category_id',
        F.concat(
            F.col('department_code'),
            F.col('section_code'),
            F.col('sub_section_code'),
        )
    )

    # PRODUCT ID

    # create unique product id
    df = df.withColumn(
        'product_id',
        F.concat(
            F.col('consumer_unit_id'),
            F.lit('_'),
            F.col('retailer_category_id'),
            F.lit('_'),
            F.col('retailer'),
        )
    )

    # PRICE

    # price is expenditure divided by quantity
    df = df.withColumn(
        'price',
        df['sales_value_vat_excl_markdowns']/df['number_sold']
    )
    # price should be non-missing and positive
    df = df.filter(df['price']>0)

    # QUANTITY

    # this is the quantity to be used in the weighted index methods
    df = df.withColumnRenamed('number_sold', 'quantity')
    # price should be non-missing and positive
    df = df.filter(df['quantity']>0)

    # DATE

    # month of first date of week when product was sold
    df = df.withColumn('commencing_month', F.trunc('week_commencing_date', 'month'))

    # month of last date of week when product was sold
    df = df.withColumn('week_ending_date', F.date_add(df['week_commencing_date'], 7))
    df = df.withColumn('ending_month', F.trunc('week_ending_date', 'month'))

    # if week falls between two consecutive months
    # we cannot decide the month it was sold in so we drop the record
    df = df.withColumn(
        'date',
        F.when(df['commencing_month']==df['ending_month'], df['week_commencing_date'])
        .otherwise(None)
    )
    df = df.withColumn('date', df['date'].cast(DateType()))

    df = df.select(
        'product_id',
        'retailer',
        'date',
        'price',
        'quantity',
        'retailer_category_id',
    )

    # drop missing values
    df = df.dropna()

    # MONTH

    # transform all price collection dates to first date of the month
    df = df.withColumn('month', F.trunc('date', 'month'))
    df = df.drop('date')

    # keep records that have been collected in the period of interest
    # any missing dates from sales between months will also be dropped
    df = df.filter(
        (df['month']>=F.lit(start_date))&
        (df['month']<=F.lit(end_date))
    )

    return df


def main(dfs, config):

    """
    Main function for preprocessing of all input data from all data suppliers.
    Includes droping and renaming columns, casting right data types, deriving
    the price from sales and quantities and preparing the date column.

    Parameters
    ----------
    spark :
        spark session

    dfs : dictionary of spark dataframes
        Dataframes to preprocess.

    config : python file/module
        It has the start date of the period of iterest. Any products sold or
        webscraped before that date are droped from the data.

    Returns
    -------
    preprocessed : dictionary of spark dataframes
        They have been cleaned/preprocessed and continue to next stage.

    """

    # import start date and end date selected by user in config file
    start_date = config.params['preprocessing']['start_date']
    end_date = config.params['preprocessing']['end_date']

    # whether to drop retailers with zero weights
    drop_retailers = config.params['preprocessing']['drop_retailers']

    # holds the preprocessing functions for each contract/supplier and each
    # data source (web scraped, scanner and conventional data sources)
    PREPROCESS_DICT = {
        # web_scraped
        'supplier_1' : preprocess_supplier_1,
        # scanner
        'supplier_2' : preprocess_supplier_2,
    }

    # for each data source, supplier and item transform/preprocess each dataframe
    for data_source in dfs:
        if data_source == 'web_scraped':
            # dictionary with weights for all web scraped suppliers, items, retailers
            weights = config.params['staged_data']['web_scraped']
            for supplier in dfs[data_source]:
                for item in dfs[data_source][supplier]:
                    df = dfs[data_source][supplier][item]
                    df = PREPROCESS_DICT[supplier](df, item, start_date, end_date)
                    if drop_retailers:
                        # retailer weights for specific supplier and item
                        ws = weights[supplier][item]
                        # retailers to drop are the ones with zero weight
                        to_drop = [r for r in ws if ws[r]==0]
                        # filter out retailers with zero weight
                        if len(to_drop) > 0:
                            df = df.filter(~F.col('retailer').isin(to_drop))
                    df = df.withColumn('data_source', F.lit(data_source))
                    df = df.withColumn('supplier', F.lit(supplier))
                    dfs[data_source][supplier][item] = df
        elif data_source in ['scanner', 'conventional']:
            for supplier in dfs[data_source]:
                df = dfs[data_source][supplier]
                df = PREPROCESS_DICT[supplier](df, start_date, end_date)
                df = df.withColumn('data_source', F.lit(data_source))
                df = df.withColumn('supplier', F.lit(supplier))
                dfs[data_source][supplier] = df

    return dfs
