# import spark libraries
from pyspark.sql import Window
from pyspark.sql import functions as F

# import python libraries
from importlib import reload

# import configuration files
from cprices.cprices.steps import utils
reload(utils)

def forward_filling(df, ffill_limit, groupby_cols):

    """
    Imputes prices and quantities with fill forward imputation when a product
    that appears in a month is missing in the following months.

    Parameters
    ----------
    df : spark dataframe
        The input dataframe with columns to impute:
        data_source, supplier, retailer, item, quantity, price

        whether the module is active or not and the number of months to
        impute with fill forward imputation (e.g. 3 months).

    ffill_limit : positive integer
        The number of months to impute with fill forward imputation.

    cols_to_impute : list of strings
        The columns to impute with forward fill imputation.

    Returns
    -------
    df : spark dataframe
        It is the same as the input dataframe with two major additions:

        * rows have been added for the months where products were missing.
        If a product was missing in January, two more rows will be added for
        February and March if we fill forward for 2 months.

        * a flag column named 'imputed' is added to show which rows were added
        from imputation (value=1) and which already existed (value=0).

    Notes
    -----
    Imputation is based on missing prices.

    The input dataframe is in long format which means that the columns
    data_source, supplier, retailer, item give all the possible combinations or
    groups while the price column has the price of products.

    Therefore, when a product that exists in a month, disappears in the next,
    there is no row in the data for this particular combination of product_id
    and month.

    By pivoting the data, a table is created where the rows are
    all the unique product_ids and each column has the prices for the
    corresponding months. The missing values for prices that are created in the
    pivoted table correspond to products that exists in some months but are
    missing in others.

    Melting this table will create the rows with all the possible combinations
    of product_ids and months with missing prices. That is how the missing
    prices will be inserted in the original input table.

    This table can now be imputed for missing prices.

    """

    cols_to_impute = groupby_cols + ['product_id', 'quantity', 'price']
    cols_to_concat = groupby_cols + ['product_id']

    # create unique product id in the unlikely case where the same
    # product_id exists among different data sources or suppliers
    df = df.withColumn('id', F.concat(*cols_to_concat))

    # create products (rows) by all months (columns) dataframe
    ids_months = df.groupby(df['id']).pivot('month').sum('price')

    # melt the columns/months into one column "month"
    ids_months = utils.melt_df(
        ids_months,
        id_vars = ['id'],
        value_vars = [c for c in ids_months.columns if c!='id'],
        var_name = 'month',
        value_name = 'price'
    )

    # by joining the table we get the table to be imputed
    df = ids_months.join(df.drop('price'), ['id', 'month'], 'left')

    # this column will be a flag: 1 for imputed rows and 0 for non-missing
    df = df.withColumn('imputed', df['price'])

    # window (by product) for fill forward imputation
    window = (
        Window
        .partitionBy('id')
        .orderBy('month')
        .rowsBetween(-ffill_limit, 0) # -ffill_limit = -sys.maxsize
    )

    # impute columns of interest sequentially
    for column in cols_to_impute:
        imputed_column = F.last(df[column], ignorenulls=True).over(window)
        df = df.withColumn(column, imputed_column)

    df = df.where(F.col('price').isNotNull()).drop('id')

    # the remaining rows that still have a missing value for price
    # (those that have not been imputed) are dropped
    df = df.withColumn(
        'imputed',
        F.when(df['imputed'].isNotNull(),0).otherwise(1)
    )

    return df


def main(df, config, dev_config):

    """
    Imputes prices and quantities with fill forward imputation when a product
    that appears in a month is missing in the following months.

    Parameters
    ----------
    df : spark dataframe
        The input dataframe with columns that have missing values to be imputed.
        data_source, supplier, retailer, item, quantity, price

    config : python module/file
        Includes the active parameter that shows whether imputation will be
        used or not and the ffill_limit that shows the number of months to
        fill forward (e.g. 3 months).

    dev_config : python file/module
        Includes the groupby columns.

    Returns
    -------
    imputed_flagged : spark dataframe
        Imputed dataframe. An extra column/flag named 'imputed' shows whether
        a row already existed (0) or was added (1). This table can be used for
        analysis to find the % of rows added due to missing products.

    imputed : spark dataframe
        The same as imputed_flagged but the 'imputed' column is dropped.
        This table will continue to the next stage.

    Notes
    -----
    If module is not active, no rows are added and all existing rows are
    flagged with 0 in the 'imputed' column which means that they are not
    rows added from imputation (they already existed in the data).

    """

    if not config.params['imputation']['active']:
        imputed_flagged = df.withColumn('imputed', F.lit(0))
    else:
        ffill_limit = config.params['imputation']['ffill_limit']
        groupby_cols = dev_config.groupby_cols
        imputed_flagged = forward_filling(df, ffill_limit, groupby_cols)

    imputed = imputed_flagged.drop('imputed')

    return imputed_flagged, imputed
