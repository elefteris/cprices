# import spark libraries
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import DataFrame
from pyspark.ml.feature import RegexTokenizer

# import python libraries
from functools import reduce
from importlib import reload
import os

from cprices.cprices.steps import utils
reload(utils)

def classify_name_using_keywords(df, keywords):

    """
    Adds classification flag/column to the input dataframe.

    This column shows whether each row/product belongs to the item of interest
    or not. The decision is based on whether any user-specified keywords exist
    in the product name. If yes, the product does not belong to the item.

    Parameters
    ----------
    df : spark dataframe
        The input table should have a 'product_name' column. Each product
        will be classified depending on whether its name includes any keywords.

    keywords : list of strings
        The keywords for each item are specified by the user in the
        configuration file and if any of those keywords exists in the product
        name, the product is flagged as not belonging to the item of interest.

    Returns
    -------
    df : spark dataframe
        This table is exactly the same as the input table with an additional
        column named 'classified'. This is a flag column that shows whether
        each product belongs to the item of interest (value=0) or not (value=1)

    Notes
    -----
    This keywords-based method works for binary classification when the input
    dataframe is a one-item dataset (such as laptops).
    It wouldn't work for a multi-item dataset (such as clothing).

    The item of interest is defined by the COICOP classification with some
    additional specifications (requirements for the product features) created
    by ONS. For example, when we want to create a price index for laptops, we
    don't collect prices for refurbished laptops. In that case, the word
    'refurbished' should not appear in the product name.

    """

    # create temporary copy  of the product_name column for classification
    df = df.withColumn('name', df['product_name'])

    # if name is missing, replace with 1st keyword to be flagged as not_item
    df = df.fillna(keywords[0], subset=['name'])

    # tokenize the name
    regextokenizer = RegexTokenizer(
        inputCol='name',
        outputCol='tokenized',
        pattern="\\W"
    )
    df = regextokenizer.transform(df)

    # number of tokes column. if n_tokens=0 then will be flagged as not_item
    df = df.withColumn('n_tokens', F.size(df['tokenized']))

    # for each keyword create column that shows whether keyword exists in name
    for word in keywords:

        # first initialize column with the word
        df = df.withColumn(word, F.lit(word))

        sql_exp = f"array_contains(tokenized,{word})"
        df = df.withColumn(word, F.when(F.expr(sql_exp), 1).otherwise(0))

    # sum the newly created columns
    df = df.withColumn("sum", sum([df[col] for col in keywords]))

    # if any keywords are present then product does not belong to item
    df = df.withColumn(
        'classified',
        F.when((df["sum"] > 0)|(df["n_tokens"]==0), 1)
        .otherwise(0)
    )

    # drop all intermediate columns
    cols_to_drop = keywords+['sum', 'n_tokens', 'tokenized', 'name']
    df = df.drop(*cols_to_drop)

    return df


def classify_webscraped(dfs, mappers_dir, active, cols_for_index):

    """
    Classifies all one-item web-scraped datasets from all suppliers.

    Parameters
    ----------
    dfs : dictionary of spark dataframes
        Dataframes from each webscraped supplier and item to be classified.

    mappers_dir : string
        The HDFS path to the directory that has the mappers for classification.

    active : boolean
        Whether classification should be used on webscraped datasets or not.

    cols_for_index : list of strings
        Names of columns needed for index calculations.

    Returns
    -------
    dfs : dictionary of spark dataframes
        The input dataframes but with an extra column "classified" each showing
        whether product belongs to item of interest (value=0) or not (value=1).

    ws_items : spark dataframe
        All the classified web scraped datasets from all suppliers where all
        products have been filtered to belong to the items of interest are
        appended into one table.

    """

    data_source = 'web_scraped'

    # will hold all the classified item datasets to be appended into one table
    ws_items = []

    for supplier in dfs[data_source]:
        # create mapper pandas dataframe
        mapper_path = os.path.join(mappers_dir, supplier+'.csv')
        mapper = utils.hdfs_read_csv(mapper_path)
        for item in dfs[data_source][supplier]:
            df = dfs[data_source][supplier][item]
            df = df.withColumn('item', F.lit(item))
            if active:
                keywords = [k for k in mapper[item] if k!='']
                df = classify_name_using_keywords(df, keywords)
                df_item = df.filter(df['classified']==0).select(cols_for_index)
            else:
                # if classification for webscraped data is not active
                # all products are assigned to the item
                df = df.withColumn('classified', F.lit(0))
                df_item = df.select(cols_for_index)
            ws_items.append(df_item)
            dfs[data_source][supplier][item] = df

    # union all clean webscraped dataframes into one table
    ws_items = reduce(DataFrame.unionByName, ws_items)

    return dfs, ws_items


def classify_scanner(spark, dfs, mappers_dir, cols_for_index):

    """
    Classifies all scanner datasets from all suppliers.

    Parameters
    ----------
    spark :
        spark session

    dfs : dictionary of spark dataframes
        Dataframes from each scanner supplier to be classified.

    mappers_dir : string
        The HDFS path to the directory that has the mappers for classification.

    cols_for_index : list of strings
        Names of columns needed for index calculations.

    Returns
    -------
    dfs : dictionary of spark dataframes
        The input dictionary of dataframes but each dataframe after
        classification has an extra column showing the item each product
        belongs to. Products that have not been mapped will have a
        missing value in that column.

    sd_items : dictionary of spark dataframes
        All the classified datasets from all suppliers for a specific source
        are appended into one table and then we create a key-value pair
        in the items dictionary.

    """

    data_source = 'scanner'

    # will hold all the classified scanner datasets to be appended into one table
    sd_items = []

    for supplier in dfs[data_source]:
        df = dfs[data_source][supplier]

        # prepare mapper dataframe
        mapper_path = os.path.join(mappers_dir, supplier+'.csv')
        mapper = spark.read.csv(mapper_path, header=True, inferSchema=False)
        mapper = (
            mapper
            .coalesce(1)
            .select('retailer_category_id', 'item')
            .dropna()
            .dropDuplicates()
        )

        # add item column
        df = df.join(mapper, 'retailer_category_id', 'left')

        # drop any unmapped retailer categories and select columns for index
        df_item = df.dropna(subset=['item']).select(cols_for_index)
        sd_items.append(df_item)

    # union all clean scanner dataframes into one table
    sd_items = reduce(DataFrame.unionByName, sd_items)

    return dfs, sd_items


def nested_dict_values(d):
    """Returns a list of all the nested values in a nested dictionary."""
    for v in d.values():
        if isinstance(v, dict):
            yield from nested_dict_values(v)
        else:
            yield v


def prepare_table_for_analysis(spark, dfs, groupby_cols):
    """
    It returns the table that will be used by the classification stage of the
    analysis pipeline. If web scraped are not included in the scenario, this
    table will have the right columns but will be empty just so that
    classification stage of the analysis pipeline can run.
    """

    cols = groupby_cols + ['month', 'classified']

    if 'web_scraped' in dfs:
        # create flat list of dataframes/values from nested dfs dictionary
        dfs = nested_dict_values(dfs['web_scraped'])

        # select columns needed for classification analysis
        # so that all dataframes can be unioned
        dfs = [df.select(cols) for df in dfs]

        # union all classified dataframes into one table
        classified = reduce(DataFrame.unionByName, dfs)
    else:
        structfields = [
            T.StructField('month', T.DateType(), True),
            T.StructField('classified', T.IntegerType(), False)
        ]
        for c in groupby_cols:
            structfields.append(T.StructField(c, T.StringType(), True))

        schema = T.StructType(structfields)
        classified = spark.createDataFrame([], schema)

    return classified


def main(spark, dfs, config, dev_config):

    """
    Main function for classification of all products from all data sources,
    suppliers, retailers and items.

    Parameters
    ----------
    spark :
        spark session

    dfs : dictionary of spark dataframes
        Dataframes for each data source, supplier and item to be classified.

    config : python file/module
        Includes the "active" parameter showing whether classification will
        be used on web scraped data or if all products will be accepted as
        belonging to the items of interest.

    dev_config : python file/module
        Includes the HDFS path to the classification mappers as well as the
        groupby columns.

    Returns
    -------
    dfs : dictionary of spark dataframes
        These are the input datafames, each with a classification column
        showing whether each product belongs to the item of interest or not.
        They will be stored in the processed data folder in HDFS.
        Includes only webscraped data.

    items : dictionary of nested spark dataframes
        Each source has a key and the value is the one table where all
        the datasets from the corresponding data source have been appended.

    Notes
    -----
    The classification process creates an 'item' column in each dataset but
    it is different for each data source:

    * for web scraped data, there is a mapper file, a table where each column
    name corresponds to a web scraped item and each column contains the
    classification keywords for the corresponding item. If the product name
    contains any of the keywords then the product does not belong to the item
    of interest. By item we mean ONS item under the COICOP 5 level.

    * for scanner data, there is a mapper file that contains at least two
    columns: one has the retailer's classification/categories of products
    and the other has the COICOP/ONS classification category it is mapped to.
    By joining the retailers data with the mapper on the retailer's
    classification, the COICOP/ONS item column is added.

    * classification is optional for webscraped but mandatory for scanner

    * we keep webscraped and scanner separately in a dictionary called items
    because they will be treated differently in following modules.

    * classification of webscraped datasets based on keywords works only for
    one-item datasets. If there are multi-item datasets in the staged_data
    dictionary, their will not exist in the classification mapper for webscraped
    data so an error will be raised when running the classification module.

    * The prepare_table_for_analysis function returns the table that will be
    used by the classification stage of the analysis pipeline. If web scraped
    are not included in the scenario, this table will have the right columns
    but will be empty just so that classification stage of the analysis
    pipeline can run.

    """

    # mappers HDFS directory for classification
    # each supplier has its own mapper file
    mappers_dir = os.path.join(dev_config.mappers_dir, 'classification')

    groupby_cols = dev_config.groupby_cols
    cols_for_index = groupby_cols + ['month', 'product_id', 'price', 'quantity']

    items = {}

    if 'web_scraped' in dfs:
        active = config.params['classification']['web_scraped']['active']
        dfs, items['web_scraped'] = classify_webscraped(
            dfs, mappers_dir, active, cols_for_index)

    if 'scanner' in dfs:
        dfs, items['scanner'] = classify_scanner(
            spark, dfs, mappers_dir, cols_for_index)

    classified = prepare_table_for_analysis(spark, dfs, groupby_cols)

    return classified, items
