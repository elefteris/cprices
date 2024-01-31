# import pyspark libraries
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# import python libraries
import pandas as pd
from importlib import reload
from time import time

# import modules
from cprices.cprices.steps import (
    preprocessing,
    classification,
    outlier_detection,
    averaging,
    grouping,
    imputation,
    filtering,
    indices,
    aggregation
)
reload(preprocessing)
reload(classification)
reload(outlier_detection)
reload(averaging)
reload(grouping)
reload(imputation)
reload(filtering)
reload(indices)
reload(aggregation)

def main(spark, staged_data, config, dev_config):

    """
    The prices core pipeline with all stages to run for a specific scenario of
    configuration parameters. It also adds a 'scenario' column to every output
    table.

    Parameters
    ----------
    spark :
        spark session

    staged_data : nested dictionary of spark dataframes
        Those dataframes correspond to the various data sources, suppliers,
        items, retailers that will go through the core pipeline.

    config : python file/module
        It contains all the configuration parameters for all the stages of the
        pipeline. It corresponds to a specific combination of config parameters
        (scenario). Multiple config python files should be created to run
        different scenarios.

    dev_config : python file/module
        It contains CDSW and HDFS paths needed to read and write data and
        modules. It is controlled by the developers only.

    Returns
    -------
    dfs : dictionary of spark dataframes
        The intermediate and final output dataframes from the core pipeline to
        store in HDFS to feed into the analysis pipeline.

    Notes
    -----
    The configuration dataset is a two-column table where the first column
    shows the stage of the core pipeline and the second column shows (as a
    dictionary) all the config parameters for the corresponding stage. This
    can be used as a reference for the user in case they want to check the
    configuration of this run.

    """

    times = {}

    # CONFIGURATION

    # create spark dataframe with configuration parameters for the scenario
    configuration = (
        pd
        .DataFrame
        .from_dict(config.params, orient='index')
        .reset_index()
        .astype(str)
    )
    configuration = spark.createDataFrame(configuration)

    # PREPROCESSING

    print('\npreprocessing...')
    start = time()

    preprocessed = preprocessing.main(staged_data, config)

    dt = time()-start
    times['preprocessing'] = dt
    print(dt)

    # CLASSIFICATION

    print('\nclassification...')
    start = time()

    classified, items = classification.main(
        spark,
        preprocessed,
        config,
        dev_config
    )
    for data_source in items:
        items[data_source].cache().count()

    dt = time()-start
    times['classification'] = dt
    print(dt)

    # OUTLIER DETECTION

    print('\noutlier detection...')
    start = time()

    inliers_outliers, inliers = outlier_detection.main(
        spark,
        items,
        config,
        dev_config
    )
    for data_source in inliers:
        inliers[data_source].cache().count()

    dt = time()-start
    times['outlier detection'] = dt
    print(dt)

    # AVERAGING

    print('\naveraging...')
    start = time()

    averaged = averaging.main(spark, inliers, config, dev_config)
    averaged.cache().count()

    dt = time()-start
    times['averaging'] = dt
    print(dt)

    # GROUPING

    print('\ngrouping...')
    start = time()

    grouped = grouping.main(spark, averaged, config, dev_config)
    grouped.cache().count()

    dt = time()-start
    times['grouping'] = dt
    print(dt)

    # IMPUTATION

    print('\nimputation...')
    start = time()

    imputed_flagged, imputed = imputation.main(grouped, config, dev_config)
    imputed.cache().count()

    dt = time()-start
    times['imputation'] = dt
    print(dt)

    # FILTERING

    print('\nfiltering...')
    start = time()

    expenditure, filtered = filtering.main(spark, imputed, config, dev_config)
    filtered.cache().count()

    dt = time()-start
    times['filtering'] = dt
    print(dt)

    # GET LOW LEVEL INDICES

    print('\nlow level indices...')
    start = time()

    low_level_indices = indices.main(
        spark, filtered, config, dev_config)

    dt = time()-start
    times['low level indices'] = dt
    print(dt)

    # GET ITEM INDICES

    print('\nitem indices...')
    start = time()

    item_indices = aggregation.main(low_level_indices, config, dev_config)

    dt = time()-start
    times['item indices'] = dt
    print(dt)

    # INDEX AGGREGATION

    # ...

    # CHAIN LINKING

    # ...

    # PREPARE OUTPUT DICTIONARY

    dfs = {
        'configuration'     : configuration,
        'classified'        : classified,
        'inliers_outliers'  : inliers_outliers,
        'imputed_flagged'   : imputed_flagged,
        'expenditure'       : expenditure,
        'filtered'          : filtered,
        'low_level_indices' : spark.createDataFrame(low_level_indices),
        'item_indices'      : spark.createDataFrame(item_indices)
    }

    return dfs, times
