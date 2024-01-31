# import pyspark libraries
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# import python libraries
from time import time
import pandas as pd
from importlib import reload, import_module
from functools import reduce
import matplotlib.pyplot as plt
import os

# import modules
from cprices.cprices.steps import validate_config, iofuns, pipeline, analysis
reload(validate_config)
reload(iofuns)
reload(pipeline)
reload(analysis)

# import configuration file
from cprices.config import dev_config
reload(dev_config)

from cprices.cprices.steps import utils
reload(utils)

def union_dfs_from_all_scenarios(dfs):
    """
    Unions the corresponding dataframes from all scenarios so in the output
    dictionary, each stage has one dataframe. Before doing that, a scenario
    column is added to each dataframe to distinguish between scenarios.

    Parameters
    ----------
    dfs : nested dictionary of spark dataframes
        Every key in the dfs dictionary holds the dataframes for the stages of
        the scenario run.

    scenario : string
        The name of the scenario, i.e. scenario_1, scenario_2, etc

    Returns
    -------
    dfs : dictionary of spark dataframes
        Each key holds the unioned dataframe across all scenarios for a
        particular stage.
    """

    # ADD SCENARIO COLUMN TO ALL DATAFRAMES

    for scenario in dfs:
        # scenarios have names: scenario_x, extract number
        scenario_no = scenario.split('_')[1]
        for df in dfs[scenario]:
            dfs[scenario][df] = dfs[scenario][df].withColumn(
                'scenario', F.lit(scenario_no))

    # UNION DATAFRAMES (IF THERE ARE MORE THAN 1 SCENARIOS)

    # name of first scenario
    scenario = list(dfs.keys())[0]

    if len(dfs) > 1:
        # names of dataframes - they are the same for each scenario
        # they can be collected from any scenario in dfs
        names = dfs[scenario].keys()

        dfs_unioned = {}
        for name in names:
            dfs_to_union = list(utils.find(name, dfs))
            dfs_unioned[name] = reduce(DataFrame.unionByName, dfs_to_union)
            dfs_unioned[name].cache().count()

        dfs = dfs_unioned
    else:
        dfs = dfs[scenario]

    return dfs


def run(spark, dev_config):
    """
    The ETL process to run the consumer prices data transformation pipeline
    (cpdt_core) pipeline for multiple scenarios and store the output in HDFS
    for further analysis.

    Parameters
    ----------
    spark :
        spark session

    dev_config : python file/module
        Includes the HDFS paths to the staged data to import and the processed
        data folder to store the outputs.

    Returns
    -------
    dfs : dictionary of spark dataframes

    times : nested dictionary
        Includes the run times of all steps and scenarios in the run.

    run_id : string
        Consists of the date, time and username of the run
        YYYYMMDD_hhmmss_USERNAME. That's the name of the processed data
        folder to store all the output datasets in HDFS.

    """

    t0 = time()
    times = {}
    config_dir = 'cprices.config'

    # import list of names of selected scenarios
    scenarios = import_module(f'{config_dir}.scenarios')
    reload(scenarios)
    selected_scenarios = scenarios.selected

    # validate configuration parameters as inserted by user
    validate_config.check_params(config_dir, selected_scenarios)

    dfs = {}
    for scenario in selected_scenarios:

        print('\n', scenario.upper(), '\n', len(scenario)*'-')
        times[scenario] = {}

        # import and reload config file for scenario
        config = import_module(f'{config_dir}.{scenario}')
        reload(config)

        # EXTRACT

        print('\nextract...')
        start = time()

        staged_data = iofuns.extract_data(spark, config, dev_config)

        dt = time()-start
        times[scenario]['extract'] = dt
        print(dt)

        # TRANSFORM
        # run core pipeline for scenario
        dfs[scenario], scenario_times = pipeline.main(
            spark, staged_data, config, dev_config)
        times[scenario].update(scenario_times)

    print('\nUNION SCENARIOS\n', 5*'-')
    start = time()
    dfs = union_dfs_from_all_scenarios(dfs)
    dt = time()-start
    times[scenario]['union'] = dt
    print(dt)

    # ANALYSIS

    print('\nANALYSIS\n', 8*'-')
    start = time()
    dfs['analysis'] = analysis.main(spark, dfs, dev_config)
    dt = time()-start
    times[scenario]['analysis'] = dt
    print(dt)

    # LOAD

    print('\nLOAD\n', 4*'-')
    start = time()

    run_id = iofuns.load_data(dfs, dev_config)

    dt = time()-start
    times[scenario]['load'] = dt
    print(dt)

    # PERFORMANCE

    performance = pd.DataFrame(times).reindex(list(times[scenario].keys()))
    print(performance)

    plt.figure()
    (
        performance
        .reindex(list(times[scenario].keys())[::-1])
        .plot(kind='barh', stacked=True, title='Run time [seconds]')
    )
    plt.show()

    print('\nrun_time:', round((time()-t0)/60, 2), 'minutes')
    print('\nrun_id:', run_id)

    return dfs, times


if __name__ == "__main__":

    # start spark session
    spark = (
        SparkSession.builder.appName('cprices')
        .config('spark.executor.memory', '10g')
        .config('spark.yarn.executor.memoryOverhead', '1g')
        .config('spark.executor.cores', 6)
        .config('spark.dynamicAllocation.maxExecutors', 5)
        .config('spark.dynamicAllocation.enabled', 'false')
        .config('spark.shuffle.service.enabled', 'true')
        .config('spark.driver.maxResultSize', '5g')
        .config('spark.sql.execution.arrow.enabled', 'false')
        .enableHiveSupport()
        .getOrCreate()
    )

    dfs, times = run(spark, dev_config)
