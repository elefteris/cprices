#@staticmethod
#  def cumprod(df, pb_col, ob_col, column, output_col):
#
#    """
#    Cumulative product of column.
#
#    Parameters
#    ----------
#    df : spark dataframe
#
#    pb_col : string
#        The column to partitionBy
#
#    ob_col : string
#        The column to orderBy
#
#    column : string
#        The column whose cumulative product will be calculated.
#
#
#    Returns
#    -------
#    df : spark dataframe
#        The original dataframe with an extra column added which is the
#        cumulative product of the column of interest named.
#
#    """
#
#    df = df.withColumn('log', F.log(df[column]))
#    w = Window.partitionBy(pb_col).orderBy(ob_col).rowsBetween(-sys.maxsize, 0)
#    df = df.withColumn(output_col, F.sum(df['log']).over(w))
#    df = df.withColumn(output_col, F.exp(df[output_col]))
#
#    df = df.drop('log')
#
#    return df
#
#
#  def chain_linking(self, spark, I):
#
#    """
#    Chain linking function.
#    It creates the link factors for all nodes and periods and
#    adds the index value linked column.
#
#    Parameters
#    ----------
#    df : spark dataframe
#        Output from aggregation with raw and old weights index values to be
#        used to create linking factors and linkedin index value. Has the
#        columns:
#
#        * period
#        * basis
#        * indicator
#        * cpa21
#        * level
#        * index_value_raw
#        * index_value_oldwgts
#
#    linking_factors_seed : spark dataframe
#        Gives the seed linking factor for the very first period for each index
#        node. Has the columns:
#
#        * period
#        * basis
#        * indicator
#        * cpa21
#        * linking factor
#
#    Returns
#    -------
#    df : spark dataframe
#        Same as input df plus linking factors and linked index values.
#        Has the columns:
#
#        * period
#        * basis
#        * indicator
#        * cpa21
#        * level
#        * index_value_raw
#        * index_value_oldwgts
#        * linking_factor
#        * index_value_linked
#
#    """
#
#    print('-'*50)
#    print('Commencing chain linking: \n')
#
#    # EXTRACT
#    df = spark.sql('SELECT * FROM ' + I.config_dev["db"]+'.'+I.config_dev["index_values_agg_hive"])
#
#    # find first period in data
#    first_period = df.groupBy().agg(F.min('period')).collect()[0][0]
#
#    # ------------------
#    # USE WHEN WE HAVE LINKING FACTOR SEEDS IN HIVE FROM ANDREW/TONY
#
#    # prepare linking factors seed dataframe
#    #    linking_factors_seed = (
#    #        linking_factors_seed
#    #        .drop('level', 'def')
#    #        .withColumnRenamed('linking_factor', 'lf0')
#    #        .withColumn('period', F.lit(first_period))
#    #    )
#
#    # USE WHEN WE DON'T HAVE SEEDS IN HIVE
#    linking_factors_seed = (
#        df
#        .select('basis', 'indicator', 'cpa21')
#        .dropDuplicates()
#        .withColumn('lf0', F.lit(1))
#        .withColumn('period', F.lit(first_period))
#    )
#    # ------------------
#
#    # add linking factors seed to data
#    # lf0 will have the seed for the first period
#    # and null for the rest of the periods
#    join_cols = ['indicator', 'basis', 'cpa21', 'period']
#    df = df.join(linking_factors_seed, join_cols, 'left')
#
#    # the ratio index_value_oldwgts/index_value_raw should be null
#    # if index_value_oldwgts and/or index_value_raw are null or zero
#    df = (
#        df
#        .withColumn('raw_null', F.when(df['raw']==0, None).otherwise(df['raw']))
#        .withColumn('oldwgts_null', F.when(df['oldwgts']==0, None).otherwise(df['oldwgts']))
#    )
#
#    # get column of ratio index_value_oldwgts/index_value_raw
#    # this way we "initialize" the linking factor as "lf1"
#    df = df.withColumn('lf1', df['oldwgts_null']/df['raw_null'])
#
#    # the linking factor for the first period should have the seed value lf0
#    df = df.withColumn(
#        'lf1',
#        F.when(df['lf0'].isNotNull(), df['lf0'])
#        .otherwise(df['lf1'])
#    )
#
#    # copy column lf1 into lf2 to keep lf1 for reference/later use
#    df = df.withColumn('lf2', df['lf1'])
#
#    # fill missing values with 1 to be able to take the cumulative product
#    # if we leave missing values, the cumulative product will become null
#    # after the first missing value it finds.
#    df = df.fillna(1, subset=['lf2'])
#
#    df = Modules.cumprod(
#        df,
#        pb_col=['indicator', 'basis', 'cpa21'],
#        ob_col='period',
#        column='lf2',
#        output_col='lf3'
#    )
#
#    # where oldwgt/raw is missing and have imputed with 1, the cumulative
#    # product will propagate the last cumulated value and all the following
#    # values will have to be divided by that cumulated value to get the
#    # real linking factor.
#
#    # lf4 is a flag showing where oldwgts/raw is missing
#    # that is where we will get the cumulated values to divide by
#    df = df.withColumn(
#        'lf4',
#        F.when(df['lf1'].isNull(), df['lf3'])
#        .otherwise(None)
#    )
#
#    # window for fill forward imputation
#    window = (
#        Window
#        .partitionBy(['indicator', 'basis', 'cpa21'])
#        .orderBy('period')
#        .rowsBetween(-sys.maxsize, 0)
#    )
#
#    # fill in with the cumulated values by which we have to divide
#    df = df.withColumn('lf5', F.last(df['lf4'], ignorenulls=True).over(window))
#
#    # impute with 1 whatever remains as missing
#    df = df.fillna(1, subset=['lf5'])
#
#    # divide all the cumulated values to get the correct linking factor
#    df = df.withColumn('linking_factor', df['lf3']/df['lf5'])
#
#    # obtain the linked index value
#    df = df.withColumn('linked', df['raw']*df['linking_factor'])
#
#    # select, re-order and rename columns for final output
#    df = df.select([
#        'level',
#        'indicator',
#        'basis',
#        'cpa21',
#        'period',
#        'raw',
#        'oldwgts',
#        'linking_factor',
#        'linked'
#    ])
#
#    df = (
#        df
#        .withColumnRenamed('raw', 'index_value_raw')
#        .withColumnRenamed('oldwgts', 'index_value_oldwgts')
#        .withColumnRenamed('linked', 'index_value_linked')
#    )
#
#    # LOAD
#
#    print('writing final hive table....')
#    my_table = I.config_dev['db']+'.'+I.config_dev['index_values_linked_hive']
#    spark.sql(f"drop table if exists {my_table}")
#    df.createOrReplaceTempView("temp_table")
#    spark.sql(f"create table {my_table} USING PARQUET as select * from temp_table")
#
#    print(f"Indices have been saved in the '{I.config_dev['db']}' database" +
#          f"in the '{I.config_dev['index_values_linked_hive']}' table")
#    print('-'*25)
#    print('Completed chain linking: \n')
#
#    return df
