# @staticmethod
#  def prepare_input_data(
#    index_weights,
#    imputed_raw,
#    imputed_oldwgts
#  ):
#
#    """
#    Data engineer weights and price relatives before aggregation.
#
#    Parameters
#    ----------
#    index_weights : spark dataframe
#        Has the weights of all the nodes from level 1 and above for the
#        aggregation process. Includes the columns (among others):
#
#        * period
#        * parent_level
#        * parent_indicator
#        * parent_basis
#        * parent_cpa21
#        * child_level
#        * child_indicator
#        * child_basis
#        * child_cpa21
#        * weight
#
#    imputed_raw : spark dataframe
#        Has the imputed price relatives that will be used to calculate the raw
#        index values. These price relatives were imputed using the current
#        weights. Includes the columns (among others):
#
#        * period
#        * indicator
#        * basis
#        * cpa21
#        * item_no
#        * price_relative
#        * rescaled_weight
#
#    imputed_oldwgts : spark dataframe
#        Has the imputed price relatives that will be used to calculate the
#        oldwgts index values. These price relatives were imputed using the
#        old weights, i.e. the weights from the previous month.
#        Includes the columns (among others):
#
#        * period
#        * indicator
#        * basis
#        * cpa21
#        * item_no
#        * price_relative
#
#    Returns
#    -------
#    weights : spark dataframe
#        Has the weights of all the nodes from level 1 and above as well as
#        the item weights for the aggregation process. Has the columns:
#
#        * period
#        * parent_level
#        * parent_id
#        * child_level
#        * child_id
#        * weight
#        * raw/oldwgts
#
#    values : spark dataframe
#        Has the imputed price relatives (raw and oldwgts) of all the items
#        and periods. Has the columns:
#
#        * period
#        * child_id
#        * value
#        * raw/oldwgts
#
#    levels : spark dataframe
#        Used to create the level column in the output dataset showing the level
#        of each node. Has the columns 'node' and 'level'.
#
#    Notes
#    -----
#    The weights table includes the pairs level 1 parent - level 0 child where
#    the level 0 children are the items.
#
#    The parent_id and child_id are the result from concatenating cpa21, item
#    and indicator unless we are talking about an item in which case
#    child_id=item_no.
#
#    The raw/oldwgts flag column allows the two tables (raw and old weights)
#    to be unioned so that the same functions are not applied twice but only
#    once on the unioned table.
#
#    The old weights (weights from previous month) are created by shifting
#    forward the weight column by one month. This way, the weight from the
#    previous month will be used with the price relative of the current month.
#
#    """
#
#    # PREPARE IMPUTED RAW TABLE
#
#    # create node id for parents (nodes on level 1)
#    imputed_raw = imputed_raw.withColumn(
#        'parent_id',
#        F.concat(
#            F.col('indicator'),
#            F.lit('/'),
#            F.col('basis'),
#            F.lit('/'),
#            F.col('cpa21')
#        )
#    )
#
#    imputed_raw = imputed_raw.drop('weight')
#
#    imputed_raw = (
#        imputed_raw
#        .withColumnRenamed('item_no', 'child_id')
#        .withColumnRenamed('rescaled_weight', 'weight')
#        .withColumnRenamed('imputed_price_relative', 'value') # imputed_price_relative, price_relative
#        .withColumn('parent_level', F.lit(1))
#        .withColumn('child_level', F.lit(0))
#    )
#
#    # PREPARE IMPUTED OLDWGTS TABLE
#
#    # create node id for parents (nodes on level 1)
#    imputed_oldwgts = imputed_oldwgts.withColumn(
#        'parent_id',
#        F.concat(
#            F.col('indicator'),
#            F.lit('/'),
#            F.col('basis'),
#            F.lit('/'),
#            F.col('cpa21')
#        )
#    )
#
#    imputed_oldwgts = imputed_oldwgts.drop('weight')
#
#    imputed_oldwgts = (
#        imputed_oldwgts
#        .withColumnRenamed('item_no', 'child_id')
#        .withColumnRenamed('imputed_price_relative_oldwgts', 'value') # imputed_price_relative_oldwgts
#        .withColumn('parent_level', F.lit(1))
#        .withColumn('child_level', F.lit(0))
#    )
#
#    # PREPARE ITEM WEIGHTS TABLE
#
#    cols = [
#        'period',
#        'parent_level',
#        'parent_id',
#        'child_level',
#        'child_id',
#        'weight'
#    ]
#
#    item_weights = imputed_raw.select(cols)
#
#    # PREPARE INDEX WEIGHTS TABLE
#
#    # create node id for parents
#    index_weights = index_weights.withColumn(
#        'parent_id',
#        F.concat(
#            F.col('parent_indicator'),
#            F.lit('/'),
#            F.col('parent_basis'),
#            F.lit('/'),
#            F.col('parent_cpa21')
#        )
#    )
#
#    # create node id for children
#    index_weights = index_weights.withColumn(
#        'child_id',
#        F.concat(
#            F.col('child_indicator'),
#            F.lit('/'),
#            F.col('child_basis'),
#            F.lit('/'),
#            F.col('child_cpa21')
#        )
#    )
#
#    # APPEND ITEM WEIGHTS AND INDEX WEIGHTS TABLES
#
#    # select columns needed for index calculations
#    item_weights = item_weights.select(cols)
#    index_weights = index_weights.select(cols)
#
#    # union tables since they have same columns in the same order
#    # this is the data/weights used for the index value raw
#    raw = index_weights.union(item_weights)
#
#    # SHIFT WEIGHT COLUMN FORWARD BY ONE MONTH TO CREATE OLD WEIGHTS
#
#    # Shift column one period forward. This way, the weight of the previous
#    # month becomes the weight of the current month for each node.
#    pb_cols = ['parent_id', 'child_id']
#    ob_cols = ['parent_id', 'child_id', 'period']
#    w = Window().partitionBy(pb_cols).orderBy(ob_cols)
#    oldwgts = raw.select("*", F.lag('weight').over(w).alias('weight_old')) #.na.drop()
#    oldwgts = oldwgts.drop('weight').withColumnRenamed('weight_old', 'weight')
#
#    # UNION RAW WEIGHTS AND OLD WEIGHTS
#
#    raw = raw.withColumn('raw/oldwgts', F.lit('raw'))
#    oldwgts = oldwgts.withColumn('raw/oldwgts', F.lit('oldwgts'))
#    weights = raw.union(oldwgts)
#
#    # PREPARE IMPUTED DATASET (VALUES = PRICE RELATIVES)
#
#    imputed_raw = imputed_raw.select('period', 'child_id','value')
#    imputed_raw = imputed_raw.withColumn('raw/oldwgts', F.lit('raw'))
#    imputed_oldwgts = imputed_oldwgts.select('period', 'child_id','value')
#    imputed_oldwgts = imputed_oldwgts.withColumn('raw/oldwgts', F.lit('oldwgts'))
#    values = imputed_raw.union(imputed_oldwgts)
#
#    # PREPARE LEVELS TABLE
#
#    levels_parents = (
#        weights
#        .select('parent_level','parent_id')
#        .withColumnRenamed('parent_level', 'level')
#        .withColumnRenamed('parent_id', 'node')
#    )
#    levels_children = (
#        weights
#        .select('child_level','child_id')
#        .withColumnRenamed('child_level', 'level')
#        .withColumnRenamed('child_id', 'node')
#    )
#    levels = levels_parents.union(levels_children)
#    levels = levels.dropDuplicates()
#
#    return weights, values, levels
#
#  @staticmethod
#  def level_aggregation(df):
#      """
#      Calculates the index values of the nodes on one level (parents) using a
#      weighted average of the index values of their children.
#
#      Parameters
#      ----------
#      df : spark dataframe
#          Has all the children nodes of the parent nodes, the values of the
#          children and the weights to aggregate the children to the parents.
#          Has the columns:
#
#          * raw/oldwgts
#          * period
#          * parent_id
#          * weight
#          * value
#
#      Returns
#      -------
#      df : spark dataframe
#          Has the parents with their index values, calculates as weighted averages
#          of the values of their children. Given that these parents will become
#          the children for parents on higher levels, the parent_id column is
#          renamed to child_id. Has the columns:
#
#          * raw/oldwgts
#          * period
#          * child_id
#          * value
#
#      Notes
#      -----
#      The values of the children can be price relatives if the parents are on
#      level 1 or index values if the parents are nodes on levels above level 1.
#
#      """
#
#      # calculate weighted average of value
#      df = df.withColumn('value', df['value']*df['weight'])
#      df = df.groupBy(['raw/oldwgts', 'period', 'parent_id']).sum()
#
#      # the sum(value) is the weighted average that will become the value for the
#      # next level and the parents will become children for the next level aggregation
#      df = (
#          df
#          .withColumnRenamed('sum(value)', 'value')
#          .withColumnRenamed('parent_id', 'child_id')
#      )
#
#      # the weights are % (e.g. 0.2 appears as 20) so need to divide by 100
#      df = df.withColumn('value', df['value']/100)
#
#      return df.select('raw/oldwgts', 'child_id', 'period', 'value')
#
#
#  def aggregate_index_values(self, weights, values, n_levels):
#
#    """
#    Carries out aggregation on all levels of classification starting from price
#    relatives of the items at the bottom under the level 1 nodes.
#
#    Parameters
#    ----------
#    weights : spark dataframe
#        Has the weights of all the nodes from level 1 and above as well as
#        the item weights for the aggregation process. Has the columns:
#
#        * period
#        * parent_level
#        * parent_id
#        * child_level
#        * child_id
#        * weight
#        * raw/oldwgts
#
#    values : spark dataframe
#        Has the imputed price relatives (raw and oldwgts) of all the items
#        and periods. Has the columns:
#
#        * period
#        * child_id
#        * value
#        * raw/oldwgts
#
#    n_levels : integer
#        Number of levels above the items (currently 7)
#
#
#    Returns
#    -------
#    values : spark dataframe
#        Has the index values (raw and ol weight) of all the nodes in the tree
#        from level 1 and above for all the periods of interest. Has the columns:
#
#        * period
#        * node
#        * value
#        * raw/oldwgts
#
#    Notes
#    -----
#    The aggregation starts from the bottom of the tree. We aggregate price
#    relatives of items to get the index values of the nodes on level one and
#    then we keep aggregating the index values of the nodes moving upwards
#    until we reach the top nodes on the highest level.
#
#    Every time we aggregate all the children that belong to parents of a
#    specific level, these parents will become children to be aggregated to
#    parents that belong to higher levels (either the next or above the next).
#
#    We loop through the parent levels. In every loop, we select the pairs
#    parent-child from the weights table for the specific parent level and we
#    join with the index values of the children that were calculated in the
#    previous loop unless parent_level=1 in which case the the values are the
#    price relatives of the items.
#
#    When parent_level=1, the children are the items. But there are level 1
#    nodes that don't have any children/items in some or any periods. In that
#    case, when we aggregate the price relatives to level 1 nodes, those
#    nodes/parents that don't have any items will not appear as children
#    for the next level aggregation. So they are brought from the item weights
#    table by filtering for: child_level=1 AND weight=0. This table is
#    appended to the calculated index values on level 1 but the level 1 nodes
#    that don't have items, get a zero weight and a zero value.
#
#    In every loop, the index values for a specific parent level are calculated
#    and then appended to the final values table (output).
#
#    """
#
#    # columns to join weights and values dataframes as we aggregate on each level
#    join_cols = ['raw/oldwgts', 'child_id', 'period']
#
#    for level in range(1, n_levels+1):
#
#        print(f'Level: {level}')
#
#        # select the parent-child pairs and weights for the specific parent level
#        weights_level = weights.filter(weights['parent_level']==level)
#
#        # join values and weights in one table to calculate the index values for the level
#        values_level = weights_level.join(values, join_cols, 'left')
#
#        # carry out aggregation on the parent level
#        values_level = Modules.level_aggregation(values_level)
#
#        values_level = values_level.dropna(subset=['child_id'])
#
#        # initialise final index values dataframe and account for those level 1
#        # nodes that don't have any items
#        if level==1:
#            missing_nodes = (
#                weights
#                .filter((weights['child_level']==1)&(weights['weight']==0))
#                .select('raw/oldwgts', 'child_id', 'period', 'weight')
#                .withColumnRenamed('weight', 'value')
#            )
#            values_level = values_level.union(missing_nodes)
#            values = values_level
#        else:
#            # append calculated index values from this level to all the index
#            # values that have been calculated so far for the index nodes below
#            # the current parent level.
#            values = reduce(DataFrame.unionByName, [values, values_level])
#
#    values = values.localCheckpoint()
#    return values.withColumnRenamed('child_id', 'node')
#
#  @staticmethod
#  def prepare_output_data(df, imputed_raw, levels):
#
#    """
#    Data engineer output table to have structure and columns based on
#    requirements.
#
#    Parameters
#    ----------
#    df : spark dataframe
#        Has the index values (raw and ol weight) of all the nodes in the tree
#        from level 1 and above for all the periods of interest after aggretaion.
#        It has the columns:
#
#        * period
#        * node
#        * value
#        * raw/oldwgts
#
#    imputed_raw : spark dataframe
#        Has the imputed price relatives that will be used to calculate the raw
#        index values. These price relatives were imputed using the current
#        weights. Includes the columns:
#
#        * period
#        * indicator
#        * basis
#        * cpa21
#        * item_no
#        * price_relative
#        * rescaled_weight
#
#        * Def
#        * Coverage
#        * Item_count
#        * Pub_marker
#        * Input_file_name
#        * Date_stamp
#
#    levels : spark dataframe
#        Used to create the level column in the output dataset showing the level
#        of each node. Has the columns 'node' and 'level'.
#
#
#    Returns
#    -------
#    df : spark dataframe
#        Final table as the output from the index aggregation stage that will
#        continue to the next stage of the pipeline - chain linking.
#        It has the following columns
#
#        * Period
#        * Level
#        * Indicator
#        * Basis
#        * Cpa21
#        * Index_value_raw
#        * Index_value_oldwgts
#
#        The following columns are added back to the table by joining with the
#        imputed_raw table:
#
#        * Def (from imputed_raw)
#        * Coverage (from imputed_raw)
#        * Item_count (from disclosure)
#        * Pub_marker (from disclosure)
#        * Input_file_name (from imputed_raw)
#        * Date_stamp (from imputed_raw)
#
#    Notes
#    -----
#    PIVOT RAW/OLDWGTS COLUMN TO GET INDEX_VALUE_RAW AND INDEX_VALUE_OLDWGTS COLUMNS
#    ADD NODE LEVEL COLUMN
#    SPLIT NODE ID COLUMN INTO INDICATOR, BASIS AND CPA21
#    BRING REMAINING COLUMNS FROM IMPUTED DATAFRAME
#
#    """
#
#    # PIVOT RAW/OLDWGTS COLUMN TO GET INDEX_VALUE_RAW AND INDEX_VALUE_OLDWGTS COLUMNS
#
#    df = (
#        df
#        .groupby(['period', 'node'])
#        .pivot('raw/oldwgts')
#        .sum('value')
#    )
#
#    # ADD NODE LEVEL COLUMN
#
#    df = df.join(levels, ['node'], 'left')
#
#    # SPLIT NODE ID COLUMN INTO INDICATOR, BASIS AND CPA21
#
#    split_col = F.split(df['node'], '/')
#    df = (
#        df
#        .where(df['node'].isNotNull())
#        .withColumn('indicator', split_col.getItem(0))
#        .withColumn('basis', split_col.getItem(1))
#        .withColumn('cpa21', split_col.getItem(2))
#        .drop('node')
#    )
#
#    # BRING REMAINING COLUMNS FROM IMPUTED DATAFRAME
#
##    df = df.join(imputed_raw, ['period', 'indicator', 'basis', 'cpa21'], 'left')
#
#    return df
#
#
#  def index_aggregation(
#    self,
#    spark,
#    I,
#    n_levels,
#  ):
#
#    """
#    Main function for index calculations.
#
#    Parameters
#    ----------
#    index_weights : spark dataframe
#        Has the weights of all the nodes from level 1 and above for the
#        aggregation process. Includes the columns (among others):
#
#        * period
#        * parent_level
#        * parent_indicator
#        * parent_basis
#        * parent_cpa21
#        * child_level
#        * child_indicator
#        * child_basis
#        * child_cpa21
#        * weight
#
#    imputed_raw : spark dataframe
#        Has the imputed price relatives that will be used to calculate the raw
#        index values. These price relatives were imputed using the current
#        weights. Includes the columns (among others):
#
#        * period
#        * indicator
#        * basis
#        * cpa21
#        * item_no
#        * price_relative
#        * rescaled_weight
#
#    imputed_oldwgts : spark dataframe
#        Has the imputed price relatives that will be used to calculate the
#        oldwgts index values. These price relatives were imputed using the
#        old weights, i.e. the weights from the previous month.
#        Includes the columns (among others):
#
#        * period
#        * indicator
#        * basis
#        * cpa21
#        * item_no
#        * price_relative
#
#    n_levels : integer
#        Number of levels in the classification tree, starting counting from
#        level 1 just above the item level up to the top of the classification.
#
#
#    Returns
#    -------
#    indices : spark dataframe
#        Final table as the output from the index aggregation stage that will
#        continue to the next stage of the pipeline - chain linking.
#        It has the following columns:
#
#        * Period
#        * Level
#        * Indicator
#        * Basis
#        * Cpa21
#        * Index_value_raw
#        * Index_value_oldwgts
#
#        The following columns are added back to the table by joining with the
#        imputed_raw table:
#
#        * Def
#        * Coverage
#        * Item_count
#        * Pub_marker
#        * Input_file_name
#        * Date_stamp
#
#    """
#
#    print('-'*50)
#    print('Commencing index aggregation: \n')
#
#    # EXTRACT
#    index_weights = spark.sql('SELECT * FROM ' + I.config_dev["db"]+'.'+I.config_dev["index_weights_hive"])
#    imputed_raw = spark.sql('SELECT * FROM ' + I.config_dev["db"]+'.'+I.config_dev["imputation"])
#    imputed_oldwgts = spark.sql('SELECT * FROM ' + I.config_dev["db"]+'.'+I.config_dev["imputation_oldwgts"])
#
#    weights, values, levels = Modules.prepare_input_data(
#        index_weights,
#        imputed_raw,
#        imputed_oldwgts
#    )
#
#    indices = Modules.aggregate_index_values(self, weights, values, n_levels)
#
#    indices = Modules.prepare_output_data(indices, imputed_raw, levels)
#
#    # LOAD
#
#    print('writing final hive table....')
#    my_table = I.config_dev['db']+'.'+I.config_dev['index_values_agg_hive']
#    spark.sql(f"drop table if exists {my_table}")
#    indices.createOrReplaceTempView("temp_table")
#    spark.sql(f"create table {my_table} USING PARQUET as select * from temp_table")
#
#    print(f"Indices have been saved in the '{I.config_dev['db']}' database" +
#          f"in the '{I.config_dev['index_values_agg_hive']}' table")
#    print('-'*25)
#    print('Completed index aggregation: \n')
#    return indices
#
#
#
