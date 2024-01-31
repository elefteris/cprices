# dimensionality
groupby_cols = ['data_source', 'supplier', 'item', 'retailer']

# HDFS directories
staged_dir      = '/dap/workspace_zone/prices_webscraping/pipeline_data/staged/'
processed_dir   = '/dap/workspace_zone/prices_webscraping/pipeline_data/processed'
test_dir        = '/dap/workspace_zone/prices_webscraping/pipeline_data/test'
mappers_dir     = '/dap/workspace_zone/prices_webscraping/pipeline_data/mappers'

analysis_params = {
    'sample_count' : {
        'input_table' : 'filtered'
    },
    'churn' : {
        'input_table' : 'filtered'
    },
    'price_stats' : {
        'input_table' : 'filtered'
    },
    'classification' : {
        'input_table' : 'classified',
        'flag_col'    : 'classified',
        'metric1'     : 'not_item_count',
        'metric2'     : 'item_count',
        'metric3'     : 'not_item_frac',
    },
    'outlier_detection' : {
        'input_table' : 'inliers_outliers',
        'flag_col'    : 'outlier',
        'metric1'     : 'outliers_count',
        'metric2'     : 'inliers_count',
        'metric3'     : 'outliers_frac',
    },
    'imputation' : {
        'input_table' : 'imputed_flagged',
        'flag_col'    : 'imputed',
        'metric1'     : 'imputed_count',
        'metric2'     : 'not_imputed_count',
        'metric3'     : 'imputed_frac',
    },
    'filtering' : {
        'input_table' : 'expenditure',
        'flag_col'    : 'filtered',
        'metric1'     : 'less_popular_count',
        'metric2'     : 'popular_count',
        'metric3'     : 'less_popular_frac',
    },
    'low_level_indices' : {
        'input_table' : 'low_level_indices'
    },
    'item_indices' : {
        'input_table' : 'item_indices'
    },
    'distributions' : {
        'input_table' : 'filtered',
        'n_bins' : 30,
        'column' : 'price'
    },
}
