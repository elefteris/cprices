from importlib import reload, import_module
from datetime import datetime

def check_params(config_dir, selected_scenarios):

    """
    This function runs at the very beginning and includes assert statements
    to ensure that the config parameters are valid, i.e. they have the right
    data type and values within the permitted range.
    """

    for scenario in selected_scenarios:
        # import and reload config file for scenario
        config = import_module(f'{config_dir}.{scenario}')
        reload(config)
        params = config.params

        ks = [
            'staged_data', 'preprocessing',  'classification',
            'outlier_detection', 'averaging', 'grouping', 'imputation',
            'filtering', 'indices'
        ]
        for k in ks:
            if k not in params.keys():
                msg = f'{k} does not appear among the config parameters.'
                raise Exception(msg)

        # Start date
        try:
            datetime.strptime(params['preprocessing']['start_date'], "%Y-%m-%d")
        except:
            raise ValueError("Datestamp in incorrect format - must be 'YYYY-mm-dd'")

        # End date
        try:
            datetime.strptime(params['preprocessing']['end_date'], "%Y-%m-%d")
        except:
            raise ValueError("Datestamp in incorrect format - must be 'YYYY-mm-dd'")

        # Classification
        msg = "parameter 'active' in classification, web_scraped is not a boolean"
        assert isinstance(params['classification']['web_scraped']['active'], bool), msg

        # Outlier detection
        msg = "parameter 'active' in outlier_detection is not a boolean"
        isbool = isinstance(params['outlier_detection']['active'], bool)
        assert isbool, msg
        if isbool:
            if params['outlier_detection']['active']:
                msg = "parameter 'log_transform' in outlier_detection is not a boolean"
                assert isinstance(params['outlier_detection']['log_transform'], bool), msg

                method = params['outlier_detection']['method']
                msg = ("the outlier detection method must be a string " +
                       "among 'tukey', 'kimber', 'ksigma', 'udf_fences'")
                assert isinstance(method, str), msg
                assert method in ['tukey', 'kimber', 'ksigma', 'udf_fences'], msg

                k = params['outlier_detection']['k']
                msg = 'k for outlier detection must be a float between 1 and 4'
                assert isinstance(k, float), msg
                assert (k>=1)&(k<=4), msg

        # Averaging
        methods = [
            'unweighted_arithmetic',
            'unweighted_geometric',
            'weighted_arithmetic',
            'weighted_geometric'
        ]
        for data_source in ['web_scraped', 'scanner']:
            method = params['averaging'][data_source]['method']
            msg = (f"the averaging method for {data_source} must be a string among {methods}")
            assert isinstance(method, str), msg
            assert method in methods, msg

        # Grouping
        msg = "parameter 'active' in grouping is not a boolean"
        assert isinstance(params['grouping']['active'], bool), msg

        # Imputation
        msg = "parameter 'active' in imputation is not a boolean"
        isbool = isinstance(params['imputation']['active'], bool)
        assert isbool, msg
        if isbool:
            if params['imputation']['active']:
                ffill_limit = params['imputation']['ffill_limit']
                msg = 'ffill_limit for imputation must be an integer greater than 0'
                assert isinstance(ffill_limit, int), msg
                assert (ffill_limit>0), msg

        # Filtering
        msg = "parameter 'active' in filtering is not a boolean"
        isbool = isinstance(params['filtering']['active'], bool)
        assert isbool, msg
        if isbool:
            if params['filtering']['active']:
                mcs = params['filtering']['max_cumsum_share']
                msg = 'max_cumsum_share for filtering must be a float between 0 and 1'
                assert isinstance(mcs, float), msg
                assert (mcs>=0)&(mcs<=1), msg

        # Indices
        d = params['indices']['splice_window_length']
        msg = 'splice window length for rygeks must be a positive integer'
        assert isinstance(d, int), msg
        assert d>0, msg
