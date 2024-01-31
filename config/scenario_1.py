fences = {
    # ONS items
    'laptop'     : [75, 7000],
    'desktop'    : [75, 5000],
    'smartphone' : [10, 3000],
    'tablet'     : [50, 7000],
    'rugs'       : [10, 150],
    'laminate'   : [10, 50],
}

params = {
    'staged_data' : {
#        'web_scraped' : {
#            'supplier_1' : {
#                'laptop' : {
#                    'retailer_1' : 1/3,
#                    'retailer_2' : 1/3,
#                    'retailer_3' : 1/3,
#                },
#                'desktop' : {
#                    ''retailer_1' : 1/3,
#                    'retailer_2' : 1/3,
#                    'retailer_3' : 1/3,
#                },
#                'smartphone' : {
#                    'retailer_1' : 1/3,
#                    'retailer_2' : 1/3,
#                    'retailer_3' : 1/3,
#                },
#                'tablet' : {
#                    'retailer_1' : 1/3,
#                    'retailer_2' : 1/3,
#                    'retailer_3' : 1/3,
#                },
#            },
#        },
        'scanner' : {
            'supplier_2' : "",
        },
    },
    'preprocessing' : {
        'start_date'     : '2010-01-01',
        'end_date'       : '2020-06-01',
        'drop_retailers' : False,
    },
    'classification' : {'web_scraped' : {'active' : True}, }, # True, False
    'outlier_detection' : {
        'active'       : True,    # True, False
        'log_transform': False,   # True, False
        'method'       : 'tukey', # tukey, kimber, ksigma, udf_fences
        'k'            : 1.5,     # float recommended values from 1...4
        'fences'       : fences
    },
    'averaging' : {
        # unweighted_arithmetic, unweighted_geometric, weighted_arithmetic, weighted_geometric
        'web_scraped' : {'method' : 'weighted_arithmetic'},
        'scanner'     : {'method' : 'unweighted_arithmetic'},
    },
    'grouping' : {
        'active'      : True, # True, False
        'web_scraped' : {'method' : 'weighted_arithmetic'},
        'scanner'     : {'method' : 'unweighted_arithmetic'},
    },
    'imputation' : {
        'active'      : True,  # True, False
        'ffill_limit' : 3      # posive integer
    },
    'filtering' : {
        'active'           : True, # True, False
        'max_cumsum_share' : 0.8   # float 0...1
    },
    'indices' : {
        'methods' : [
            # UNWEIGHTED'
            'carli_fixed_base',
            'carli_chained',
            'dutot_fixed_base',
            'dutot_chained',
            'jevons_fixed_base',
            'jevons_chained',
            'rygeks_jevons',
            #'geks_jevons',
            # WEIGHTED'
            'laspeyres_fixed_base',
            'tornqvist_fixed_base',
            #'rygeks_tornqvist',
            #'rygeks_fisher',
            #'rygeks_laspeyres',
            #'rygeks_paasche',
            #'geks_tornqvist',
            #'geks_paasche',
            #'geks_laspeyres',
            #'geary_khamis',
        ],
        'splice_window_length' : 3,
        'geary_khamis_all_periods' : True, # True, False
        'geary_khamis_convergence_tolerance' : 1e-5, # floating point value
    }
}
