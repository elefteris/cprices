import pandas as pd

def geks(df):

    """
    Creates the geks indices table.

    Parameters
    ----------
    df : pandas dataframe
        Has the following columns:

        * 'group' : concatenated groupby column names with '//'
        * 'pair_of_months' : concatenated months (eg 2019-01-01_2019-05-01).
        * 'index_value' : the bilateral index values (based on any index method).

    Returns
    -------
    dfg : pandas dataframe
        The output geks dataframe has the groupby columns, month, index_value.
        It has the index value using geks coupled with any other index method
        for all the groups and months.

    Notes
    -----
    The mathematical theory behind this method can be found here:
    <LINK>

    """

    df = df.copy()

    # reshape to pairs of months (rows) by item//retailer (columns)
    df = pd.pivot_table(
        df,
        values='index_value',
        index='pair_of_months',
        columns='group'
    )

    df = df.reset_index()

    # the column names are the names of the groups
    cols = [c for c in df.columns if c != 'pair_of_months']

    # split pairs of months into m1, m2 and turn to datetime
    df['m1'], df['m2'] = df['pair_of_months'].str.split('_', 1).str
    months = sorted(list(set(list(df[['m1','m2']].values.flatten()))))
#    df['m1'], df['m2'] = pd.to_datetime(df['m1']), pd.to_datetime(df['m2'])

    # the first month across the whole period covered in the data
    m0 = months[0]

    # initialize geks jevons table
    dfg = pd.DataFrame({'group':cols, 0:1, 'month':m0})

#    # chris's approach
#    last_month = months[-1]
#    n_months = len(months)
#
#    for m in months[1:]:
#        # dataframe with all jevons indices needed for month m
#        dfm = pd.concat([
#            df[(df['m1']==m0) & (df['m2']>m0) & (df['m2']<=last_month)][cols],
#            df[(df['m2']==m) & (df['m1']>=m0) & (df['m1']<m)][cols],
#            df[(df['m1']==m) & (df['m2']>m) & (df['m2']<=last_month)][cols].rdiv(1)
#        ])
#        # calculate geometric averages
#        dfm = pd.DataFrame(dfm.product()**(1/n_months)).reset_index()
#        dfm['month'] = m
#
#        dfg = pd.concat([dfg, dfm])

    # matt's approach
    for n, m in enumerate(months[1:]):
        # dataframe with all jevons indices needed for month m
        dfm = pd.concat([
            df[(df['m1']==m0) & (df['m2']>m0) & (df['m2']<=m)],
            df[(df['m2']==m) & (df['m1']>=m0) & (df['m1']<m)]
        ])
        # calculate geometric averages
        dfm = pd.DataFrame(dfm[cols].product()**(1/(n+2))).reset_index()
        dfm['month'] = m
        dfg = pd.concat([dfg, dfm])

    dfg = dfg.rename(columns={'index':'group', 0:'index_value'})

    return dfg


def rygeks(df, d):

    """
    Creates the rygeks indices table.

    Parameters
    ----------
    df : pandas dataframe
        Has the following columns:

        * 'group' : concatenated groupby column names with '//'
        * 'pair_of_months' : concatenated months (eg 2019-01-01_2019-05-01).
        * 'index_value' : the bilateral index values (based on any index method).

    d : integer
        Length of rolling window (e.g. 13 months from January to January)

    Returns
    -------
    dfg : pandas dataframe
        The output rygeks dataframe has the groupby columns, month, index_value.
        It has the index value using rygeks coupled with any other index method
        for all the groups and months.

    Notes
    -----
    The mathematical theory behind this method can be found here:
    <LINK>

    """

    df = df.copy()

    # reshape to pairs of months (rows) by item//retailer (columns)
    df = pd.pivot_table(
        df,
        values='index_value',
        index='pair_of_months',
        columns='group'
    )

    df = df.reset_index()

    # the column names are the names of the groups
    cols = [c for c in df.columns if c != 'pair_of_months']

    # split pairs of months into m1, m2 and turn to datetime
    df['m1'], df['m2'] = df['pair_of_months'].str.split('_', 1).str
    months = sorted(list(set(list(df[['m1','m2']].values.flatten()))))
#    df['m1'], df['m2'] = pd.to_datetime(df['m1']), pd.to_datetime(df['m2'])

    # the first month across the whole period covered in the data
    m0 = months[0]

    # initialize rygeks jevons table up to month d-2
    # if d=13 and base month=Jan, then d-2 is the index of December in the
    # list of months
    dfrg1 = pd.DataFrame({'group':cols, 0:1, 'month':m0})

    # geks-j from m=0 up to m=d-1 (inclusive)
    for n, m in enumerate(months[1:d]):
        # dataframe with all jevons indices needed for month m
        dfm = pd.concat([
            df[(df['m1']==m0) & (df['m2']>m0) & (df['m2']<=m)],
            df[(df['m2']==m) & (df['m1']>=m0) & (df['m1']<m)]
        ])
        # calculate geometric averages
        dfm = pd.DataFrame(dfm[cols].product()**(1/(n+2))).reset_index()
        dfm['month'] = m
        dfrg1 = pd.concat([dfrg1, dfm])

    # separate the last month geks-j from dfrgj1
    # the last month is m as the last value from previous for-loop
    dfrg2 = dfrg1[dfrg1['month']==m]
    dfrg1 = dfrg1[dfrg1['month']!=m]

    # numerical columns of dfj (they have the jevons indices)
    num_cols = [c for c in df.columns if c not in ['m1', 'm2', 'pair_of_months']]

    for n, m in enumerate(months[d:]):

        m0 = months[n+1]
        m_previous = months[n+d-1]

        # dataframe with all jevons indices needed for month m

        # need to take the reciprocal of those indices
        dfm = df[(df['m1']>=m0) & (df['m1']<m_previous) & (df['m2']==m_previous)]

        # inverse value but copy first to avoid SettingWithCopyWarning
        dfm = dfm.copy()
        dfm[num_cols] = 1/dfm[num_cols]

        dfm = pd.concat([
            dfm,
            df[(df['m1']>=m0) & (df['m1']<m_previous) & (df['m2']==m)],
            df[(df['m1']==m_previous) & (df['m2']==m)],
            df[(df['m1']==m_previous) & (df['m2']==m)],
        ])
        # calculate geometric averages
        dfm = pd.DataFrame(dfm[cols].product()**(1/13)).reset_index()
        dfm['month'] = m
        dfrg2 = pd.concat([dfrg2, dfm])

    dfrg2 = dfrg2.sort_values(by=['group', 'month'], ascending=True)

    # rygeksj for months after the window is the cumulative product
    # of the rolling window geksj indices up to the month of interest
    dfrg2[0] = dfrg2.groupby('group').cumprod()

    # bring together all rygeksj indices for all months
    # up to the end of window and after
    dfrg = pd.concat([dfrg1, dfrg2])

    dfrg = dfrg.rename(columns={'index':'group', 0:'index_value'})

    return dfrg
