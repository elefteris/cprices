# python3 ~/cprices/cprices/steps/dashboard.py

# import python libraries
import plotly.offline as pyo
import plotly.graph_objs as go
import plotly.figure_factory as ff
from plotly import tools
import pandas as pd
import numpy as np
import os
import sys
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from importlib import reload

sys.path.append("/home/cdsw/")

# import custom modules
import utils
reload(utils)

# import configuration files
from cprices.config import dev_config
#import config
#reload(config)
reload(dev_config)

####################################
############## SET ME ##############
run_id = '20200107_111416_karace'
####################################

def create_button_title(g):
    '''Create radio button to control displaying figure tile
    '''
    button = (
        html.Div([
            html.Label('Display title:'),
            dcc.RadioItems(
                id=f'title_picker_{g}',
                options=[{'label': i, 'value': i} for i in ['Yes', 'No']],
                value='Yes',
            )
        ],
        style={'width': '10%',
               'display': 'inline-block'
              }
        )
    )
    return button


def create_button_ylim(g):
    '''Create radio button to restrict y-axis range.
    '''
    button = (
        html.Div([
            html.Label('Y axis limits:'),
            dcc.RadioItems(
                id=f'ylim_picker_{g}',
                options=[{'label': i, 'value': i} for i in ['Fixed', 'Free']],
                value='Fixed',
            )
        ],
        style={'width': '10%',
               'display': 'inline-block'
              }
        )
    )
    return button


def create_button_dists(g):
    '''Create radio button to choose distribution plot type
    '''
    button = (
        html.Div([
            html.Label('Display distribution data as:'),
            dcc.RadioItems(
                id=f'distplot_picker_{g}',
                options=[{'label': i, 'value': i} for i in ['Frequency', 'Histogram']],
                value='Frequency',
            )
        ],
        style={'width': '20%',
               'display': 'inline-block'
              }
        )
    )
    return button

def create_button_dists_abs_rel(g):
    '''Create radio button to choose relative or absolute for distribution plot
    '''
    button = (
        html.Div([
            html.Label('Display distribution data values as:'),
            dcc.RadioItems(
                id=f'dist_absrel_picker_{g}',
                options=[{'label': i, 'value': i} for i in ['Absolute', 'Relative']],
                value='Absolute',
            )
        ],
        style={'width': '24%',
               'display': 'inline-block'
              }
        )
    )
    return button

def create_dropdown(c, options, g):
    return html.Div([
        html.Label(f"{c.replace('_', ' ').title()}"),
        dcc.Dropdown(
            id=f'{c}_picker_{g}',
            options=options[c],
            placeholder=f"Select {c.replace('_', ' ')}",
            # value=options[s][0]['label']
        ),
    ],
        style={'width': '20%',
               'display': 'inline-block'
              }
    )


def create_pickers(columns, n):
    return [Input(f'{c}_picker_{n}', 'value') for c in columns]


def create_scatter_traces(df, column):
    traces = []
    for value in df[column].unique():
        dfb = df[df[column]==value]
        traces.append(
            go.Scatter(
                x=dfb['month'],
                y=dfb['value'],
                name=value,
                mode='markers+lines',
                opacity=0.7,
                marker={'size': 10}
            )
        )
    return traces

def create_frequency_traces(df, column):

    # frequency
    traces = []
    if not df.empty:
        df = df.sort_values(by=['month', 'metric'])

        for value in df[column].unique():
            dfb = df[df[column]==value]

            traces.append(
                go.Scatter(
                    x=dfb['metric'],
                    y=dfb['value'],
                    name=value,
                    mode='markers+lines',
                    opacity=0.7,
                    marker={'size': 5}
                )
            )
    return traces


def create_histogram_traces(df, column):

    # BARPLOTS
    traces = []
    if not df.empty:
        df = df.sort_values(by=[column, 'metric'])

        for value in df[column].unique():
            dfb = df[df[column]==value]

            traces.append(
                go.Bar(
                    x=dfb['metric'],
                    y=dfb['value'],
                    name=value,
                    opacity = 0.5
                )
            )


#    # HISTOGRAMS
#    # use this code for histograms instead of barplots (not big data)
#    traces = []
#    start = np.min(df['value'])
#    end = np.max(df['value'])
#    size = (end-start)/100
#    for value in df[column].unique():
#        dfb = df[df[column]==value]
#        traces.append(
#            go.Histogram(
#                x = dfb['value'],
#                name = value,
#                opacity = 0.5,
#                marker={'line' : {'color':'black', 'width' : 0.5}},
#                xbins=dict(start=start, end=end, size=size)
#            )
#        )

    return traces

# IMPORT AND PREPARE DATA

# data for dashboard
#run_id = config.run_id
processed_dir = dev_config.processed_dir
folderpath = os.path.join(processed_dir, run_id, 'analysis')
filepaths = utils.get_hdfs_files(folderpath, strings_to_search_for=[".csv"])
df = utils.hdfs_read_csv(filepaths[0])
df['value'] = pd.to_numeric(df['value'], errors='coerce')
cols = ['table','data_source', 'scenario', 'item', 'retailer', 'metric', 'month', 'value']
df = df.sort_values(by=cols[:-1], ascending=True)
df['scenario'] = df['scenario'].apply(lambda x: ' '.join(['scenario',str(x)]))

df_ts = df[df['table']!='distributions']
df_dist = df[df['table']=='distributions']
df_dist['metric'] = pd.to_numeric(df_dist['metric'])

# visualizations parameters
layout = go.Layout(xaxis={'title': 'month'}, yaxis={'title': 'value'})
options_ts = {}
for c in ['table', 'data_source', 'scenario', 'item', 'retailer', 'metric', 'month']:
    options_ts[c] = [{'label':str(i),'value':i} for i in df_ts[c].unique()]
options_dist = {}
for c in ['table', 'data_source', 'scenario', 'item', 'retailer', 'metric', 'month']:
    options_dist[c] = [{'label':str(i),'value':i} for i in df_dist[c].unique()]

# CREATE DASH APPLICATION

app = dash.Dash()

app.layout = html.Div([

    html.H1('Consumer Price Indices Dashboard'),
    html.H3(f'Using data from: {run_id}'),

    html.Div([

        # TIME SERIES

        html.Div([
            html.H2('1. Time series: Compare metrics for a specific table, scenario, data source, item and retailer'),
            create_dropdown('table', options_ts, '1'),
            create_dropdown('scenario', options_ts, '1'),
            create_dropdown('data_source', options_ts, '1'),
            create_dropdown('item', options_ts, '1'),
            create_dropdown('retailer', options_ts, '1'),
            html.Br(),
            html.Br(),
            create_button_title('1'),
            create_button_ylim('1'),
            dcc.Graph(id='g1'),
        ]),
        html.Div([
            html.H2('2. Time series: Compare scenarios for a specific table, data source, item, retailer and metric'),
            create_dropdown('table', options_ts, '2'),
            create_dropdown('data_source', options_ts, '2'),
            create_dropdown('item', options_ts, '2'),
            create_dropdown('retailer', options_ts, '2'),
            create_dropdown('metric', options_ts, '2'),
            html.Br(),
            html.Br(),
            create_button_title('2'),
            create_button_ylim('2'),
            dcc.Graph(id='g2'),
        ]),
        html.Div([
            html.H2('3. Time series: Compare retailers for a specific table, scenario, data source, item and metric'),
            create_dropdown('table', options_ts, '3'),
            create_dropdown('scenario', options_ts, '3'),
            create_dropdown('data_source', options_ts, '3'),
            create_dropdown('item', options_ts, '3'),
            create_dropdown('metric', options_ts, '3'),
            html.Br(),
            html.Br(),
            create_button_title('3'),
            create_button_ylim('3'),
            dcc.Graph(id='g3'),
        ]),

        # HISTOGRAMS

        html.Div([
            html.H2('4. Price distributions: Compare different months for a specific item, retailer and scenario'),
            create_dropdown('data_source', options_dist, '4'),
            create_dropdown('item', options_dist, '4'),
            create_dropdown('retailer', options_dist, '4'),
            create_dropdown('scenario', options_dist, '4'),
            html.Br(),
            html.Br(),
            create_button_dists('4'),
            create_button_dists_abs_rel('4'),
            create_button_title('4'),
            dcc.Graph(id='g4'),
        ]),
        html.Div([
            html.H2('5. Price distributions: Compare different retailers for a specific item, month and scenario'),
            create_dropdown('data_source', options_dist, '5'),
            create_dropdown('item', options_dist, '5'),
            create_dropdown('month', options_dist, '5'),
            create_dropdown('scenario', options_dist, '5'),
            html.Br(),
            html.Br(),
            create_button_dists('5'),
            create_button_dists_abs_rel('5'),
            create_button_title('5'),
            dcc.Graph(id='g5'),
        ]),
        html.Div([
            html.H2('6. Price distributions: Compare different scenarios for a specific item, retailer and month'),
            create_dropdown('data_source', options_dist, '6'),
            create_dropdown('item', options_dist, '6'),
            create_dropdown('retailer', options_dist, '6'),
            create_dropdown('month', options_dist, '6'),
            html.Br(),
            html.Br(),
            create_button_dists('6'),
            create_button_dists_abs_rel('6'),
            create_button_title('6'),
            dcc.Graph(id='g6'),
        ]),
    ])
])

# TIME SERIES

@app.callback(
    Output('g1', 'figure'),
    create_pickers(['table', 'data_source', 'scenario', 'item', 'retailer', 'ylim', 'title'], 1),
)
def update_figure(table, data_source, scenario, item, retailer, ylim, title):
    dff = df_ts[
        (df_ts['metric']!='price')&
        (df_ts['table']==table)&
        (df_ts['data_source']==data_source)&
        (df_ts['scenario']==scenario)&
        (df_ts['item']==item)&
        (df_ts['retailer']==retailer)
    ]
    return {
        # https://dash.plot.ly/dash-core-components/graph
        'data': create_scatter_traces(dff, 'metric'),
        'layout': go.Layout(xaxis={'title': 'month'},
                            # see: https://plot.ly/javascript/reference/#layout-yaxis
                            yaxis={'title': 'value',
                                   'range': [0.75,1.25] if ylim == 'Fixed' else None,
                                   'dtick': 0.05 if ylim == 'Fixed' else None,
                                  },
                            title=f'price - {table} - {data_source} - {scenario} - {item} - {retailer}' if title == 'Yes' else None
                           )
    }

@app.callback(
    Output('g2', 'figure'),
    create_pickers(['table', 'data_source', 'item', 'retailer', 'metric', 'ylim', 'title'], 2),
)
def update_figure(table, data_source, item, retailer, metric, ylim, title):
    dff = df_ts[
        (df_ts['metric']!='price')&
        (df_ts['table']==table)&
        (df_ts['data_source']==data_source)&
        (df_ts['item']==item)&
        (df_ts['retailer']==retailer)&
        (df_ts['metric']==metric)
    ]
    return {
        'data': create_scatter_traces(dff, 'scenario'),
        'layout': go.Layout(xaxis={'title': 'month'},
                            yaxis={'title': 'value',
                                   'range': [0.75,1.25] if ylim == 'Fixed' else None,
                                   'dtick': 0.05 if ylim == 'Fixed' else None,
                                  },
                            title=f'{metric} - {table} - {data_source} - scenarios - {item} - {retailer}' if title == 'Yes' else None
                           )
    }

@app.callback(
    Output('g3', 'figure'),
    create_pickers(['table', 'scenario', 'data_source', 'item', 'metric', 'ylim', 'title'], 3),
)
def update_figure(table, scenario, data_source, item, metric, ylim, title):
    dff = df_ts[
        (df_ts['metric']!='price')&
        (df_ts['table']==table)&
        (df_ts['scenario']==scenario)&
        (df_ts['data_source']==data_source)&
        (df_ts['item']==item)&
        (df_ts['metric']==metric)
    ]
    return {
        'data': create_scatter_traces(dff, 'retailer'),
        'layout': go.Layout(xaxis={'title': 'month'},
                            yaxis={'title': 'value',
                                   'range': [0.75,1.25] if ylim == 'Fixed' else None,
                                   'dtick': 0.05 if ylim == 'Fixed' else None,
                                  },
                            title=f'{metric} - {table} - {data_source} - {scenario} - {item} - retailers' if title == 'Yes' else None
                           )
    }

# HISTOGRAMS

@app.callback(
    Output('g4', 'figure'),
    create_pickers(['data_source', 'item', 'retailer', 'scenario', 'title', 'distplot', 'dist_absrel'], 4),
)
def update_figure(data_source, item, retailer, scenario, title, distplot, dist_absrel):
    dff = df_dist[
        (df_dist['data_source']==data_source)&
        (df_dist['item']==item)&
        (df_dist['retailer']==retailer)&
        (df_dist['scenario']==scenario)
    ]
    if dist_absrel == 'Relative':
        sums_df = dff.groupby(['month'])['value'].sum().reset_index()
        dff = dff.merge(sums_df, on='month')
        dff['value'] = dff['value_x'].div(dff['value_y'])
        dff = dff.drop(['value_x', 'value_y'], axis=1)

    return {
        'data': create_histogram_traces(dff, 'month') if distplot=='Histogram' else create_frequency_traces(dff, 'month'),
        'layout': go.Layout(
            barmode='overlay' if distplot=='Histogram' else None,
            xaxis={'title': 'price'},
            yaxis={'title': 'frequency'},
            title=f'{data_source} - {item} - {retailer} - months - {scenario}' if title == 'Yes' else None
        )
    }


@app.callback(
    Output('g5', 'figure'),
    create_pickers(['data_source', 'item', 'month', 'scenario', 'title', 'distplot', 'dist_absrel'], 5),
)
def update_figure(data_source, item, month, scenario, title, distplot, dist_absrel):
    dff = df_dist[
        (df_dist['data_source']==data_source)&
        (df_dist['item']==item)&
        (df_dist['month']==month)&
        (df_dist['scenario']==scenario)
    ]
    if dist_absrel == 'Relative':
        sums_df = dff.groupby(['retailer'])['value'].sum().reset_index()
        dff = dff.merge(sums_df, on='retailer')
        dff['value'] = dff['value_x'].div(dff['value_y'])
        dff = dff.drop(['value_x', 'value_y'], axis=1)

    return {
        'data': create_histogram_traces(dff, 'retailer') if distplot=='Histogram' else create_frequency_traces(dff, 'retailer'),
        'layout': go.Layout(
            barmode='overlay' if distplot=='Histogram' else None,
            xaxis={'title': 'price'},
            yaxis={'title': 'frequency'},
            title=f'{data_source} - {item} - retailers - {month} - {scenario}' if title == 'Yes' else None
        )
    }

@app.callback(
    Output('g6', 'figure'),
    create_pickers(['data_source', 'item', 'retailer', 'month', 'title', 'distplot', 'dist_absrel'], 6),
)
def update_figure(data_source, item, retailer, month, title, distplot, dist_absrel):
    dff = df_dist[
        (df_dist['data_source']==data_source)&
        (df_dist['item']==item)&
        (df_dist['retailer']==retailer)&
        (df_dist['month']==month)
    ]
    if dist_absrel == 'Relative':
        sums_df = dff.groupby(['scenario'])['value'].sum().reset_index()
        dff = dff.merge(sums_df, on='scenario')
        dff['value'] = dff['value_x'].div(dff['value_y'])
        dff = dff.drop(['value_x', 'value_y'], axis=1)

    return {
        'data': create_histogram_traces(dff, 'scenario') if distplot=='Histogram' else create_frequency_traces(dff, 'scenario'),
        'layout': go.Layout(
            barmode='overlay' if distplot=='Histogram' else None,
            xaxis={'title': 'price'},
            yaxis={'title': 'frequency'},
            title=f'{data_source} - {item} - {retailer} - {month} - scenarios' if title == 'Yes' else None
        )
    }


if __name__ == '__main__':
    app.run_server(
        debug=True,
        port=int(os.environ.get("CDSW_PUBLIC_PORT")),
        host=os.environ.get("CDSW_IP_ADDRESS")
    )
