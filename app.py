from dash import Dash, html, dcc, Input, Output
import pandas as pd
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import numpy as np
import dash_bootstrap_components as dbc
from kafka import KafkaConsumer
from json import loads
from river import compose
from river import preprocessing
from river import metrics
from river import cluster

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css', dbc.themes.BOOTSTRAP]

app = Dash(__name__, external_stylesheets=external_stylesheets)

activePage = 1
maxValue = 1
clusters = 4
spaceWarsEvents = []
LABELS = {0: 'Casual Killer', 1: 'Casual Achiever', 2:'Hardcore Achiever', 3:'Hardcore Killer'}

model = compose.Pipeline(
    preprocessing.StandardScaler(),
    cluster.KMeans(n_clusters=clusters, seed=42)
)

consumer = KafkaConsumer(
    'space-wars-events',
    bootstrap_servers=['localhost:29092'],
    value_deserializer=lambda x: loads(x),
    auto_offset_reset='earliest'
)

def riverPredict(data):
    global model
    pred = model.predict_one(data)
    model = model.learn_one(data)
    return pred

def getConsumer():
    try:
        print("Start Consumer !!!")
        for message in consumer:
            if message:
                key = message.key.decode('utf-8')
                value = message.value['data']

                group = riverPredict(value)
                player = message.value['player']
                
                value['GROUP'] = group
                value['NAME'] = player
                value['LABEL'] = LABELS.get(group)
       
                spaceWarsEvents.append(value)
    except Exception as e: 
        print(f"[CUNSUMER-ERROR]: {e}")

def calLabels():
    global LABELS
    if len(spaceWarsEvents) > 0:
        df = pd.json_normalize(spaceWarsEvents)
        df = df.fillna(0)
        mean_collected_coin = list(df.groupby(['GROUP']).mean()['A2'])
        mean_enemies_kills = list(df.groupby(['GROUP']).mean()['A3'])
        # print(f'mean_enemies_kills: {mean_enemies_kills} mean_collected_coin: {mean_collected_coin}')

        labels = []
        num_labels = len(mean_collected_coin)
        for i in range(num_labels):
            if i % 2 == 0:
                sorted_index = np.argsort(mean_collected_coin)
            else:
                sorted_index = np.argsort(mean_enemies_kills)

            sorted_index = list(sorted_index)
            sorted_index.reverse()
            for max_index in sorted_index:
                if max_index not in labels:
                    labels.append(max_index)
                    break
        # print(f"labels: {labels} === {clusters}")          
        if len(labels) == clusters:
            LABELS = {
                labels[0]: 'Hardcore Achiever',
                labels[1]: 'Hardcore Killer',
                labels[2]: 'Casual Achiever',
                labels[3]: 'Casual Killer',
            }
            # print(LABELS)

@app.callback(
    Output('graph1-data', 'figure'),
    [Input('interval-component', 'n_intervals')]
)
def update_graph1(n):
    try:
        graph = make_subplots(rows=1, cols=1, specs=[[{}]], shared_xaxes=True, shared_yaxes=False, vertical_spacing=0.001)
        graph.update_layout(
            title="[Graph 1] Scatter plot จุดที่ผู้เล่นตายใน Map",
            xaxis=dict(
                title="Position in X axis"
            ),
            yaxis=dict(
                title="Position in Y axis"
            ),
        )
        graph.update_xaxes(range=[0, 800])
        graph.update_yaxes(range=[0, 600])

        if len(spaceWarsEvents) > 0:
            df = pd.json_normalize(spaceWarsEvents)

            graph.append_trace(go.Scatter(
                x=df["A0"],
                y=df["A1"],
                mode='markers'
            ), 1, 1)
    except Exception as e: 
        print(f"[GRAPH1-ERROR]: {e}")
    return graph

@app.callback(
    Output('graph2-data', 'figure'),
    [Input('interval-component', 'n_intervals')]
)
def update_graph2(n):
    try:
        graph = make_subplots(rows=1, cols=1, specs=[[{}]], shared_xaxes=True, shared_yaxes=False, vertical_spacing=0.001)
        graph.update_layout(
            title="[Graph 2] กราฟแสดง Total Shot (a4), Enermy kill (a3), Accuracy (a3/a4)",
        )

        if len(spaceWarsEvents) > 0:
            df = pd.json_normalize(spaceWarsEvents)

            enemyKill = df.loc[:,["A3"]]
            totalShot = df.loc[:,["A4"]]

            enemyKill = enemyKill.sum().values[0]
            totalShot = totalShot.sum().values[0]
            acc = int(enemyKill / totalShot * 100)

            graph.append_trace(go.Bar(x=["Total Shot", "Enemy kill", "Accuracy"], y=[totalShot, enemyKill, acc] , text=[totalShot, enemyKill, str(acc) + "%"], textposition='auto',), 1, 1)
            graph.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)

    except Exception as e: 
        print(f"[GRAPH2-ERROR]: {e}")
    return graph

@app.callback(
    Output('graph3-data', 'figure'),
    [Input('interval-component', 'n_intervals')]
)
def update_graph3(n):
    try:
        global LABELS
        graph = make_subplots(rows=1, cols=1, specs=[[{}]], shared_xaxes=True, shared_yaxes=False, vertical_spacing=0.001)
        graph.update_layout(
            title="[Graph3] Bar กราฟแสดง type ของผู้เล่นที่ Predict ออกมาได้​",
        )
            
        if len(spaceWarsEvents) > 0:
            df = pd.json_normalize(spaceWarsEvents)
            df = df.loc[:,["GROUP", "LABEL"]]
            df["COUNT"] = 0
            df = df.groupby(["GROUP", "LABEL"]).count().reset_index()
          
            graph.append_trace(go.Bar(x=df["LABEL"], y=df["COUNT"].to_list(), text=df["COUNT"].to_list(), textposition='auto'), 1, 1)
            graph.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
           
    except Exception as e: 
        print(f"[GRAPH3-ERROR]: {e}")
    return graph
    
@app.callback(
    Output('data-table-id', 'children'),
    [Input('interval-component', 'n_intervals')]
)
def update_data(n):
    if n == 0:
        getConsumer()
    # if n % 5 == 0:
    # calLabels()
    table_header = [
        html.Thead(html.Tr([
            html.Th("Player"),
            html.Th("Group"),
            html.Th("Label"),
            html.Th("Position in X axis"),
            html.Th("Position in Y axis"),
            html.Th("Number of coins collected"),
            html.Th("Number of destroyed enemies"),
            html.Th("Number of shots"),
            html.Th("Number of shots without enemies"),
            html.Th("Level reach"),
            html.Th("Key X pressed count"),
            html.Th("Key Y pressed count"),
            html.Th("Number of enemy created"),
            html.Th("Number of coin created"),
        ])),
    ]

    table_data = []
    startIndex = (activePage - 1) * 10
    endIndex = startIndex + 9
    if len(spaceWarsEvents) > 0:
        df = pd.json_normalize(spaceWarsEvents).loc[startIndex:endIndex]
    
        for i in df.to_dict('records'):
            table_data.append(
                html.Tr([
                    html.Td(i["NAME"]),
                    html.Td(i["GROUP"]),
                    html.Td(i["LABEL"]),
                    html.Td(i["A0"]),
                    html.Td(i["A1"]),
                    html.Td(i["A2"]),
                    html.Td(i["A3"]),
                    html.Td(i["A4"]),
                    html.Td(i["A5"]),
                    html.Td(i["A6"]),
                    html.Td(i["A7"]),
                    html.Td(i["A8"]),
                    html.Td(i["A9"]),
                    html.Td(i["A10"]),
                ])
            )

    table_body = [html.Tbody(table_data)]

    table = dbc.Table(table_header + table_body, bordered=True)
    if len(spaceWarsEvents) % 10 == 0:
        maxValue = int(len(spaceWarsEvents) / 10)
    else:
        maxValue = int(len(spaceWarsEvents) / 10) + 1
    pagin = dbc.Pagination(id="pagin-id", active_page=activePage, max_value=maxValue, fully_expanded=False)
    div = html.Div( 
            className = "box-centent-wrapper table-wrapper",
            children=[table, pagin],
        )
    return div

@app.callback(
    Output("pagin-id", "active_page"),
    [Input("pagin-id", "active_page")],
)
def click_pagin(value):
    global activePage
    activePage = value
    return activePage

app.layout = html.Div(children=[
    dcc.Interval(id='interval-component', interval=1*1000, n_intervals=0),
    html.Div(
        className = "centent-wrapper",
        children = [
            dbc.Card(
                dbc.CardBody(
                    [
                        html.Div(
                            className = "box-centent-wrapper",
                            children=[
                                dcc.Graph(
                                    id='graph1-data',
                                ),
                                dcc.Graph(
                                    id='graph2-data',
                                ),
                            ]
                        ),
                        html.Div(
                            className = "box-centent-wrapper",
                            children=[
                                # dcc.Graph(
                                #     id='graph2-data',
                                # ),
                            ],
                        ),
                        html.Div(
                            className = "box-centent-wrapper",
                            children=[
                                dcc.Graph(
                                    id='graph3-data',
                                ),             
                            ],
                        ),
                    ]
                )
            )
        ]
    ),
    html.Div(
        className = "centent-wrapper",
        children = [
            dbc.Card(
                dbc.CardBody(
                    [
                        html.Div(
                            className = "box-centent-wrapper",
                            children=[
                                html.H1("Descriptions", style = { "flex": "1" }),
                              
                            ],
                            style = { "height": "auto" }
                        ),
                        html.Div(
                            id="data-table-id",
                            className = "box-centent-wrapper",
                            children=[
                            ]
                        ),
                    ]
                )
            )
        ]
    ),
], style = { "padding": "1rem", "backgroundColor": "whitesmoke",  "height": "100vh"})

if __name__ == '__main__':
    app.run_server(debug=True)