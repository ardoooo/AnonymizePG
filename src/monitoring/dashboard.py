import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import pandas as pd

from src.monitoring import metrics


metrics = metrics.MetricsCollector(
    db_path="/home/ardooo/learning/diplom/metrics/metrics.db"
)


def interpolate_and_difference(ts1, ts2):
    df1 = pd.DataFrame({"timestamp": ts1[0], "values": ts1[1]})
    df2 = pd.DataFrame({"timestamp": ts2[0], "values": ts2[1]})

    df1.set_index(pd.to_datetime(df1["timestamp"]), inplace=True)
    df2.set_index(pd.to_datetime(df2["timestamp"]), inplace=True)

    df1_resampled = df1.resample("1s").agg({"values": "max"})
    df2_resampled = df2.resample("1s").agg({"values": "max"})

    merged_df = pd.merge(
        df1_resampled,
        df2_resampled,
        left_index=True,
        right_index=True,
        suffixes=("_1", "_2"),
        how="outer",
    )
    merged_df.interpolate(inplace=True)
    merged_df = pd.concat(
        [
            pd.DataFrame(
                {"values_1": [0], "values_2": [0]},
                index=[merged_df.index.min() - pd.Timedelta(seconds=1)],
            ),
            merged_df,
        ]
    )
    merged_df.fillna(0, inplace=True)

    difference = merged_df["values_1"] - merged_df["values_2"]

    return difference


app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])
app.title = "Dashboard"

app.layout = dbc.Container(
    [
        dbc.Row(
            [
                dbc.Col(
                    html.H1("Метрики AnonymizePG", style={"color": "#FFA07A"}),
                    align="center",
                    style={
                        "display": "flex",
                        "alignItems": "center",
                        "justifyContent": "center",
                    },
                    width=6,
                ),
                dbc.Col(
                    [
                        dbc.InputGroup(
                            [
                                dbc.InputGroupText(
                                    "Обновлять через:",
                                    style={"color": "#FFA07A"},
                                ),
                                dbc.Input(
                                    id="input-update-interval",
                                    type="text",
                                    value="1m",
                                    style={
                                        "color": "#FFA07A",
                                        "backgroundColor": "#555555",
                                    },
                                ),
                                dbc.Button(
                                    "Обновить",
                                    id="update-button",
                                    n_clicks=0,
                                    className="ms-2",
                                ),
                            ],
                            style={"maxWidth": "400px"},
                        ),
                    ],
                    width=6,
                    style={
                        "display": "flex",
                        "alignItems": "center",
                        "justifyContent": "flex-end",
                    },
                ),
            ],
            justify="between",
        ),
        dcc.Interval(id="interval-component", interval=60 * 1000, n_intervals=0),
        dbc.Tooltip(
            "например, 30s или 2m, или чтобы отключить 0/0s/0m",
            target="input-update-interval",
            placement="bottom",
        ),
        dbc.Row([dbc.Col(dcc.Graph(id="metrics-graph"), width=12)], className="mt-2"),
        dbc.Row(
            [dbc.Col(dcc.Graph(id="batch-time-graph"), width=12)], className="mt-2"
        ),
        dcc.Store(id="hosts-store", data=[]),
        html.Div(id="graphs-container"),
    ],
    fluid=True,
    style={"padding": "20px"},
)


@app.callback(
    Output("interval-component", "interval"),
    Output("interval-component", "disabled"),
    [Input("input-update-interval", "value")],
)
def update_interval(input_value):
    if not input_value:
        return 60 * 1000, False

    if input_value in ["0", "0s", "0m"]:
        return dash.no_update, True

    try:
        if input_value[-1] == "s":
            interval = int(input_value[:-1]) * 1000
        elif input_value[-1] == "m":
            interval = int(input_value[:-1]) * 60 * 1000
        else:
            return 60 * 1000, False
    except ValueError:
        return 60 * 1000, False

    return interval, False


@app.callback(
    Output("metrics-graph", "figure"),
    [Input("update-button", "n_clicks")],
    [Input("interval-component", "n_intervals")],
)
def update_graph(n_clicks, n_intervals):
    ts1 = metrics.get_metric_by_name("total_mark_processed")
    ts2 = metrics.get_metric_by_name("total_deleted")

    difference = interpolate_and_difference(ts1, ts2)

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=difference.index,
            y=difference,
            mode="lines",
            name="Количество строк в трансферной таблице",
        )
    )

    fig.update_layout(
        title="Количество записей в промежуточной таблице",
        title_font_size=22,
        template="plotly_dark",
        title_font_color="#FFA07A",
        font_color="#FFA07A",
        yaxis_title="Количество",
        yaxis_title_font_size=18,
        hovermode="x unified",
    )
    return fig


@app.callback(
    Output("batch-time-graph", "figure"),
    [Input("update-button", "n_clicks")],
    [Input("interval-component", "n_intervals")],
)
def update_batch_time_graph(n_clicks, n_intervals):
    timestamps, values = metrics.get_metric_by_name("batch_time_execution_s")
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(x=timestamps, y=values, mode="lines", name="batch_time_execution_s")
    )

    fig.update_layout(
        title="Время обработки одного батча",
        title_font_size=22,
        yaxis_title="Время",
        yaxis_title_font_size=18,
        template="plotly_dark",
        title_font_color="#FFA07A",
        font_color="#FFA07A",
        hovermode="x unified",
    )
    return fig


@app.callback(
    Output("hosts-store", "data"),
    [Input("update-button", "n_clicks")],
    [Input("interval-component", "n_intervals")],
)
def update_hosts(n_clicks, n_intervals):
    hosts = metrics.get_hosts()
    return hosts


@app.callback(
    Output("graphs-container", "children"),
    [Input("hosts-store", "data")],
)
def update_graphs(hosts):
    rows = []
    for host in hosts:
        timestamps, values = metrics.get_metric_by_tag_and_name(host, "total_cnt")
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=timestamps, y=values, mode="lines"))
        fig.update_layout(
            title=f"Динамика передачи записей {host}",
            title_font_size=22,
            yaxis_title="Количество",
            yaxis_title_font_size=18,
            template="plotly_dark",
            title_font_color="#FFA07A",
            font_color="#FFA07A",
            hovermode="x unified",
        )
        graph = dcc.Graph(figure=fig)
        row = dbc.Row(
            [dbc.Col(graph, width=12)],
            className="my-2",
        )
        rows.append(row)
    return rows


if __name__ == "__main__":
    app.run_server()
