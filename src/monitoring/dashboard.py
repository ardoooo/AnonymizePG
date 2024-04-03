import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import plotly.graph_objects as go

from src.monitoring import metrics


metrics = metrics.MetricsCollector(
    db_path="/home/ardooo/learning/diplom/metrics/metrics.db"
)


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
        dbc.Row([dbc.Col(dcc.Graph(id="metric3-graph"), width=12)], className="mt-2"),
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
    metric_names = ["total_mark_processed", "total_deleted"]
    fig = go.Figure()
    for name in metric_names:
        timestamps, values = metrics.get_metric(name)
        fig.add_trace(go.Scatter(x=timestamps, y=values, mode="lines", name=name))

    fig.update_layout(
        title="Трансферные метрики",
        template="plotly_dark",
        title_font_color="#FFA07A",
        font_color="#FFA07A",
        hovermode="x unified",
    )
    return fig


@app.callback(
    Output("metric3-graph", "figure"),
    [Input("update-button", "n_clicks")],
    [Input("interval-component", "n_intervals")],
)
def update_metric3_graph(n_clicks, n_intervals):
    timestamps, values = metrics.get_metric("batch_time_execution_s")
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(x=timestamps, y=values, mode="lines", name="batch_time_execution_s")
    )

    fig.update_layout(
        title="Время обработки одного батча",
        yaxis_title="Значение",
        template="plotly_dark",
        title_font_color="#FFA07A",
        font_color="#FFA07A",
        hovermode="x unified",
    )
    return fig


if __name__ == "__main__":
    app.run_server()
