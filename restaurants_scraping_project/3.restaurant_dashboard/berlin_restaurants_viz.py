import pandas as pd
import plotly.express as px
import dash
from dash import dash_table
from dash import dcc
from dash import html


app = dash.Dash(__name__)

df = pd.read_csv('scraped_data/quandoo_berlin_restaurants.csv')

# Scatter Plot for Comparing Restaurant Cuisine and its Average Reviews
df_a = (
    df.groupby('Restaurant_cuisine', as_index=False)
    .agg(Mean_Number_of_Reviews=('Number_of_reviews', 'mean'))
    .sort_values('Mean_Number_of_Reviews', ascending=False)
    .head(10)
)

scatter_figure_1 = px.scatter(
    df_a,
    x='Restaurant_cuisine',
    y='Mean_Number_of_Reviews',
    size='Mean_Number_of_Reviews',
    color='Restaurant_cuisine',
    size_max=30,
    template='plotly_dark',
    title='Average Reviews for Each Cuisine',
)
scatter_figure_1.update_layout(width=1000, height=500, title_x=0.5)
scatter_figure_1.update_xaxes(title='Restaurant Cuisines', showticklabels=True)
scatter_figure_1.update_yaxes(title='Mean Number of Reviews', showticklabels=True)

# Scatter Plot for Restaurant Location and Average Reviews
df_b = (
    df.groupby('Restaurant_location', as_index=False)
    .agg(Mean_Number_of_Reviews=('Number_of_reviews', 'mean'))
    .sort_values('Mean_Number_of_Reviews', ascending=False)
    .head(10)
)

scatter_figure_2 = px.scatter(
    df_b,
    x='Restaurant_location',
    y='Mean_Number_of_Reviews',
    size='Mean_Number_of_Reviews',
    color='Restaurant_location',
    size_max=30,
    template='plotly_dark',
    title='Average Reviews of Restaurants in Each Locality',
)
scatter_figure_2.update_layout(width=1000, height=500, title_x=0.5)
scatter_figure_2.update_xaxes(title='Restaurant Location', showticklabels=True)
scatter_figure_2.update_yaxes(title='Mean Number of Reviews', showticklabels=True)

# Bar plot for Number of Restaurants in Each location

df_c = (
    df.groupby('Restaurant_location', as_index=False)
    .agg(Number_of_restaurants=('Restaurant_location', 'size'))
    .sort_values(by='Number_of_restaurants', ascending=False)
    .head(10)
)

bar_plot_1 = px.bar(
    df_c,
    x='Restaurant_location',
    y='Number_of_restaurants',
    template='plotly_dark',
    color='Restaurant_location',
    title='Number of Restaurants in Each Location',
)
bar_plot_1.update_layout(title_x=0.5)
bar_plot_1.update_xaxes(title='Restaurant Location', showticklabels=True)
bar_plot_1.update_yaxes(title='Number of Restaurants', showticklabels=True)


app.layout = html.Div(
    [
        html.H2('Quandoo Restaurants in Berlin', style={'textAlign': 'center'}),
        html.Hr(),
        html.Div(
            [
                dash_table.DataTable(
                    id='datatable_id',
                    data=df.to_dict('records'),
                    columns=[
                        {'name': i, 'id': i, 'deletable': False, 'selectable': False}
                        for i in df.columns
                    ],
                    editable=False,
                    filter_action='native',
                    sort_action='native',
                    sort_mode='multi',
                    row_deletable=False,
                    selected_rows=[],
                    page_action='none',
                    style_cell={'whiteSpace': 'normal'},
                    fixed_rows={'headers': True, 'data': 0},
                    virtualization=False,
                    style_cell_conditional=[
                        {
                            'if': {'column_id': 'Restaurant_name'},
                            'width': '25%',
                            'textAlign': 'left',
                        },
                        {
                            'if': {'column_id': 'Restaurant_location'},
                            'width': '25%',
                            'textAlign': 'left',
                        },
                        {
                            'if': {'column_id': 'Restaurant_cuisine'},
                            'width': '25%',
                            'textAlign': 'left',
                        },
                        {
                            'if': {'column_id': 'Restaurant_score'},
                            'width': '10%',
                            'textAlign': 'center',
                        },
                        {
                            'if': {'column_id': 'Number_of_reviews'},
                            'width': '10%',
                            'textAlign': 'center',
                        },
                    ],
                ),
            ],
            className='row',
        ),
        html.Br(),
        html.Br(),
        html.Br(),
        html.Div(
            style={'display': 'flex', 'width': '100%'},
            children=[
                dcc.Graph(id='scatterplot_1', figure=scatter_figure_1,),
                dcc.Graph(id='scatterplot_2', figure=scatter_figure_2,),
            ],
        ),
        html.Br(),
        html.Div([dcc.Graph(id='barplot_1', figure=bar_plot_1)]),
    ]
)


if __name__ == '__main__':
    app.run_server(debug=True, host="0.0.0.0", port=9000)
