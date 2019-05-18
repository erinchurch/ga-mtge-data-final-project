import pandas as pd
import plotly
import plotly.plotly as py
from plotly.offline import iplot, init_notebook_mode
import plotly.graph_objs as go
import plotly.io as pio
import glob
import numpy as np


def table_vintage(df, x, y, z, fname):
    drop = df.columns.tolist()
    drop.remove(x) #reporting field, first column of table, numeric or non numeric
    drop.remove(y) #second table field
    drop.remove(z)  #thrid table field
    df1 = df.drop(drop, axis=1)  #remove unneeded fields
    df2 = df1.sort_values(by=[x])  #sort the first column data acending

    trace = go.Table(
        header=dict(values=list(df2.columns),
                    fill=dict(color='#C2D4FF'),
                    align=['left'] * 5),
        cells=dict(values=[df2.VINTAGE_YEAR, df2.Total_UPB, df2.Total_Loan_Count],
                   fill=dict(color='#F5F8FF'),
                   align=['left'] * 5))
    layout = dict(width=900, height=600)
    data = [trace]
    fig = dict(data=data, layout=layout)
    py.iplot(fig, filename='styled_table')
    path1 = 'images/' + fname + '.jpg'
    print(path1)
    path2 = 'images/' + fname + '.pdf'
    print(path2)
    path3 = 'images/' + fname + '.png'
    print(path3)
    pio.write_image(fig, path1)
    pio.write_image(fig, path2)
    pio.write_image(fig, path3)
    return


def bubble_chart(df, x, y, fname):
    n = 100
    x = df[x]
    y = df[y]
    colors = np.random.rand(n)
    sz = np.random.rand(n) * 30

    fig = go.Figure()
    fig.add_scatter(x=x,
                    y=y,
                    mode='markers',
                    marker={'size': sz,
                            'color': colors,
                            'opacity': 0.6,
                            'colorscale': 'Viridis'
                           })
    iplot(fig)
    path1 = 'images/'+fname+'.jpg'
    print(path1)
    path2 = 'images/'+fname+'.pdf'
    print(path2)
    path3 = 'images/'+fname+'.png'
    print(path3)
    pio.write_image(fig, path1)
    pio.write_image(fig, path2)
    pio.write_image(fig, path3)
    return

def multi_line_scatter_chart(df, x, y0, y1, y2, y0_title, y1_title, y2_title, fname):
    x = (df[x])  #'VINTAGE_YEAR'
    y0 = df[y0] / 100000000  #'Total_UPB'
    y1 = df[y1] / 1000  #'Avg UPB'
    y2 = df[y2] / 1000  #'Total_Loan_Count'

    # Create traces
    trace0 = go.Scatter(
        x=x,
        y=y0,
        mode='markers',
        name= y0_title #'Total Balance ($BB)'
    )
    trace1 = go.Scatter(
        x=x,
        y=y1,
        mode='markers',
        name=y1_title #'Avg Balance ($k)'
    )
    trace2 = go.Scatter(
        x=x,
        y=y2,
        mode='markers',
        name=y2_title #'Total Loan Count (k)'
    )
    data = [trace0, trace1, trace2]
    # py.iplot(data, filename='line-mode')
    path1 = 'images/' + fname + '.jpg'
    print(path1)
    path2 = 'images/' + fname + '.pdf'
    print(path2)
    path3 = 'images/' + fname + '.png'
    print(path3)
    pio.write_image(data, path1)
    pio.write_image(data, path2)
    pio.write_image(data, path3)
    return

def map_chart(df, x, y, title,fname):
    df.head()
    br_tag = "'<br>"+y+"'"
    df['text'] = df[x] + br_tag + (df[y] / 1e9).astype(str) + ' billion'
    limits = [(0, 2), (3, 10), (11, 20), (21, 50), (50, 3000)]
    colors = ["rgb(0,116,217)", "rgb(255,65,54)", "rgb(133,20,75)", "rgb(255,133,27)", "lightgrey"]
    cities = []
    scale = 10000000

    for i in range(len(limits)):
        lim = limits[i]
        df_sub = df[lim[0]:lim[1]]
        city = go.Scattergeo(
            locationmode='USA-states',
            lon=df_sub['longitude'],
            lat=df_sub['latitude'],
            text=df_sub['text'],
            marker=go.scattergeo.Marker(
                size=df_sub[y] / scale,
                color=colors[i],
                line=go.scattergeo.marker.Line(
                    width=0.5, color='rgb(40,40,40)'
                ),
                sizemode='area'
            ),
            name='{0} - {1}'.format(lim[0], lim[1]))
        cities.append(city)

    layout = go.Layout(
        title=go.layout.Title(
            text=title
        ),
        showlegend=True,
        geo=go.layout.Geo(
            scope='usa',
            projection=go.layout.geo.Projection(
                type='albers usa'
            ),
            showland=True,
            landcolor='rgb(217, 217, 217)',
            subunitwidth=1,
            countrywidth=1,
            subunitcolor="rgb(255, 255, 255)",
            countrycolor="rgb(255, 255, 255)"
        )
    )
    map = go.Figure(data=cities, layout=layout)
    # py.iplot(map, filename='d3-bubble-map-upb-exposure-by-state')
    path1 = 'images/' + fname + '.jpg'
    print(path1)
    path2 = 'images/' + fname + '.pdf'
    print(path2)
    path3 = 'images/' + fname + '.png'
    print(path3)
    pio.write_image(map, path1)
    pio.write_image(map, path2)
    pio.write_image(map, path3)
    return

def state_charts():
    files_state = glob.glob('groupby_data/state_bucket*')
    dfs = []
    for file in files_state:
        df = pd.read_csv(file)
        print(df)
        dfs.append(df)
    df_all = pd.concat(dfs, axis=0, ignore_index=True)
    df_lat_long = pd.read_csv('state.csv')
    df_state = pd.concat([df_all, df_lat_long], axis=1, ignore_index=False,)
    a = map_chart(df_state, 'PROPERTY STATE', 'Total_UPB','FNMA Mortgage Exposure By State', 'demo_state_upb')
    return

def upb_charts():
    files_upb = glob.glob('groupby_data/upb_bucket*')
    dfs = []
    for file in files_upb:
        # name = file.replace('draft_groupby_data/', '')
        # print(name)
        df = pd.read_csv(file)
        # df['SOURCE'] = name
        print(df)
        dfs.append(df)
    df_all = pd.concat(dfs, axis=0, ignore_index=True)
    a = bubble_chart(df_all, 'UPB_BUCKET','LTV WA', 'demo_upb_ltv' )
    return

def vintage_chart():
    files_state = glob.glob('groupby_data/vintage_bucket*')
    dfs = []
    for file in files_state:
        df = pd.read_csv(file)
        print(df)
        dfs.append(df)
    df_all = pd.concat(dfs, axis=0, ignore_index=True)
    #multi_line_scatter_chart(df_all, 'VINTAGE_YEAR','Total_UPB','Avg UPB','Total_Loan_Count', 'Total Balance ($BB)','Avg Balance ($k)','Total Loan Count (k)',"demo-vintage-upb-lncount")
    table_vintage(df_all, 'VINTAGE_YEAR','Total_UPB','Total_Loan_Count',"demo-vintage-table" )

def main():
    plotly.tools.set_credentials_file(username='erinchurch6', api_key='fXwAuCrzutNF2zWbXAj1')
    upb_charts()
    state_charts()
    vintage_chart()


if __name__ == '__main__':
    main()

