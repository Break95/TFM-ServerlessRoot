#!/usr/bin/env python3
import csv
import sys
import pandas as pd
import plotly.express as px


df = pd.read_csv(sys.argv[1], delimiter='|')
print(df)
for fun in df['function'].unique():

    for id in df['id'].unique():
        target_data = df[(df['function'] == fun) & (df['id'] == id)]

        cpu_fig = px.line(target_data,
                          x='time',
                          y='cpu_percent',
                          title=f'CPU usage of {fun} {id}')
        mem_fig = px.line(target_data,
                          x='time',
                          y='mem_percent',
                          title=f'Memory usage of {fun} {id}')

        cpu_fig.show()
        mem_fig.show()
