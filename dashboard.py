import random
import pandas as pd
from bokeh.plotting import curdoc, figure
from bokeh.models import ColumnDataSource, DatetimeTickFormatter
from bokeh.models.widgets import Div
from bokeh.layouts import column, row
import json
import ast
import time
from kafka import KafkaConsumer

# Kafka Configuration
consumer = KafkaConsumer(
    'transactions',  # Your Kafka topic
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Bokeh Configuration
UPDATE_INTERVAL = 1000
ROLLOVER = 10

source = ColumnDataSource({"x": [], "y": []})
div = Div(text='', width=120, height=35)

# Function to update data from Kafka
def update():
    for msg in consumer:
        value = msg.value
        date_str = value['Date']  # Extract date as string
        x = pd.to_datetime(date_str)  # Convert to Pandas datetime
        y = value['Price']       # Extract price

        div.text = f"Date: {x.strftime('%Y-%m-%d')}"

        source.stream({"x": [x], "y": [y]}, ROLLOVER)
        break  # Process only one message per update

# Bokeh Plot Configuration
p = figure(
    title="E-commerce Transactions",
    x_axis_type="datetime",
    width=1000,  # Use 'width' instead of 'plot_width'
    y_axis_label="Price"
)

# Customize appearance
p.line("x", "y", source=source)
p.xaxis.formatter = DatetimeTickFormatter(
    days="%Y-%m-%d", months="%Y-%m-%d", years="%Y-%m-%d"
)

p.xaxis.major_label_orientation = "vertical"
p.title.align = "right"
p.title.text_color = "orange"
p.title.text_font_size = "25px"

# Create the Bokeh Document
doc = curdoc()
doc.add_root(row(children=[div, p]))
doc.add_periodic_callback(update, UPDATE_INTERVAL)
