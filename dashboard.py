from flask import Flask, render_template
from bokeh.plotting import figure
from bokeh.embed import components
from bokeh.resources import CDN
from bokeh.models import ColumnDataSource, HoverTool
from pymongo import MongoClient

app = Flask(__name__)


@app.route('/')
def index():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["natone_db"]
    collection = db["transactions"]

    # Fetch data from MongoDB
    transactions = list(collection.find())

    # Prepare data for Bokeh
    source = ColumnDataSource(data={
        'TransactionNo': [t['TransactionNo'] for t in transactions],
        'Date': [t['Date'] for t in transactions],
        'Price': [t['Price'] for t in transactions],
        'Quantity': [t['Quantity'] for t in transactions],
        'CustomerNo': [t['CustomerNo'] for t in transactions],
        'Country': [t['Country'] for t in transactions]
    })

    # Create a Bokeh plot
    p = figure(title="NatOne Transactions", x_axis_label="Transaction No", y_axis_label="Price")
    p.scatter(x='TransactionNo', y='Price', source=source, size=10, color="blue", alpha=0.5)  # Use scatter()

    # Add hover tool
    hover = HoverTool(tooltips=[
        ("Transaction No", "@TransactionNo"),
        ("Date", "@Date"),
        ("Price", "@Price"),
        ("Quantity", "@Quantity"),
        ("CustomerNo", "@CustomerNo"),
        ("Country", "@Country")
    ])
    p.add_tools(hover)

    # Embed Bokeh plot in HTML
    script, div = components(p)
    cdn_js = CDN.js_files
    cdn_css = CDN.css_files

    return render_template('index.html', script=script, div=div, cdn_js=cdn_js, cdn_css=cdn_css)


if __name__ == '__main__':
    app.run(debug=True)