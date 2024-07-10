import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from bokeh.plotting import figure, output_file, show
from bokeh.layouts import column
from bokeh.models import ColumnDataSource, DataTable, TableColumn
from bokeh.io import curdoc
from bokeh.server.server import Server
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# PostgreSQL Configuration
POSTGRES_URL = "postgresql://postgres:1234@localhost:5432/ST2CBD"
POSTGRES_TABLE = "transactions_aggregated"

# Connect to PostgreSQL and fetch data
def fetch_data():
    try:
        engine = create_engine(POSTGRES_URL)
        query = f"SELECT * FROM {POSTGRES_TABLE}"
        df = pd.read_sql(query, engine)
        logger.info(f"Data fetched successfully with columns: {df.columns.tolist()}")
        return df
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        return pd.DataFrame()

# Fetch data
df = fetch_data()

# Verify if the required column exists
if 'TransactionNo' not in df.columns:
    logger.error("Required column 'TransactionNo' not found in the DataFrame")
    sys.exit(1)

# Prepare the data source for Bokeh
source = ColumnDataSource(df)

# Create a Bokeh DataTable
columns = [
    TableColumn(field="TransactionNo", title="Transaction No"),
    TableColumn(field="ProductNo", title="Product No"),
    TableColumn(field="ProductName", title="Product Name"),
    TableColumn(field="Price", title="Price"),
    TableColumn(field="Quantity", title="Quantity"),
    TableColumn(field="CustomerNo", title="Customer No"),
    TableColumn(field="Country", title="Country"),
    TableColumn(field="Date", title="Date"),
]
data_table = DataTable(source=source, columns=columns, width=800)

# Create a simple plot
p = figure(title="Transaction Prices", x_axis_label='TransactionNo', y_axis_label='Price', x_range=df["TransactionNo"].astype(str))
p.line(x='TransactionNo', y='Price', source=source, line_width=2)
p.circle(x='TransactionNo', y='Price', source=source, size=5, color="navy", alpha=0.5)

# Arrange plots and tables in a layout
layout = column(data_table, p)

# Add layout to the current document
curdoc().add_root(layout)

# Function to start the Bokeh server
def bkapp(doc):
    doc.add_root(layout)

# Start the Bokeh server
server = Server({'/': bkapp}, port=5006)
server.start()
logger.info("Bokeh app running on http://localhost:5006/")
server.io_loop.add_callback(server.show, "/")
server.io_loop.start()
