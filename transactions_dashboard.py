import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from bokeh.plotting import figure, output_file, show
from bokeh.layouts import column
from bokeh.models import ColumnDataSource, DataTable, TableColumn
from bokeh.io import curdoc
from bokeh.server.server import Server
import logging
import sys

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# PostgreSQL Configuration
POSTGRES_URL = "postgresql://postgres:1234@localhost:5432/ST2CBD"
POSTGRES_TABLE = "transactions_aggregated"


# Connect to PostgreSQL and fetch data
def fetch_data(queries):
    try:
        engine = create_engine(POSTGRES_URL)
        query = queries
        df = pd.read_sql(query, engine)
        logger.info(f"Data fetched successfully with columns: {df.columns.tolist()}")
        return df
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        return pd.DataFrame()

query = f"SELECT * FROM {POSTGRES_TABLE}"

# Fetch data
df = fetch_data(query)

# Verify if the required column exists
if 'transactionno' not in df.columns:
    logger.error("Required column 'transactionno' not found in the DataFrame")
    sys.exit(1)

# Prepare the data source for Bokeh
source = ColumnDataSource(df)

# Create a Bokeh DataTable
columns = [
    TableColumn(field="transactionno", title="Transaction No"),
    TableColumn(field="productno", title="Product No"),
    TableColumn(field="productname", title="Product Name"),
    TableColumn(field="price", title="Price"),
    TableColumn(field="quantity", title="Quantity"),
    TableColumn(field="customerno", title="Customer No"),
    TableColumn(field="country", title="Country"),
    TableColumn(field="date", title="Date"),
]
data_table = DataTable(source=source, columns=columns, width=800)

# Create a simple plot
p = figure(title="Transaction Prices", x_axis_label='Transaction No', y_axis_label='Price',
           x_range=df["transactionno"].astype(str))
p.line(x='transactionno', y='price', source=source, line_width=2)
p.circle(x='transactionno', y='price', source=source, size=5, color="navy", alpha=0.5)

# Arrange plots and tables in a layout
layout = column(data_table, p)

# Add layout to the current document
curdoc().add_root(layout)

# Fetch aggregated data for total price by date
aggregated_query = f"""
SELECT Country as countries, SUM(price) as total_price
FROM {POSTGRES_TABLE}
GROUP BY Country
ORDER BY Country
"""
aggregated_df = fetch_data(aggregated_query)
print(aggregated_df)
# Prepare the data source for Bokeh
aggregated_source = ColumnDataSource(aggregated_df)

# Create a plot for total quantity sold by product
p_aggregated = figure(title="Total Price by Country", x_axis_label='Country', y_axis_label='Total Price',
                      x_range=aggregated_df["countries"])
p_aggregated.vbar(x='countries', top='total_price', source=aggregated_source, width=0.9)

# Arrange plots and tables in a layout
layout_aggreg = column(p_aggregated)

# Add layout to the current document
curdoc().add_root(layout_aggreg)



# Function to start the Bokeh server
def bkapp(doc):
    doc.add_root(layout)



# Start the Bokeh server
server = Server({'/': bkapp}, port=5006)
server.start()
logger.info("Bokeh app running on http://localhost:5006/")
server.io_loop.add_callback(server.show, "/")
server.io_loop.start()
