from pyspark.sql.functions import *

# Filter closed orders
def filter_closed_orders(orders_df):
    return orders_df.filter("order_status='CLOSED'")

# Join orders and customers
def join_orders_customers(orders_df, customers_df):
    return orders_df.join(customers_df, "customer_id")

# Count orders by state
def count_orders_state(joined_df):
    return joined_df.groupBy('state','city').count()
