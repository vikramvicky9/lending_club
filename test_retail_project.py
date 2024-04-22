import pytest
from lib.Utils import get_spark_session
from lib.DataReader import read_customers,read_orders
from lib.DataManipulation import filter_closed_orders

def test_read_customers_df():
    spark=get_spark_session("LOCAL")
    customer_count=read_customers(spark,"LOCAL").count()
    assert customer_count==12435

def test_read_orders_df():
    spark=get_spark_session("LOCAL")
    orders_count=read_orders(spark,"LOCAL").count()
    assert orders_count==68884

def test_filter_closed_orders():
    spark=get_spark_session("LOCAL")
    orders_df=read_orders(spark,"LOCAL")
    filter_closed=filter_closed_orders(orders_df).count()
    assert filter_closed==7556

