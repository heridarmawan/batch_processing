#!/usr/bin/python3

import os
import json
import sqlparse

import pandas as pd
import numpy as np

import connection
import conn_warehouse
import conn_lake

if __name__ == '__main__':
    path_query = os.getcwd()+'/query/'

    # connection to lake
    print(f"[INFO] Service ETL is Starting connection Lake .....")
    conn_lake, engine_lake = conn_lake.conn()
    cursor_lake = conn_lake.cursor()

    conf = connection.config('postgresql')
    conn, engine = connection.psql_conn(conf)
    cursor_lake = conn.cursor()

    # # create schema in lake
    query_schema_lake = sqlparse.format(
        open(
            path_query+'schema.sql', 'r'
        ).read(), strip_comments=True).strip()
    cursor_lake.execute(query_schema_lake)
    conn_lake.commit()
    print(f"[INFO] Success Create Schema in Lake .....")

    # # insert data to lake
    insert_data_to_lake = sqlparse.format(
        open(
            path_query+'data.sql', 'r'
        ).read(), strip_comments=True).strip()
    cursor_lake.execute(insert_data_to_lake)
    conn_lake.commit()
    print(f"[INFO] Success insert data to Lake .....")

    # connection to dwh
    print(f"[INFO] Service ETL is Starting connection DWH .....")
    conn_dwh, engine_dwh = conn_warehouse.conn()
    cursor_dwh = conn_dwh.cursor()

    conf = connection.config('warehouse')
    conn, engine = connection.psql_conn(conf)
    cursor_dwh = conn.cursor()

    # # create schema in dwh
    query_schema_dwh = sqlparse.format(
        open(
            path_query+'schema_dwh.sql', 'r'
        ).read(), strip_comments=True).strip()
    cursor_dwh.execute(query_schema_dwh)
    conn_dwh.commit()
    print(f"[INFO] Success Create Schema in dwh  .....")

#     # insert data to dim_users
    query_select_tb_users = sqlparse.format(
        open(
            path_query+'query_insert_users.sql', 'r'
        ).read(), strip_comments=True).strip()
try:
    df = pd.read_sql(query_select_tb_users, engine_lake)
    df.to_sql('dim_users', engine_dwh, if_exists='append', index=False)
    print(f"[INFO] Success insert data users to dwh  .....")
except:
    print(f"[INFO] Failed insert data users to dwh  .....")

    # insert data to fact_orders
    query_select_orders = sqlparse.format(
        open(
            path_query+'query_insert_into_fact_orders.sql', 'r'
        ).read(), strip_comments=True).strip()
try:
    df = pd.read_sql(query_select_orders, engine_lake)
    df.to_sql('fact_users', engine_dwh, if_exists='append', index=False)
    print(f"[INFO] Success insert data users to dwh  .....")
except:
    print(f"[INFO] Failed insert data users to dwh  .....")
