#!/usr/bin/python3

import os
import json
import psycopg2

from sqlalchemy import create_engine


def config(param):
    path = os.getcwd()
    with open(path+'/'+'config.json') as file:
        conf = json.load(file)[param]
    return conf


def conn():
    conf = config('postgresql')
    try:
        conn = psycopg2.connect(host=conf['host'],
                                database=conf['db'],
                                user=conf['user'],
                                password=conf['pwd']
                                )
        print(f"[INFO] Success connect Lake .....")
        engine = create_engine(
            f"postgresql+psycopg2://{conf['user']}:{conf['pwd']}@{conf['host']}/{conf['db']}")
        return conn, engine
    except:
        print(f"[INFO] Can't connect Lake .....")
