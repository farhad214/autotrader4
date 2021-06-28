from sqlalchemy import create_engine
import pyodbc
import psycopg2
import pandas as pd

import __main__ as main
import sys
import inspect
import log1 as log1

# Boolean for Logging content
blc = True

# from constants import *
cred = {
    "GFPT":
        {"host_name": "gfp-prod-db-transfer.cemxpuf6t17z.eu-west-1.rds.amazonaws.com",
         "user_name": "GFPT_RO",
         "password": "gFpt42*",
         "db_name": "GFPT"},
    "INTERNAL":
        {"host_name": "gfp-tscript-db.cemxpuf6t17z.eu-west-1.rds.amazonaws.com",
         "user_name": "admin",
         "password": "Leici|y8Ah",
         "db_name": ""},
    "LILYPAD":
        {"host_name": "10.16.1.20",
         "user_name": "farhad",
         "password": "Farhadinho123",
         "db_name": "gfpdash"},
    "OUTAGE":
        {"host_name": "10.16.1.56",
         "user_name": "farhad",
         "password": "ViridisPower(214)",
         "driver": "{SQL Server Native Client 11.0};",
         "db_name": ""},
    "GCP":
        {"host_name":"35.197.199.134",
         "user_name":"pgsqlstagefarhad",
         "password":"6r33nfr0G",
         "driver":"",
         "db_name":"gcp_staging",
         "port":"5432"}}
schema_name = 'm7'

def get_postgresql_engine():

    s_conn = (
            "postgres+psycopg2://"
            + cred["GCP"]["user_name"]
            + ":" + cred["GCP"]["password"]
            + "@" + cred["GCP"]["host_name"]
            + ":" + cred["GCP"]["port"]+"/"
            + cred["GCP"]["db_name"]
    )
    return create_engine(s_conn)

def read_from_mssql(query_str, cn_inp=""):

    q_db_cred = 'mssql+pyodbc://'
    q_db_cred = q_db_cred + f'{conrad_db_cred["username"]}:{conrad_db_cred["password"]}@{conrad_db_cred["server"]}'
    q_db_cred = q_db_cred + '/BMRS?driver=SQL+Server'

    engine = sql.create_engine(q_db_cred)

    conn = engine.connect()
    df = pd.read_sql_query(query_str, con=conn)

    conn.close()
    engine.dispose()

    if cn_inp != "":
        df.columns = cn_inp

    return df

def query_postgresql(q, select_query = True):
    # if blc: log1.log_content(inspect.stack()[0][3], (main.__file__).split("/")[-1])

    engine = get_postgresql_engine ()
    connection = engine.connect ()

    if select_query:
        result_proxy = connection.execute(q).fetchall()
        if result_proxy!=[]:
            dfr = pd.DataFrame(result_proxy, columns=result_proxy[0].keys())
        else:
            dfr = None
    else:
        result_proxy = connection.execute(q)
        dfr = None
    connection.close()
    engine.dispose()

    return dfr

def write_to_postgresql(dfi, tbl_name, schema_name, if_exists="append"):
    # if blc: log1.log_content(inspect.stack()[0][3], (main.__file__).split("/")[-1])
    engine = get_postgresql_engine()
    connection = engine.connect()
    dfi.to_sql(tbl_name, con=engine, if_exists=if_exists, index=False, schema=schema_name)
    connection.close()
    engine.dispose()