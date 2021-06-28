import gcp as gcp
import pandas as pd

def log_content(func_name, file_name):
    df = pd.DataFrame([pd.to_datetime("now"), file_name,func_name],
                      index=["timestamp", "file_name", "function_name"]).T

    gcp.write_to_postgresql(df, "logged_content", "m7")