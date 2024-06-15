from snowflake.snowpark import Session
from snowflake.snowpark.functions import col,to_timestamp
import pandas as pd

def snowconnection(connection_config):
    session = Session.builder.configs(connection_config).create()
    session_details = session.create_dataframe(
        [
            [session._session_id,
             session.sql("select current_user();").collect()[0][0],
             str(session.get_current_warehouse()).replace('"',''),
             str(session.get_current_role()).replace('"','')
             ]
        ],
            schema=["session_id","user_name","warehouse","role"])
            
    session_details.write.mode("append").save_as_table("session_audit")
    return session