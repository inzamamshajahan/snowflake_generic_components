from snowflake.snowpark import Session
from snowflake.snowpark.functions import col,to_timestamp
import pandas as pd
from dotenv import dotenv_values
secrets = dotenv_values(".env")

connection_parameters = {
"account":secrets['account'], 
"user":secrets['user'], 
"password": secrets['password'], 
"role":secrets['role'], 
"warehouse":"COMPUTE_WH", 
"database":"DEMO_DB", 
"schema":"PUBLIC" 
}


def snowconnection(connection_config):
    """
        # def snowconnection(connection_config)
        
                > It should create a connection
                > It should create a table with session_id, user_name, warehouse and role
                > By creating this function we can ensure that the user can create sessions convieniently and that the session creation activity has been logged into an audit table
    """
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
    

def copy_to_table(session,config_file,schema='NA'):
    """
    # def copy_to_table(session,config_file,schema='NA'):
    
        > To copy data from s3 raw layer to snowflake table
        > Currently the function only supports copying of csv files from s3 to snowflake 
    """
    
    database_name = config_file.get("Database_name")
    Schema_name = config_file.get("Schema_name")
    Target_table = config_file.get("Target_table")
    target_columns = config_file.get("target_columns")
    on_error = config_file.get("on_error")
    Source_location = config_file.get("Source_location")


    if config_file.get("Source_file_type") == 'csv':
            schema = schema
            df = session.read.options({"skip_header":1}).schema(schema).csv("'"+Source_location+"'")
    else :
        print( "The program currently only supports csv source_file_type")
        
    with session.query_history() as query_history:
        copied_into_result = df.copy_into_table(database_name+"."+Schema_name+"."+Target_table, target_columns=target_columns,force=True,on_error=on_error )
    query = query_history.queries
    # Mention command to collect query id of copy command executed.
    for id in query:
        if "COPY" in id.sql_text:
            qid = id.query_id
    return copied_into_result, qid


def collect_rejects(session,qid,config_file):
        """
        # def collect_rejects(session,qid,config_file):
            > https://docs.snowflake.com/en/sql-reference/functions/validate
            > Collect all the errors which happned while trying to load using a copy commmand (quiery id of the copy command is there in the qid parameter
            >
        """
    database_name = config_file.get("Database_name")
    Schema_name = config_file.get("Schema_name")
    Target_table = config_file.get("Target_table")
    Reject_table = config_file.get("Reject_table")
    rejects = session.sql("select *  from table(validate("+database_name+"."+Schema_name+"."+Target_table+" , job_id =>"+ "'"+ qid +"'))")
    rejects.write.mode("append").save_as_table(Reject_table)
    return rejects