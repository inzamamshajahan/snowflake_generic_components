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
    
def map_columns(df,map_columns):
    """
     Remove double qoutes from the column names and drop unwanted columns
     Rename the dataframe column names, this is necessary because after renaming the columns we put this dataframe into an internal stage as a csv file, from where we use the copy command to load the data into the snowflake table so that we can get the on_error continue option.
    """
    # Remove double qoutes from the column names and drop unwanted columns
    cols = df.columns
    map_keys = [key.upper() for key in map_columns.keys()]
    for c in cols:
        df = df.with_column_renamed(c,c.replace('"',''))
    cols = df.columns
    for c in cols:
        if c.upper() not in map_keys:
            print("Dropped column,"+" "+c.upper())
            df=df.drop(c.upper())   

    # Rename the dataframe column names
    for k,v in map_columns.items():
        df = df.with_column_renamed(k.upper(),v.upper())
    return df
    
def copy_to_table_semi_struct_data(session,config_file,schema='NA'):
    
    """
    This function will help in copying avro data from s3 to snowflake
    In the config_file we can specify the following details
    {
    "Database_name":"DEMO_DB",
    "Schema_name":"PUBLIC",
    "Target_table":"EMP_DETAILS_AVRO_CLS",
    "Reject_table":"EMPLOYEE_AVRO_REJECTS",      
    "target_columns":["REGISTRATION",
    "USER_ID",
    "FIRST_NAME",
    "LAST_NAME",
    "USER_EMAIL"],
    "on_error":"CONTINUE",
    "Source_location":"@DEMO_DB.EXTERNAL_STAGES.S3_STG2/sp_avro/",
    "Source_file_type":"avro",
    "map_columns":{
        "REGISTRATION_DTTM":"REGISTRATION",
        "ID":"USER_ID",
        "FIRST_NAME":"FIRST_NAME",
        "LAST_NAME":"LAST_NAME",
        "EMAIL":"USER_EMAIL"
    }
    
    }
    The target_columns specify the columns in the target table present inside snowflake (it is not compulosry for the table to exist, if it dosent exist the framework will create the table)
    The map_columns key contains a dictionary, the keys in the dictionary shows the column name in the source avro file which is currently in the s3 bucket and the values shows the corresponding column names in the table which is in snowflake
    
    """
    database_name = config_file.get("Database_name")
    Schema_name = config_file.get("Schema_name")
    Target_table = config_file.get("Target_table")
    target_columns = config_file.get("target_columns")
    on_error = config_file.get("on_error")
    Source_location = config_file.get("Source_location")
    transformations = config_file.get("transformations")
    maped_columns = config_file.get("map_columns")
    # print(f"maped_columns : {maped_columns}")

    if config_file.get("Source_file_type") == 'csv':
            return "Expecting semi structured data but got csv"
    elif config_file.get("Source_file_type") == 'avro':
        df = session.read.avro(Source_location)
    
    # Map columns in df to target table
    df = map_columns(df,maped_columns)

    # Create temporary stage
    _ = session.sql("create or replace temp stage demo_db.public.mystage").collect()
    remote_file_path = '@demo_db.public.mystage/'+Target_table+'/'
    # Write df to temporary internal stage location
    df.write.copy_into_location(remote_file_path, file_format_type="csv", format_type_options={"FIELD_OPTIONALLY_ENCLOSED_BY":'"'}, header=False, overwrite=True)
    
    # Read the file from temp stage location
    df = session.read.schema(schema).csv("'"+remote_file_path+"'")
    with session.query_history() as query_history:
        copied_into_result = df.copy_into_table(database_name+"."+Schema_name+"."+Target_table, target_columns=target_columns,force=True,on_error=on_error,format_type_options={"FIELD_OPTIONALLY_ENCLOSED_BY":'"'})
    query = query_history.queries
    # Mention command to collect query id of copy command executed.
    for id in query:
        if "COPY" in id.sql_text:
            qid = id.query_id
    return copied_into_result, qid