from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType,LongType,DoubleType


# Declare schema for csv file and read data
emp_stg_schema = StructType([StructField("FIRST_NAME", StringType()),
StructField("LAST_NAME", StringType()),
StructField("EMAIL", StringType()),
StructField("ADDRESS", StringType()),
StructField("CITY", StringType()),
StructField("DOJ",DateType())])

int_emp_details_avro = StructType([StructField('registration_dttm', StringType(16777216), nullable=False), StructField('id', LongType(), nullable=False), StructField('first_name', StringType(16777216), nullable=False), StructField('last_name', StringType(16777216), nullable=False), StructField('email', StringType(16777216), nullable=False), StructField('gender', StringType(16777216), nullable=False), StructField('ip_address', StringType(16777216), nullable=False), StructField('cc', LongType(), nullable=True), StructField('country', StringType(16777216), nullable=False), StructField('birthdate', StringType(16777216), nullable=False), StructField('salary', DoubleType(), nullable=True), StructField('title', StringType(16777216), nullable=False), StructField('comments', StringType(16777216), nullable=False)]) 


emp_details_avro_cls = StructType([StructField('REGISTRATION', StringType(), nullable=False), 
    StructField('USER_ID', LongType(), nullable=False), 
        StructField('FIRST_NAME', StringType(), nullable=False), 
            StructField('LAST_NAME', StringType(), nullable=False), 
                StructField('USER_EMAIL', StringType(), nullable=False)])