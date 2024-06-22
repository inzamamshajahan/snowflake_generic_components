# snowflake_generic_components
This repo contains the generic components which we can use while working on a snowflake snowpark project

### > Files ending with ‘_reqs’ represent the objectives of the corresponding files. For example, ‘code_library_reqs.ipynb’ defines the requirements for the ‘code_library.py’ file
### > The order of the files starts from 100.
### > Every function will have a comment associated with it, stating its function
# Scope of improvement
## for the code_library module
### Currently we have functions for the following:
     > copy csv data
     > copy semistructured data (avro)
     > create session
     > map columns
    ### we can create functions for the following:
     > We can create a module to close a session
     > Module to audit the records which got loaded into the table
     > Module to calculate the cost of warehouse
     > Module to spin up a particular warehouse
     > Module to spin down a particular warehouse
     > Method to unload the data from snowflake to s3 stage location
     
     
     