In this Homework Assignment we are going to use Python scripting along with SQL cte the latest record for each each flightkey based on the lastupddt timestamp value.

Pre-Requisites : 
1. Latest version of psycopg2 package must have been installed in the system already
2. The output of the Select_query is printed on the same window.
3. Connection details like Host,Databasename,Username and Password to connect to the Postgres Database needs to configured in the script.
4. Required data is assumed to be already present in the postgres table called "flightleg"


Input : No Parameter is needed for the input.The python script needs to be executed as is 
Ex: C:\Users\XXXX\Documents\Python>python sql_query.py

Output : The required output will be printed in the terminal/notebook

Project Structure and Approach:

sql_query.py script is used to connect to the postgres db ,run a select query and fetch the output of that query.

As part of this script , we ae connecting to a Postgres db that has the required table and data. A connection from Python connects to Postgres db,fetches the requierd information.

In case of any errors at each step error handling is done to print the error message. 

