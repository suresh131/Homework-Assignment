In this Homework Assignment we ae going to use Python scripting to read a csv file placed at a location,process the data as a dataframe and get the latest record for each each flightkey based on the lastupddt timestamp value.

Pre-Requisites : 
1. Latest version of Pandas package must have been installed in the system already
2. The output of the Data Pipeline gets generated in the same directory where the source file got read from.Proper permissions should be there for the file to written to thatdirectory
3. The Script etl.py accepts one argument (file_name_with_absolute_path) without any quotes
4. Change Directory to the place where the etl.py script is present and then execute the command as below :
ex: C:\Users\XXXX\Documents\Python>python etl.py C:\Users\XXXX\Documents\Python\FlightPlan\Dummy_Flight_Leg_Data.csv


Input : Filename along the File Path should be to the python script
Ex: python etl.py C:\Users\XXXX\Documents\FlightPlan\Dummy_Flight_Leg_Data.csv

Output : The Dedup file wil be generated in the same directory path as input.The filename will be prefixed with "Dedup_" to the input filename.

Project Structure and Approach:

etl.py script take the csv file along with path as input ,calls Python commands and creates the dataframe in the required format.Data cleaning is done on the dataframe to make the data in a readable/presentable format.

There are multiple functions used in the script.
Main() which has the inputname_with_absolute_path and calls the get_airline_code function to retrieve the Carrier_code. The consume_file function take the filename with path and airliine code to do further data processing.
consume_file function check_valid_row functions to read the raw data, identify the needed records for processing and creates the dataframe.
Data cleansing and Data Dedup in form of a new csv file is created in the consume_file function logic.

In case of any errors at each step error handling is done to print the error message. 

Additional considerations taken care of  in code :

1. Some of the lastupdt values in the source file are in HH:MM:SS format (24:00:00) which is incorrect format.It should be 00:00.This is replaced as part of Data Cleansing
2. Airline_code column is dropped from the final output as per the sample example provided.
3. Flight_dt column values are formatted to show more accurately
4. Flight_dt value is concatenated to the lastupdt column value to show accurate date,python adds default date (1900/01/01) otherwise