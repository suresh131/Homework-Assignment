import sys
import os
import re
import datetime
import pandas as pd

# To identify Airline Code from the description of the airline row
def get_airline_code(filename_with_absolute_path):
    try:
        with open(filename_with_absolute_path, "r") as file:
            file_contents = file.read()
            airline_code = re.search(r"Airline:\s+.*?\((\w+)\)", file_contents).group(1)
            file.close()
            return airline_code
    except AttributeError:
        print("Error: No airline code found in the data.")
    except FileNotFoundError:
        print("Error: File Not Found.")
    except IOError:
        print("Error: Failed to read the file.")


# To get rid of any garbage data using Airline Code
def check_valid_row(data, airline_code):
    # Split the data by commas
    columns = data.split(",")

    # Get the first column and remove leading/trailing whitespace characters
    first_column = columns[0].strip()

    # Check if the flight code is DL
    return first_column == airline_code


# To validate if the date is correct
def check_valid_date(data):

    # Split the data by commas
    columns = data.split(",")

    # Get the first column and remove leading/trailing whitespace characters
    second_column = columns[1].strip()

    # Check if the flight code is DL
    try:
        datetime.datetime.strptime(second_column, "%d/%m/%y")
        return True
    except ValueError:
        return False


# Main section of the code
def consume_file(filename_with_absolute_path, airline_code):
    try:
        # Create an empty dictionary for Flight Data
        data_for_df = {
            "Airline_Code": [],
            "flight_dt": [],
            "flightnum": [],
            "orig_arpt": [],
            "dest_arpt": [],
            "flightstatus": [],
            "lastupdt": [],
            "flightkey": [],
        }

        # Dictionary for appending data to Dataframe Dictionary dynamically
        column_list = {
            "Airline_Code": 0,
            "flight_dt": 1,
            "flightnum": 2,
            "orig_arpt": 3,
            "dest_arpt": 4,
            "flightstatus": 5,
            "lastupdt": 6,
            "flightkey": 7,
        }

        # Open file for data processing in Read mode
        with open(filename_with_absolute_path, "r") as file:

            # Print status of starting of process
            print("Started processing file: ", filename_with_absolute_path)

            # Process each line one after another
            for line in file:

                # Process each line of the file
                # Remove leading/trailing whitespace characters
                row = line.strip()

                # Check if the data is Delta Airline row
                if check_valid_row(row, airline_code) and check_valid_date(row):

                    # Split the data using a delimiter
                    columns = row.split(",")

                    # Append data to the data_for_df Dictionary
                    # using column name and column id from column_list dictionary
                    for col_name, col_id in column_list.items():
                        data_for_df[col_name].append(columns[col_id].strip())

                # If not good data, skip the row and continue the process
                else:
                    continue

            # Close the file
            file.close()

            # Create a data frame using the data from the dictionary
            data_frame = pd.DataFrame(data_for_df)

            # Drop the "Airline_Code" Column from the generated dataframe from being displayed
            data_frame = data_frame.drop("Airline_Code", axis=1)

            # Replace invalid time to valid time, data can have 24:00:00 but as per
            # 24 hour time format it should be 00:00
            data_frame["lastupdt"] = data_frame["lastupdt"].str.replace(
                "24:00:00", "00:00"
            )

            # Add the date to the timestamp for accurate date, else python will add
            # default year and date 1900/01/01
            data_frame["lastupdt"] = pd.to_datetime(
                data_frame["flight_dt"] + " " + data_frame["lastupdt"],
                format="%m/%d/%y %H:%M",
            )

            # Convert the data from data frame to date and datetime from
            # row data for Flight Date and Last Updated Time
            data_frame["flight_dt"] = pd.to_datetime(
                data_frame["flight_dt"], format="%m/%d/%y"
            )

            # Sort the data frame on flight key, date, and timestamp
            # in  descending order
            sorted_df = data_frame.sort_values(
                ["flightkey", "lastupdt"], ascending=[True, False]
            )

            # Filter out unwanted data by grouping based on the Flight key to get first row
            filtered_df = sorted_df.groupby(["flightkey"]).first().reset_index()

            # Extract directory and filename components
            directory = os.path.dirname(filename_with_absolute_path)
            filename = os.path.basename(filename_with_absolute_path)

            # Add prefix to the filename
            output_filename = os.path.join(directory, "Dedup_" + filename)

            # write CSV file after cleaning data
            filtered_df.to_csv(output_filename, index=False)

            # print Output file save message after process finishes
            print("Dedup File generated successfully: ", output_filename)

    # Exception handling in case of file-level error
    except FileNotFoundError:
        print("Error: File Not Found.")
    except IOError:
        print("Error: Failed to read the file.")


def main():
    # Get the argument value
    filename_with_absolute_path = sys.argv[1]

    # Get the airline code to filter data
    airline_code = get_airline_code(filename_with_absolute_path)

    # Start file processing for data
    consume_file(filename_with_absolute_path, airline_code)


if __name__ == "__main__":
    main()
