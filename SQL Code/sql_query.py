import psycopg2

# Database connection details
host = "your_host"
database = "your_database"
username = "your_username"
password = "your_password"

# Establish database connection
connection = psycopg2.connect(
    host=host, database=database, user=username, password=password
)


# Create a cursor
cursor = connection.cursor()

# assuming that data is already avialble in a table called "flightleg" in Postgres db,preparing the query as select_query
select_query = """
            with cte_flightplan as (
  Select
    flightkey,
    flightnum,
    flight_dt,
    orig_arpt,
    dest_arpt,
    flightstatus,
    lastupdt,
    row_number() over (
      partition by flightkey
      order by
        lastupdt desc
    ) rnk
  from
    flightleg
)
Select
  flightkey,
  flightnum,
  flight_dt,
  orig_arpt,
  dest_arpt,
  flightstatus,
  lastupdt
from
  cte_flightplan
where
  rnk = 1
;"""


try:
    cursor.execute(select_query)
    rows = cursor.fetchall()
    for row in rows:
        print(row)


except (Exception, psycopg2.Error) as error:
    Print("Error: ", error)

finally:
    # Close the cursor and connection
    cursor.close()
    connection.close()
