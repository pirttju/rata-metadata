import argparse
from getpass import getpass
import os
import json
import sys
import psycopg
import requests

# API Endpoint
API_BASE_URL = "https://rata.digitraffic.fi/api/v1"
API_STATIONS = "/metadata/stations"
API_OPERATORS = "/metadata/operators"
API_CAUSES = "/metadata/cause-category-codes?show_inactive=true"
API_DETAILED_CAUSES = "/metadata/detailed-cause-category-codes?show_inactive=true"
API_THIRD_CAUSES = "/metadata/third-cause-category-codes?show_inactive=true"

# Schema
class Station:
    def __init__(self, data):
        self.passenger_traffic = data["passengerTraffic"]
        self.type = data["type"]
        self.station_name = data["stationName"]
        self.station_short_code = data["stationShortCode"]
        self.station_uic_code = data["stationUICCode"]
        self.country_code = data["countryCode"]
        self.latitude = data["latitude"]
        self.longitude = data["longitude"]

class Operator:
    def __init__(self, data):
        self.operator_name = data["operatorName"]
        self.operator_short_code = data["operatorShortCode"]
        self.operator_uic_code = data["operatorUICCode"]

class Cause:
    def __init__(self, data):
        self.category_code = data["categoryCode"]
        self.category_name = data["categoryName"]
        self.valid_from = data["validFrom"]
        if "validTo" in data:
            self.valid_to = data["validTo"]
        else:
            self.valid_to = None

class DetailedCause:
    def __init__(self, data):
        self.category_code = data["detailedCategoryCode"]
        self.category_name = data["detailedCategoryName"]
        self.valid_from = data["validFrom"]
        if "validTo" in data:
            self.valid_to = data["validTo"]
        else:
            self.valid_to = None

class ThirdCause:
    def __init__(self, data):
        self.category_code = data["thirdCategoryCode"]
        self.category_name = data["thirdCategoryName"]
        self.valid_from = data["validFrom"]
        if "validTo" in data:
            self.valid_to = data["validTo"]
        else:
            self.valid_to = None

# Database functions
def truncate_tables(connection):
    with connection.cursor() as cursor:
        cursor.execute("TRUNCATE digitraffic.station;")
        cursor.execute("TRUNCATE digitraffic.operator;")
        cursor.execute("TRUNCATE digitraffic.cause_code;")

def insert_station(connection, data):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO digitraffic.station VALUES (
                %(passenger_traffic)s,
                %(type)s,
                %(station_name)s,
                %(station_short_code)s,
                %(station_uic_code)s,
                %(country_code)s,
                %(longitude)s,
                %(latitude)s
            );
        """,
            data,
        )

def insert_operator(connection, data):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO digitraffic.operator VALUES (
                %(operator_name)s,
                %(operator_short_code)s,
                %(operator_uic_code)s
            );
        """,
            data,
        )

def insert_cause_code(connection, data):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO digitraffic.cause_code VALUES (
                %(category_code)s,
                %(category_name)s,
                %(valid_from)s,
                %(valid_to)s
            );
        """,
            data,
        )

# Updater
def update_metadata(connection):
    # Responses
    res_stations = requests.get(API_BASE_URL + API_STATIONS)
    for k in res_stations.json():
        data = Station(k)
        insert_station(connection, vars(data))

    res_operators = requests.get(API_BASE_URL + API_OPERATORS)
    for k in res_operators.json():
        data = Operator(k)
        insert_operator(connection, vars(data))

    res_causes = requests.get(API_BASE_URL + API_CAUSES)
    for k in res_causes.json():
        data = Cause(k)
        insert_cause_code(connection, vars(data))

    res_detailed_causes = requests.get(API_BASE_URL + API_DETAILED_CAUSES)
    for k in res_detailed_causes.json():
        data = DetailedCause(k)
        insert_cause_code(connection, vars(data))

    res_third_causes = requests.get(API_BASE_URL + API_THIRD_CAUSES)
    for k in res_third_causes.json():
        data = ThirdCause(k)
        insert_cause_code(connection, vars(data))

def main():
    ap = argparse.ArgumentParser(prog="rata-metadata", description="Metadata Import Tool", conflict_handler="resolve")

    # Postgres related arguments
    ap.add_argument("-d", "--dbname", required=True, help="specifies the name of the database to connect to")
    ap.add_argument(
        "-h",
        "--host",
        default="localhost",
        required=False,
        help="specifies the host name on which the server is running",
    )
    ap.add_argument(
        "-p", "--port", default=5432, required=False, help="specifies the port on which the server is listening"
    )
    ap.add_argument("-U", "--username", required=True, help="connect to the database as the username")
    ap.add_argument(
        "-W",
        "--password",
        required=False,
        action="store_true",
        help="prompt for a password before connecting to a database",
    )
    ap.add_argument("-t", required=False, action="store_true", help="test only without committing")
    args = ap.parse_args()

    # Postgres connection details
    dsn = f"dbname={args.dbname} user={args.username} host={args.host} port={args.port}"

    # Prompt for a password if requested
    if args.password == True:
        args.password = getpass(prompt="Password: ", stream=None)
        dsn = f"{dsn} password={args.password}"

    # Create a connection
    with psycopg.connect(dsn) as connection:
        truncate_tables(connection)
        update_metadata(connection)

        # If a test then rollback otherwise commit
        if args.t == True:
            connection.rollback()
        else:
            connection.commit()

if __name__ == "__main__":
    main()
