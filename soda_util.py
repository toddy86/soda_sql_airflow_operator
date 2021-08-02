import os
from pathlib import Path
from typing import TypedDict
from urllib.parse import urlparse

import yaml


class SodaConnection(TypedDict):
    type: str
    host: str
    port: int
    username: str
    password: str
    database: str
    schema: str


class SodaWarehouse(TypedDict):
    name: str
    connection: SodaConnection


def build_soda_warehouse(
    warehouse_name: str,
    database_name: str,
    database_type: str = "postgres",
    schema: str = "public",
    port: int = 5432,
    airflow_connection: str = "AIRFLOW_CONN_DEFAULT_POSTGRES",
) -> SodaWarehouse:
    """
    Builds a Soda Warehouse dictionary object
    """
    connection = urlparse(os.getenv(airflow_connection))

    warehouse: SodaWarehouse = {
        "name": warehouse_name,
        "connection": {
            "type": database_type,
            "host": str(connection.hostname),
            "port": connection.port or port,
            "username": str(connection.username),
            "password": str(connection.password),
            "database": str(database_name),
            "schema": schema,
        },
    }

    return warehouse


def convert_templated_yml_to_dict(scan_path: Path, scan_file: str):
    """
    Converts a templated yml file to a templated dictionary
    Allows the use of templates in yml fields which are not currently templated by Soda SQL (e.g. table_name)

    Enhancement request submitted to Soda SQL team and they are looking to implement in the coming weeks
    Could alternatively just create the scan as a dict, but type safety and readability suffer
    """

    path = scan_path / scan_file
    with open(path) as file:
        return yaml.safe_load(file)
