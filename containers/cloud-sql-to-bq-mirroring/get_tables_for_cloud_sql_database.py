import argparse
import json
import logging
import os
import sqlalchemy
import subprocess
import time

from typing import List


class CloudSqlDatabaseOps:
    """ Class providing a set of operations that can be executed on a Cloud SQL database """

    def __init__(
            self,
            _driver_name: str,
            _db_username: str,
            _db_password: str,
            _db_name: str,
            _instance_connection_name: str
    ):
        engine_url = sqlalchemy.engine.url.URL(
            drivername=_driver_name,
            username=_db_username,
            password=_db_password,
            database=_db_name,
            query={'unix_sock': f'/cloudsql/{_instance_connection_name}/.s.PGSQL.5432'}
        )
        self.db_engine = sqlalchemy.create_engine(
            engine_url,
            # 'pool_size' is the maximum number of permanent connections to keep.
            pool_size=5,
            # Temporarily exceeds the set pool_size if no connections are available.
            max_overflow=2,
            # 'pool_timeout' is the maximum number of seconds to wait when retrieving a new connection from the pool.
            # After the specified amount of time, an exception will be thrown.
            pool_timeout=30,  # 30 seconds
            # 'pool_recycle' is the maximum number of seconds a connection can persist. Connections that live longer
            # than the specified amount of time will be reestablished.
            pool_recycle=1800,  # 30 minutes
        )

    def get_table_names(self) -> List[str]:
        """
        Method for querying the table names associated with the database in question.
        @return: list of tables under a database, not including those prefixed with "knex"
        """
        _table_names = self.db_engine.table_names()
        non_knex_table_names = filter(lambda table: not table.startswith('knex'), _table_names)
        return list(non_knex_table_names)


def start_cloud_sql_proxy(connection_name: str) -> subprocess.Popen:
    """
    Method for starting up the Cloud SQL proxy, by way of which we will interact with an instance's db.
    @param connection_name: name of the connection to a Cloud SQL instance
    @return:                pid of the started Cloud SQL proxy process
    """
    cmd = f"./cloud_sql_proxy -instances={connection_name} -dir=/cloudsql"
    proxy_proc = subprocess.Popen(cmd, shell=True)
    time.sleep(10)  # buffer time for proxy to start up
    return proxy_proc


def argument_parser():
    cli = argparse.ArgumentParser(description="Argument parser for Cloud SQL table listing.")
    cli.add_argument("--instance", type=str, required=True, help="Cloud SQL instance connection name")
    cli.add_argument("--database", type=str, required=True, help="Name of database we are connecting to")
    return cli


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    # Parse input arguments
    args = argument_parser().parse_args()

    driver_name = 'postgres+pg8000'
    db_username = os.environ.get('USER')
    db_password = os.environ.get('PASS')

    instance_connection_name = args.instance
    db_name = args.database

    logging.info(f"Gathered arguments for connection [instance: {instance_connection_name}, db: {db_name}]")

    # Start Cloud SQL proxy
    proxy = start_cloud_sql_proxy(instance_connection_name)
    logging.info(f"Started Cloud SQL proxy [pid: {proxy.pid}]")

    # Establish DB engine (connection pool to a database on Cloud SQL instance in question)
    cloud_sql_database_ops = CloudSqlDatabaseOps(
        _driver_name=driver_name,
        _db_username=db_username,
        _db_password=db_password,
        _db_name=db_name,
        _instance_connection_name=instance_connection_name
    )
    logging.info(f"Connected to Cloud SQL [instance: {instance_connection_name}, db: {db_name}]")

    # Gather table names for the database
    table_names = cloud_sql_database_ops.get_table_names()
    logging.info(f"Tables for database [database: {db_name}, table_names: {table_names}]")

    # Write to sidecar table values (to be used in a downstream Kubernetes pod)
    fs = open("/airflow/xcom/return.json", "x")
    fs.write(json.dumps(','.join(table_names)))
    fs.close()

    # Close connection to Cloud SQL proxy
    proxy.terminate()
    proxy.wait()
