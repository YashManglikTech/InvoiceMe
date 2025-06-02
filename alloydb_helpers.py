# your_project/db_connector.py
import config
import logging
import psycopg2
from psycopg2 import pool

from typing import Annotated, Optional
from psycopg2.extras import execute_values

class AlloyDBConnector:
    _connection_pool = None

    def __init__(self, database, username, password, host, port, bulk_insert=False):
        self.database = database
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.bulk_insert = bulk_insert

        # Initialize connection pool if it hasn't been initialized yet
        if AlloyDBConnector._connection_pool is None:
            try:
                AlloyDBConnector._connection_pool = pool.SimpleConnectionPool(
                    minconn=config.MIN_ALLOYDB_CONNECTIONS,  # Minimum number of connections in the pool
                    maxconn=config.MAX_ALLOYDB_CONNECTIONS, # Maximum number of connections in the pool
                    database=self.database,
                    user=self.username,
                    password=self.password,
                    host=self.host,
                    port=self.port
                )
                logging.info("AlloyDB connection pool initialized successfully.")
            except psycopg2.Error as e:
                error_msg = f"Error while initializing AlloyDB connection pool: {str(e)}"
                logging.error(error_msg)
                raise Exception(error_msg)

    def run(
        self,
        query: Annotated[str, "The input SQL query for execution"],
        insertion_data: Annotated[list, "Bulk data insert list"] = None,
        query_type: Annotated[str, "Type of the input query"] = "retrieval"
    ) -> tuple[list, list]:
        conn = None
        cur = None
        self.columns = []
        self.results = []

        try:
            # Get a connection from the pool
            conn = AlloyDBConnector._connection_pool.getconn()
            cur = conn.cursor()
            logging.debug(f"Executing query: {query}")

            if self.bulk_insert:
                if not insertion_data:
                    raise ValueError("For bulk insertion, insertion data is required")
                execute_values(cur, query, insertion_data)
                logging.info(f"Bulk insert executed successfully for query: {query}")
            else:
                cur.execute(query)
                if query_type == "retrieval":
                    self.results = cur.fetchall()
                    self.columns = [desc[0] for desc in cur.description]
                    logging.debug(f"Query executed successfully. Results: {len(self.results)} rows.")
                else:
                    logging.info(f"Non-retrieval query executed successfully: {query}")

            conn.commit()
            return self.columns, self.results

        except psycopg2.Error as e:
            if conn:
                conn.rollback() # Rollback in case of an error
            self.error = f"Error executing query: {str(e)}"
            logging.error(self.error)
            raise Exception(self.error)

        except Exception as e:
            self.error = f"An unexpected error occurred: {str(e)}"
            logging.error(self.error)
            raise Exception(self.error)

        finally:
            if cur:
                cur.close()
            if conn:
                AlloyDBConnector._connection_pool.putconn(conn) # Return connection to the pool

    @staticmethod
    def close_connection_pool():
        """
        Closes all connections in the AlloyDB connection pool.
        """
        if AlloyDBConnector._connection_pool:
            AlloyDBConnector._connection_pool.closeall()
            logging.info("AlloyDB connection pool closed.")
            AlloyDBConnector._connection_pool = None
