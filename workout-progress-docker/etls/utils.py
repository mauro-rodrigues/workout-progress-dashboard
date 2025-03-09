import os
import types
import psycopg2

from typing import Any, Optional, Tuple


class PostgresConnector:
    def __init__(self) -> None:
        """
        Initializes a PostgreSQL connection using the set environment variables.
        """
        self.conn = None
        self.cursor = None
        self.which_conn = "locally"

    def __enter__(self) -> 'PostgresConnector':
        """
        Set up the database connection and return the connector.

        This method establishes a connection to the PostgreSQL database
        using credentials from environment variables depending on the 
        running environment (prod, local-docker, or dev).
        
        Returns:
            PostgresConnector: The database connection object itself, 
                                allowing it to be used in a 'with' statement.
        
        Raises:
            psycopg2.Error: If there's an error connecting to the database.
        """
        try:
            # read the required env vars
            WORKOUT_DB_HOST = os.getenv("WORKOUT_DB_HOST")  # localhost for dev
            WORKOUT_DB_PORT = os.getenv("WORKOUT_DB_PORT")
            WORKOUT_DB_NAME = os.getenv("WORKOUT_DB_NAME")
            WORKOUT_DB_USER = os.getenv("WORKOUT_DB_USER")
            WORKOUT_DB_PASSWORD = os.getenv("WORKOUT_DB_PASSWORD")

            # disable SSL for local connections
            SSL_MODE = 'disable' if WORKOUT_DB_HOST == 'host.docker.internal' else 'require'

            self.conn = psycopg2.connect(
                dbname=WORKOUT_DB_NAME,
                user=WORKOUT_DB_USER,
                password=WORKOUT_DB_PASSWORD,
                host=WORKOUT_DB_HOST,
                port=WORKOUT_DB_PORT,
                sslmode=SSL_MODE
            )

            self.cursor = self.conn.cursor()
            print(f"Successfully connected to PostgreSQL {self.which_conn}.")
            return self  # return the connector itself so it can be used within the 'with' block
        except psycopg2.Error as e:
            print(f"Error connecting to database: {e}")
            raise  # re-raise the exception so the 'with' statement can properly handle it

    def __exit__(self, exc_type: Optional[type], exc_value: Optional[BaseException], traceback: Optional[types.TracebackType]) -> bool:
        """
        Close the database connection when exiting the 'with' block.

        This method ensures the connection and cursor are properly closed when
        the 'with' block finishes, even if an exception is raised.

        Args:
            exc_type (Optional[type]): The exception type, if an exception occurred.
            exc_value (Optional[BaseException]): The exception instance, if an exception occurred.
            traceback (Optional[types.TracebackType]): The traceback object, if an exception occurred.

        Returns:
            bool: Always returns True to prevent further propagation of the exception if handled.
        """
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            print("🔌 Connection closed")

        # if an exception was raised, print it
        if exc_type:
            print(f"An error occurred: {exc_value}")
        return True  # prevent propagation of exceptions if handled

    def execute_query(self, query: str, params: Optional[Tuple[Any, ...]] = None, fetch_one: bool = False, fetch_all: bool = False) -> Optional[Any]:
        """
        Executes a SQL query (SELECT, INSERT, UPDATE, DELETE).

        This method executes a query using the cursor and returns the result
        depending on the flags for fetching results.

        Args:
            query (str): The SQL query to be executed.
            params (Optional[Tuple[Any, ...]]): The parameters for the query (defaults to None).
            fetch_one (bool): Flag indicating whether to fetch only one result (defaults to False).
            fetch_all (bool): Flag indicating whether to fetch all results (defaults to False).

        Returns:
            Optional[Any]: The fetched result(s) from the query if requested, otherwise None.

        Raises:
            psycopg2.Error: If the query execution fails.
        """
        try:
            self.cursor.execute(query, params or ())
            if fetch_one:
                return self.cursor.fetchone()
            if fetch_all:
                return self.cursor.fetchall()
            self.conn.commit()
        except psycopg2.Error as e:
            print(f"Query error: {e}")
            self.conn.rollback()
            raise  # re-raise the exception so it can be handled outside if necessary
