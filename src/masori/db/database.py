"""
Handles database-related functionality
"""
import string
import secrets
import psycopg2
from typing import TypedDict, Optional, List, Dict
from psycopg2 import OperationalError, connect, sql
from datetime import datetime
import loguru
from contextlib import contextmanager

from masori.config import settings

class PGCredentials(TypedDict):
    dbname: str
    user: str
    password: str
    host: str
    port: str

class Database:
    def __init__(self):
        self.logger = loguru.logger
        self.db_host = settings.DB_HOST
        self.db_port = settings.DB_PORT
        self.db_name = settings.DB_NAME
        self.db_user = settings.DB_USER
        self.db_password = settings.DB_PASSWORD
        self.default_password = settings.DEFAULT_PASSWORD

        self.superuser = settings.PG_USER
        self.super_pw = settings.PG_PASSWORD

    def generate_password(self, length: int=20) -> str:
        """
        Generates a new password string for database user

        Args:
            length: int - length of new password
        
        Returns:
            str - new password string
        """
        alphabet = string.ascii_letters + string.digits + string.punctuation
        return ''.join(secrets.choice(alphabet) for _ in range(length))

    def try_login(self):
        """
        Tests if user can login with given credentials

        Args:
            None
        
        Returns:
            None
        """
        try:
            creds = self.get_pg_creds()
            conn = self.get_db_connection(creds)
            conn.close()

            return True
        
        except OperationalError:
            return False
        
    def rotate_password(self) -> None:
        """
        Checks if db user password is default value and rotates it if so

        TODO:
            Add in other criteria to rotate password ie schedule 

        Returns:
            None
        """
        self.logger.info('Attempting to rotate password.')
        if self.try_login():
            self.logger.info(f'Connected to Database with user {self.db_user}.')
            if self.db_password == self.default_password:
                self.logger.info('Password meets rotation criteria. Generating new password')
                new_pw = self.generate_password()
                self.logger.info('New password generated. Attempting to change password in database...')
                try:
                    self.update_db_user_password(new_pw)
                    self.logger.info('Successfully changed password as super user. Updating .env file')
                    settings.update_db_password(new_pw)
                    self.logger.info('Rotation complete.')
                except Exception as e:
                    self.logger.info(f'Error updating password in database - {e}')
            else:
                self.logger.info('Password meets criteria. No need to rotate.')
        else:
            self.logger.warning(f'Unable to login to database with user {self.db_user}')


    
    def update_db_user_password(self, new_pw: str) -> None:
        """
        Logs in as superuser and rotates password for masori user

        Args:
            new_pw: str - new password to set db user to
        
        Returns:
            None
        """
        with connect(
            dbname = self.db_name,
            user = self.superuser,
            password = self.super_pw,
            host = self.db_host,
            port = self.db_port
        ) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f"ALTER USER {self.db_user} WITH PASSWORD %s;", (new_pw,))

    def get_pg_creds(self) -> PGCredentials:
        """
        Ensures a rotated password and retrieves connection info from config.py

        Args:
            None
        
        Returns:
            Dict of pg credentials
        """
        return {
            "host": self.db_host,
            "port": self.db_port,
            "dbname": self.db_name,
            "user": self.db_user,
            "password": self.db_password
        }


    def get_db_connection(self, pg_creds: PGCredentials) -> Optional[psycopg2.extensions.connection]:
        """
        Returns a psycop2 connection object to the pg database

        Args:
            pg_creds: 
        """
        try:
            conn = connect(**pg_creds)
        except OperationalError as e:
            self.logger.info(f"Failed to retrieve connection with error {e}")

        return conn

    @contextmanager
    def db_connection(self):
        creds = self.get_pg_creds()
        conn = self.get_db_connection(creds)
        try:
            yield conn
        finally:
            if conn:
                conn.close()


    def infer_postgres_type(self, value) -> str:
        if isinstance(value, bool):
            return "BOOLEAN"
        elif isinstance(value, int):
            return "INTEGER"
        elif isinstance(value, float):
            return "REAL"
        elif isinstance(value, str):
            try:
                datetime.fromisoformat(value)
                return "TIMESTAMP"
            except ValueError:
                return "TEXT"
        else:
            return "TEXT"

    def upsert_table(self, database: str, schema: str, table_name: str, 
                     rows: List[Dict], partition_keys: List[str]) -> None:
        """
        Creates a table given a data input and upserts those rows based on on partition keys

        Args:
            database: str - database to add table to
            schema: str - schema to add table to 
            table_name: str - name of table to process data
            rows: list[dict] - data to add to postgres in dictionary form
            partition_keys: list[str] - key identifiers to update data on

        Returns:
            None
        """

        if not rows:
            self.logger.warn('No rows to upsert')
        
        with self.db_connection() as conn:
            with conn.cursor() as cur:

                first_row = rows[0]
                columns_names = list(first_row.keys())
                column_defs = [
                    sql.SQL("{} {}").format(
                        sql.Identifier(col),
                        sql.SQL(self.infer_postgres_type(first_row[col]))
                        )
                    for col in columns_names
                ]
                if partition_keys:
                    pk_constraint = sql.SQL("PRIMARY KEY ({})").format(
                        sql.SQL(', ').join(map(sql.Identifier, partition_keys))
                    )
                    print(f'PRIMARY KEY : {pk_constraint} ')
                    column_defs.append(pk_constraint)

                create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
                    sql.Identifier(schema, table_name),
                    sql.SQL(", ").join(column_defs)
                )

                cur.execute(create_table_query)

                insert_cols = sql.SQL(', ').join(map(sql.Identifier, columns_names))
                insert_vals = sql.SQL(', ').join(sql.Placeholder() * len(columns_names))

                update_cols = [
                    sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col))
                    for col in columns_names if col not in partition_keys
                ]

                upsert_query = sql.SQL("""
                    INSERT INTO {} ({})
                    VALUES ({})
                    ON CONFLICT ({})
                    DO UPDATE SET {};
                    """).format(
                        sql.Identifier(schema, table_name),
                        insert_cols,
                        insert_vals,
                        sql.SQL(", ").join(map(sql.Identifier, partition_keys)),
                        sql.SQL(", ").join(update_cols)
                    )
                
                for row in rows:
                    values = [str(row[col]) for col in columns_names]
                    cur.execute(upsert_query, values)
                                       
            conn.commit()
            self.logger.info(f"Upsert complete for table {database}.{schema}.{table_name} with {len(rows)} rows updated.")