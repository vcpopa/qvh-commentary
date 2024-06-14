from contextlib import contextmanager
import urllib
from typing import Iterator,Literal
import pandas as pd
from sqlalchemy import create_engine,text
from sqlalchemy.engine import Engine
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from exc import KeyVaultError,SQLError  # pylint: disable=import-error


def get_credential(name: str) -> str:
    """
    Retrieves a credential value from Azure KeyVault

    Parameters:
    name (str): The name of the credential inside KeyVault

    Returns:
    - credential (str)

    Raises:
    - KeyVaultError: If credential is not found or is empty
    """
    kv_uri = "https://qvh-keyvault.vault.azure.net/"
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=kv_uri, credential=credential)
    credential_value = client.get_secret(name).value
    if not credential_value:
        raise KeyVaultError("Credential value not found, please check KeyVault")
    return credential_value




@contextmanager
def connection(db_name:Literal['public-dataflow-connectionstring',"public-dos-connectionstring"]) -> Iterator[Engine]:
    """
    Context manager to create and close a database connection.

    Loads database connection parameters from environment variables, creates
    a SQLAlchemy engine, and yields the engine. The engine is closed when the
    context is exited.

    Returns:
        Iterator[Engine]: An iterator that yields a SQLAlchemy Engine.
    """

    connstr = get_credential(db_name)
    params = urllib.parse.quote_plus(connstr)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    try:
        yield engine
    finally:
        engine.dispose()

def read_sql(query: str,db_name:Literal['public-dataflow-connectionstring',"public-dos-connectionstring"]) -> pd.DataFrame:
    """
    Executes a SQL query and returns the result as a Pandas DataFrame.

    Args:
        query (str): The SQL query to execute.

    Returns:
        pd.DataFrame: A DataFrame containing the query results.
    """
    with connection(db_name=db_name) as conn:
        return pd.read_sql(sql=query, con=conn)

def execute_stored_proc(proc:str) -> None:
    with connection(db_name='public-dataflow-connectionstring') as conn:
        with conn.connect() as cursor:
            try:
                transaction=cursor.begin()
                cursor.execute(text(proc))
                transaction.commit()
            except:
                transaction.rollback()
                raise SQLError("Couldn't execute stored proc")