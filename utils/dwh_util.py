import psycopg2
import config.secrets as secrets

def get_dwh_conn():
    conn = psycopg2.connect(
        f"host='{secrets.dwh_host}' port='{secrets.dwh_port}' dbname='{secrets.dwh_db_name}' user='{secrets.dwh_user}' password='{secrets.dwh_password}'")

    return conn

def get_query_string_from_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        query_string = file.read()

    return query_string


def execute_query(query_string):
    conn = get_dwh_conn()
    with conn:
        cursor = conn.cursor()
        cursor.execute(query_string)