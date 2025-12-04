import xml.etree.ElementTree as ElementTree
from datetime import datetime

import psycopg2

import config.variables as var
import config.secrets as secrets

import requests

start_year = 2013
end_year = 2030
location = 'ru'
format_string = "%Y.%m.%d"
type_dict = {'1': 'Нерабочий день', '2': 'Рабочий сокращенный день', '3': 'Рабочий день'}


def get_date_for_day(day_str, year):
    day_date_str = f'{year}.{day_str}'
    date = datetime.strptime(day_date_str, format_string)

    return date


def get_day_dict(day, year, holidays_dict):
    date_dict = {}
    day_str = day.get('d')
    date_ts = get_date_for_day(day_str, year)

    type_code = day.get('t')
    day_type = type_dict.get(type_code)

    holiday_code = day.get('h')
    holiday_name = holidays_dict.get(holiday_code)

    date_dict['date'] = date_ts
    date_dict['type'] = day_type
    date_dict['holiday'] = holiday_name

    return date_dict


def get_dates_for_year(year):
    xml_url = f'https://xmlcalendar.ru/data/{location}/{year}/calendar.xml'
    dates_list = []
    holidays_dict = {}
    try:
        response = requests.get(xml_url)
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
        xml_data = response.content
    except requests.exceptions.RequestException as e:
        print(f"Error fetching XML from URL: {e}")

        return None

    root_element = ElementTree.fromstring(xml_data)

    if root_element is not None:
        for holidays in root_element.findall('holidays'):
            for holiday in holidays.findall('holiday'):
                holidays_dict[holiday.get('id')] = holiday.get('title')

        for days in root_element.findall('days'):
            for day in days.findall('day'):
                date_dict = get_day_dict(day, year, holidays_dict)
                dates_list.append(date_dict)

    return dates_list


def get_holidays():
    dates_list = []
    for year in range(start_year, end_year + 1):
        year_dates_list = get_dates_for_year(year)
        if year_dates_list is not None:
            dates_list.extend(year_dates_list)
    return dates_list

def insert_holidays_data(cur, table, date_ts, date_type, holiday_name):
    cur.execute(
        f"""
            INSERT INTO {table} (date_ts, date_type, holiday_name)
            VALUES (%(date_ts)s, %(date_type)s, %(holiday_name)s)
            ON CONFLICT (date_ts) DO UPDATE
                SET date_type    = EXCLUDED.date_type,
                    holiday_name  = EXCLUDED.holiday_name;
            """,
        {
            "date_ts": date_ts,
            "date_type": date_type,
            "holiday_name": holiday_name
        }
    )

def get_dwh_conn():
    conn = psycopg2.connect(
        f"host='{secrets.dwh_host}' port='{secrets.dwh_port}' dbname='{secrets.dwh_db_name}' user='{secrets.dwh_user}' password='{secrets.dwh_password}'")

    return conn

def load_holidays():
    holidays = get_holidays()
    conn = get_dwh_conn()
    with conn:
        cursor = conn.cursor()
        for holiday in holidays:
            insert_holidays_data(cursor, var.dwh_holidays_table, holiday['date'], holiday['type'], holiday['holiday'])

def get_query_string_from_file(file_path):
    with open(file_path, 'r') as file:
        query_string = file.read()

    return query_string

def execute_query(query_string):
    conn = get_dwh_conn()
    with conn:
        cursor = conn.cursor()
        cursor.execute(query_string)

def init_holidays_table():
    query_file_path = 'sql/init_holidays_table.sql'
    query_string = get_query_string_from_file(query_file_path)
    execute_query(query_string)

def init_dates_table():
    query_file_path = '../sql/init_dates_info_table.sql'
    query_string = get_query_string_from_file(query_file_path)
    execute_query(query_string)

def load_dates_table():
    query_file_path = '../sql/load_dates_info_table.sql'
    query_string = get_query_string_from_file(query_file_path)
    execute_query(query_string)



# init_holidays_table()
# load_holidays()
# init_dates_table()
load_dates_table()
