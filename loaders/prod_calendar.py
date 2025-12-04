import xml.etree.ElementTree as ElementTree
from datetime import datetime

import requests

import config.variables as var
import utils.dwh_util as dwh_util


def get_date_for_day(day_str, year):
    day_date_str = f'{year}.{day_str}'
    date = datetime.strptime(day_date_str, var.format_string)

    return date


def get_day_dict(day, year, holidays_dict):
    date_dict = {}
    day_str = day.get('d')
    date_ts = get_date_for_day(day_str, year)

    type_code = day.get('t')
    day_type = var.type_dict.get(type_code)

    holiday_code = day.get('h')
    holiday_name = holidays_dict.get(holiday_code)

    moved_from_holiday = day.get('f')
    moved_from_holiday_date = None
    if moved_from_holiday:
        moved_from_holiday_date = get_date_for_day(moved_from_holiday, year)

    date_dict['date'] = date_ts
    date_dict['type'] = day_type
    date_dict['holiday'] = holiday_name
    date_dict['moved_from_holiday_date'] = moved_from_holiday_date

    return date_dict


def get_dates_for_year(year):
    xml_url = f'{var.holidays_url}/data/{var.location}/{year}/calendar.xml'
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
    for year in range(var.start_year, var.end_year + 1):
        year_dates_list = get_dates_for_year(year)
        if year_dates_list is not None:
            dates_list.extend(year_dates_list)
    return dates_list


def insert_holidays_data(cur, table, date_ts, date_type, holiday_name, moved_from_holiday_date):
    cur.execute(
        f"""
            INSERT INTO {table} (date_ts, date_type, holiday_name, moved_from_holiday_date)
            VALUES (%(date_ts)s, %(date_type)s, %(holiday_name)s, %(moved_from_holiday_date)s)
            ON CONFLICT (date_ts) DO UPDATE
                SET date_type    = EXCLUDED.date_type,
                    holiday_name  = EXCLUDED.holiday_name,
                    moved_from_holiday_date = EXCLUDED.moved_from_holiday_date;
            """,
        {
            "date_ts": date_ts,
            "date_type": date_type,
            "holiday_name": holiday_name,
            "moved_from_holiday_date": moved_from_holiday_date,
        }
    )


def load_holidays():
    holidays = get_holidays()
    print(holidays)
    conn = dwh_util.get_dwh_conn()
    with conn:
        cursor = conn.cursor()
        for holiday in holidays:
            insert_holidays_data(cursor, var.dwh_holidays_table, holiday['date'], holiday['type'], holiday['holiday'],
                                 holiday['moved_from_holiday_date'])

def init_holidays_table():
    query_file_path = var.init_holidays_file_path
    query_string = dwh_util.get_query_string_from_file(query_file_path)
    dwh_util.execute_query(query_string)


def init_dates_table():
    query_file_path = var.init_dates_file_path
    query_string = dwh_util.get_query_string_from_file(query_file_path)
    dwh_util.execute_query(query_string)


def load_dates_table():
    query_file_path = var.load_dates_file_path
    query_string = dwh_util.get_query_string_from_file(query_file_path)
    dwh_util.execute_query(query_string)
