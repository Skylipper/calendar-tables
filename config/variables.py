import os

cwd = os.getcwd()

holidays_url = 'https://xmlcalendar.ru'
dwh_holidays_table = "public.holidays"
load_dates_file_path = f"{cwd}/sql/load_dates_info_table.sql"
init_dates_file_path = f'{cwd}/sql/init_dates_info_table.sql'
init_holidays_file_path = f'{cwd}/sql/init_holidays_table.sql'
start_year = 2013
end_year = 2030
location = 'ru'
format_string = "%Y.%m.%d"
type_dict = {'1': 'Нерабочий день', '2': 'Рабочий сокращенный день', '3': 'Рабочий день'}