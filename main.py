import loaders.prod_calendar as prod_calendar

prod_calendar.init_holidays_table()
prod_calendar.load_holidays()
prod_calendar.init_dates_table()
prod_calendar.load_dates_table()
