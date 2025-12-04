WITH dates AS
    (SELECT generate_series('1990-01-01'::date, '2050-12-31'::date, '1 day'::interval) as date_ts)

   , dates_enriched AS
    (SELECT date_ts
          , to_char(date_ts, 'YYYY-MM-DD')                                     as date_form1
          , to_char(date_ts, 'DD-MM-YYYY')                                     as date_form2
          , to_char(date_ts, 'DD.MM.YYYY')                                     as date_form3
          , to_char(date_ts, 'DDMMYYYY')                                       as date_form4
          , to_char(date_ts, 'YYYY/MM/DD')                                     as date_form5
          , to_char(date_ts, 'DD/MM/YYYY')                                     as date_form6
          , to_char(date_ts, 'DD.MM.YY')                                       as date_form7
          , EXTRACT(YEAR FROM date_ts)                                         as year
          , TO_CHAR(date_ts, 'YY')                                             as year_short
          , EXTRACT(MONTH FROM date_ts)                                        as month_num
          , to_char(date_ts, 'MM')                                             as month_num_str
          , TO_CHAR(date_ts, 'Month')                                          as month_name_en
          , TO_CHAR(date_ts, 'Mon')                                            as month_name_en_short
          , CASE
                WHEN EXTRACT(MONTH FROM date_ts) = 1 THEN 'Январь'
                WHEN EXTRACT(MONTH FROM date_ts) = 2 THEN 'Февраль'
                WHEN EXTRACT(MONTH FROM date_ts) = 3 THEN 'Март'
                WHEN EXTRACT(MONTH FROM date_ts) = 4 THEN 'Апрель'
                WHEN EXTRACT(MONTH FROM date_ts) = 5 THEN 'Май'
                WHEN EXTRACT(MONTH FROM date_ts) = 6 THEN 'Июнь'
                WHEN EXTRACT(MONTH FROM date_ts) = 7 THEN 'Июль'
                WHEN EXTRACT(MONTH FROM date_ts) = 8 THEN 'Август'
                WHEN EXTRACT(MONTH FROM date_ts) = 9 THEN 'Сентябрь'
                WHEN EXTRACT(MONTH FROM date_ts) = 10 THEN 'Октябрь'
                WHEN EXTRACT(MONTH FROM date_ts) = 11 THEN 'Ноябрь'
                WHEN EXTRACT(MONTH FROM date_ts) = 12 THEN 'Декабрь'
            END                                                                as month_name_ru
          , CASE
                WHEN EXTRACT(MONTH FROM date_ts) = 1 THEN 'Янв'
                WHEN EXTRACT(MONTH FROM date_ts) = 2 THEN 'Фев'
                WHEN EXTRACT(MONTH FROM date_ts) = 3 THEN 'Март'
                WHEN EXTRACT(MONTH FROM date_ts) = 4 THEN 'Апр'
                WHEN EXTRACT(MONTH FROM date_ts) = 5 THEN 'Май'
                WHEN EXTRACT(MONTH FROM date_ts) = 6 THEN 'Июнь'
                WHEN EXTRACT(MONTH FROM date_ts) = 7 THEN 'Июль'
                WHEN EXTRACT(MONTH FROM date_ts) = 8 THEN 'Авг'
                WHEN EXTRACT(MONTH FROM date_ts) = 9 THEN 'Сен'
                WHEN EXTRACT(MONTH FROM date_ts) = 10 THEN 'Окт'
                WHEN EXTRACT(MONTH FROM date_ts) = 11 THEN 'Нояб'
                WHEN EXTRACT(MONTH FROM date_ts) = 12 THEN 'Дек'
            END                                                                as month_name_ru_short
          , CASE
                WHEN EXTRACT(MONTH FROM date_ts) = 1 THEN 'Января'
                WHEN EXTRACT(MONTH FROM date_ts) = 2 THEN 'Февраля'
                WHEN EXTRACT(MONTH FROM date_ts) = 3 THEN 'Марта'
                WHEN EXTRACT(MONTH FROM date_ts) = 4 THEN 'Апреля'
                WHEN EXTRACT(MONTH FROM date_ts) = 5 THEN 'Мая'
                WHEN EXTRACT(MONTH FROM date_ts) = 6 THEN 'Июня'
                WHEN EXTRACT(MONTH FROM date_ts) = 7 THEN 'Июля'
                WHEN EXTRACT(MONTH FROM date_ts) = 8 THEN 'Августа'
                WHEN EXTRACT(MONTH FROM date_ts) = 9 THEN 'Сентября'
                WHEN EXTRACT(MONTH FROM date_ts) = 10 THEN 'Октября'
                WHEN EXTRACT(MONTH FROM date_ts) = 11 THEN 'Ноября'
                WHEN EXTRACT(MONTH FROM date_ts) = 12 THEN 'Декабря'
            END                                                                as month_name_ru_rod
          , EXTRACT(QUARTER FROM date_ts)                                      as quarter_num
          , EXTRACT(QUARTER FROM date_ts) || 'Q' || EXTRACT(YEAR FROM date_ts) as quarter_id
          , EXTRACT(QUARTER FROM date_ts) || 'Q' || TO_CHAR(date_ts, 'YY')     as quarter_id_short
          , EXTRACT(WEEK FROM date_ts)                                         as iso_week            -- ISO 8601 standard defines weeks starting on Monday, and the first week of a year is the one containing January 4th.
          , EXTRACT(isoyear FROM date_ts)                                      as iso_year
          , to_char(date_ts, 'IYYY-IW')                                        AS iso_year_week
          , EXTRACT(WEEK FROM date_ts) || 'W' || EXTRACT(isoyear FROM date_ts) as iso_year_week1
          , EXTRACT(DAY FROM date_ts)                                          as day_of_month_num
          , EXTRACT(DOY FROM date_ts)                                          as day_of_year_num
          , EXTRACT(DOW FROM date_ts)                                          as day_of_week_num     -- starts with 0 from Sun
          , EXTRACT(ISODOW FROM date_ts)                                       as day_of_week_num_iso -- start with 1 from Mon
          , to_char(date_ts, 'Day')                                            AS day_of_week_name_en
          , to_char(date_ts, 'Dy')                                             AS day_of_week_name_en_short
          , CASE
                WHEN EXTRACT(ISODOW FROM date_ts) = 1 THEN 'Понедельник'
                WHEN EXTRACT(ISODOW FROM date_ts) = 2 THEN 'Вторник'
                WHEN EXTRACT(ISODOW FROM date_ts) = 3 THEN 'Среда'
                WHEN EXTRACT(ISODOW FROM date_ts) = 4 THEN 'Четверг'
                WHEN EXTRACT(ISODOW FROM date_ts) = 5 THEN 'Пятница'
                WHEN EXTRACT(ISODOW FROM date_ts) = 6 THEN 'Суббота'
                WHEN EXTRACT(ISODOW FROM date_ts) = 7 THEN 'Воскресенье'
            END                                                                as day_of_week_name_ru
          , CASE
                WHEN EXTRACT(ISODOW FROM date_ts) = 1 THEN 'Пн'
                WHEN EXTRACT(ISODOW FROM date_ts) = 2 THEN 'Вт'
                WHEN EXTRACT(ISODOW FROM date_ts) = 3 THEN 'Ср'
                WHEN EXTRACT(ISODOW FROM date_ts) = 4 THEN 'Чт'
                WHEN EXTRACT(ISODOW FROM date_ts) = 5 THEN 'Пт'
                WHEN EXTRACT(ISODOW FROM date_ts) = 6 THEN 'Сб'
                WHEN EXTRACT(ISODOW FROM date_ts) = 7 THEN 'Вс'
            END                                                                as day_of_week_name_short
     FROM dates)

INSERT
INTO public.dates_info (date_ts, date_form1, date_form2, date_form3, date_form4, date_form5, date_form6, date_form7,
                        year, year_short, month_num, month_num_str, month_name_en, month_name_en_short, month_name_ru,
                        month_name_ru_short, month_name_ru_rod, quarter_num, quarter_id, quarter_id_short, iso_week,
                        iso_year, iso_year_week, iso_year_week1, day_of_month_num, day_of_year_num, day_of_week_num,
                        day_of_week_num_iso, day_of_week_name_en, day_of_week_name_en_short, day_of_week_name_ru,
                        day_of_week_name_short)
SELECT date_ts,
       date_form1,
       date_form2,
       date_form3,
       date_form4,
       date_form5,
       date_form6,
       date_form7,
       year,
       year_short,
       month_num,
       month_num_str,
       month_name_en,
       month_name_en_short,
       month_name_ru,
       month_name_ru_short,
       month_name_ru_rod,
       quarter_num,
       quarter_id,
       quarter_id_short,
       iso_week,
       iso_year,
       iso_year_week,
       iso_year_week1,
       day_of_month_num,
       day_of_year_num,
       day_of_week_num,
       day_of_week_num_iso,
       day_of_week_name_en,
       day_of_week_name_en_short,
       day_of_week_name_ru,
       day_of_week_name_short
FROM dates_enriched;








