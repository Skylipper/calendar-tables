DROP table if exists public.holidays;

CREATE table if not exists public.holidays
(
    id                      serial primary key,
    date_ts                 timestamp    not null unique,
    date_type               varchar(128) not null,
    holiday_name            varchar(128),
    moved_from_holiday_date timestamp
);