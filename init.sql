CREATE SCHEMA IF NOT EXISTS digitraffic;

CREATE TABLE IF NOT EXISTS digitraffic.station (
    passenger_traffic       boolean,
    type                    text,
    station_name            text,
    station_short_code      text,
    station_uic_code        integer,
    country_code            text,
    longitude               numeric,
    latitude                numeric
);

CREATE TABLE IF NOT EXISTS digitraffic.operator (
    operator_name           text,
    operator_short_code     text,
    operator_uic_code       integer
);

CREATE TABLE IF NOT EXISTS digitraffic.cause_code (
    category_code           text,
    category_name           text,
    valid_from              date,
    valid_to                date
);

CREATE TABLE IF NOT EXISTS digitraffic.train_type (
    name                    text,
    train_category          text
);
