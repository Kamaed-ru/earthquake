CREATE DATABASE dwh;

\connect dwh;

CREATE SCHEMA IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS dwh.earthquakes (
    event_id          text PRIMARY KEY,
    time_utc          timestamp,
    latitude          double precision,
    longitude         double precision,
    depth_km          double precision,
    magnitude         double precision,
    magnitude_type    text,
    place             text,
    source_network    text,
    updated_utc       timestamp
);
