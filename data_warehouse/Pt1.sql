-- Create external tablE in new schema pt
-- Create external tablE in new schema pt
CREATE SCHEMA `nytaxitrips.pt`
OPTIONS (
  location = "EU"
);


-- Create table for yellow taxi data
CREATE OR REPLACE EXTERNAL TABLE nytaxitrips.nytaxi_data.yellow_data
OPTIONS (
 FORMAT = 'PARQUET',
 URIS= ['gs://nytaxitrips-bucket/raw/yellow*.parquet']
);


-- create table for green taxi data 
CREATE OR REPLACE EXTERNAL TABLE nytaxitrips.nytaxi_data.green_data
OPTIONS (
 FORMAT = 'CSV',
 URIS= ['gs://nytaxitrips-bucket/raw/green_tripdata_2020-01.parquet']
);


-- create table for  FHV data 
CREATE OR REPLACE EXTERNAL TABLE nytaxitrips.nytaxi_data.fhv_data
OPTIONS (
 FORMAT = 'PARQUET',
 URIS= ['gs://nytaxitrips-bucket/raw/fhv*.parquet']
);


-- create table for  taxi zones 
CREATE OR REPLACE EXTERNAL TABLE nytaxitrips.nytaxi_data.zone_data
OPTIONS (
 FORMAT = 'CSV',
 URIS= ['gs://nytaxitrips-bucket/raw/taxi_zone*.parquet']
);


-- Create a (regular/materialized) table in BQ using the Yellow Taxi Trip Records (do not partition or cluster this table)
CREATE OR REPLACE TABLE nytaxitrips.pt.yellow_trips
AS 
SELECT 
  VendorID,
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  passenger_count,
  trip_distance,
  RatecodeID,
  store_and_fwd_flag,
  PULocationID,
  DOLocationID,
  payment_type,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  congestion_surcharge
FROM `nytaxitrips.pt.external_table`;




