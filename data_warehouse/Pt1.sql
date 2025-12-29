-- Create external tablE in new schema pt
CREATE SCHEMA `nytaxitrips.pt`
OPTIONS (
  location = "EU"
);


CREATE OR REPLACE EXTERNAL TABLE nytaxitrips.pt.external_table
OPTIONS (
 FORMAT = 'PARQUET',
 URIS= ['gs://nytaxitrips-bucket/raw/yellow*.parquet']
);

CREATE OR REPLACE TABLE nytaxitrips.pt.yellow_trips
AS 
SELECT * FROM `nytaxitrips.pt.external_table`;

