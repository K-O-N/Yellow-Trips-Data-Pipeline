# Week 4 – Analytics Engineering (dbt Core)

## Overview

In **Week 4**, the focus of the Yellow Trips project shifted from data ingestion and storage to **analytics engineering**. The goal was to transform raw and staged data in the data warehouse (BigQuery) into **clean, tested, and analytics-ready models** using **dbt Core**.

This week introduced:

* dbt Core fundamentals
* Project initialization and configuration
* Modeling layers (staging, intermediate, marts)
* Use of `ref()` and Jinja templating
* Data testing and documentation
* dbt compilation and execution workflows

The end result is a **reproducible analytics pipeline** where transformations are version-controlled, testable, and well-documented.

---

## Architecture Context

At this stage, the pipeline looks like this:

1. **Raw data** stored in Google Cloud Storage (GCS)
2. **External and native tables** in BigQuery (created in Week 3)
3. **dbt Core** connects to BigQuery
4. dbt models transform data into analytics-ready tables and views
5. dbt tests validate data quality
6. dbt docs generate project documentation and lineage

---

## Tooling Used

* **dbt Core** (open-source)
* **BigQuery** (data warehouse)
* **Docker** (optional, depending on setup)
* **GitHub** (version control)

---

## Step 1: Setting Up dbt Core

### 1.1 Installing dbt Core

dbt Core was installed using Docker with the BigQuery adapter:

* Adapter: `dbt-bigquery`
* Python environment used to isolate dependencies

> dbt Core is chosen instead of dbt Cloud to ensure full control over execution and environment.

---

### 1.2 Initializing the dbt Project

The dbt project was initialized inside the existing project directory:

* First step was to make a new directory in our main project directory named `analytics_engineering`)
* Second is to create a dockerfile in the new directory using the requirements from docker for dbt-bigQuery imagine here
* Create a docker compose.yml file
```
version: '3'
services:
  nytaxi-dtc:
    build:
      context: .
      dockerfile: Dockerfile
      target: dbt-bigquery
    image: dbt/bigquery
    working_dir: /usr/app/analytics_engineering
    volumes:
      - .:/usr/app
      - ./credentials/google_credentials.json:/credentials/google_credentials.json

    environment:
      GOOGLE_APPLICATION_CREDENTIALS: /credentials/google_credentials.json
    network_mode: host
```
This step is really important. Note how the your google credentials is stored.
* Next step is to run your docker compose build: from your compose file, dbt is service is named as nytaxi-dtc. Hence replace dbt with nytaxi-dtc anytime during your modelling.
```
docker compose build
docker compose run nytaxi-dtc init
```
The above steps initiates your project and creates teh following folders. Ensure the name of your project in dbt_project.yml matches the name in your profile.yml.
```
models/
macros/
seeds/
snapshots/
tests/
profiles.yml
dbt_project.yml
```

### Using Macros and dbt packages
#### Creating a dbt Macro
Macros are defined in the macros/ directory of the dbt project. A macro is written using Jinja and allows SQL logic to be reused across multiple models. It is like a function recated for reproducibility or code.
To create one, Create a file inside the macros/ directory (for example: macros/utils.sql)
```
{#
    This macro returns the description of the payment_type 
#}
{% macro get_payment_type_description(column_name) -%}
    CASE {{column_name}}
         WHEN 1 THEN 'Credit Card'
         WHEN 2 THEN 'Cash'
         WHEN 3 THEN 'No Charge'
         WHEN 4 THEN 'Dispute'
         WHEN 5 THEN 'Unknown'
         WHEN 6 THEN 'Voided trip'
         ELSE 'EMPTY'
    END
{%- endmacro %}
```
This macro can be used in a model like this `{{ get_payment_type_description("payment_type") }} as payment_type_desc`
#### Packages
dbt packages are installed using a packages.yml file located in the root of the dbt project. dbt has so many packages that can be used during modelling. 

Packages Installation and Use Steps:
* Create a packages.yml file (if it does not already exist)
* Add the required package name and version
* Run the dbt package installation command `docker compose run nytaxi-dtc deps`
This downloads the package and makes its macros available to the project.
```
packages:
  - package: dbt-labs/dbt_utils
    version: 1.3.3
  - package: dbt-labs/codegen
    version: 0.14.0
```
Examples of pacakges used 
1. Surrogate keys `{{ dbt_utils.generate_surrogate_key(['vendorid', 'tpep_pickup_datetime']) }} as tripid,`
2. Codegen to generate yml file documentations for the models <a href="https://hub.getdbt.com/">dbt - Package hub</a>

## Testing Data Quality
Built-in dbt Tests were added to validate assumptions:

* `not_null`
* `unique`
* `accepted_values`





