FROM python:3.9.1

RUN request
RUN pip install pandas sqlalchemy psycopg2 

WORKDIR /app 
COPY ingest_data.py ingest_data.py 

ENTRYPOINT [ "python", "ingest_data.py" ]

## docker run -it -e POSTGRES_USER="root"  -e POSTGRES_PASSWORD="root" -e POSTGRES_DB="ny_taxi"  -v C:/Users/PC/Documents/Project/Yellow_trips/Yellow-Trips-Data-Pipeline/01_setup/docker_sql/ny_taxi_postgres_data:/var/lib/postgresql/data -p 5432:5432 postgres:13