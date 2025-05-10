FROM python:3.9.1

RUN pip install pandas 

WORKDIR /app 
COPY pipeline.py pipeline.py 

ENTRYPOINT [ "bash" ]

## docker run -it -e POSTGRES_USER="root"  -e POSTGRES_PASSWORD="root" -e POSTGRES_DB="ny_taxi"  -v C:/Users/PC/Documents/Project/Zoomcamp_project/Data-Engineering-Zoomcamp/ny_taxi_postgres_data:/var/lib/postgresql/data -p 5432:5432 postgres:13