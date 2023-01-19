# Sample Spark-App for Fancy DigiMart Co.
Application extracts data from customer data files, transforms and groups it for reporting purposes saving it to custom report as csv file and custom report to PostgreSQL database.

### Architecture Decsription

Client uploads data to HDFS folder on cluster of his choice. Required file format - parquet.
Script reads set of files (impressions and clicks) to separate Dataframes and eventualy joins it by date, hour aggregating sum of impressions and clicks.
Resulting dataframe is written to staging folder as csv file.

For testing purposes Docker is used to run PostgreSQL database with a simple PostgreSQL adminer client running on localhost:8080.

Connection to PostgreSQL DB is psycopg2 and table is created.
Since spark is creating suplementary files while exporting data function is defined to grab the csv file name Data from staging folder and csv file is loaded into PostgreSQL database.

In final step, source data is deleted.

NOTE: App is ment to be used for testing, thus run it on Spark running on local machine. For cluster mode, update config file to use YARN.


### Technologies used
- Apache Spark
- Python
- SQL
- Docker

### Running the application

git clone the application.
Pyspark library and all its dependancies are exceeding github allowed space limit. Thus dependancies file should be created manualy after cloning a repo.
In parent directory run:
```
pip install -t dependencies -r requirements.txt
```
This commnad will create dependancies folder, compress it with zip and name dependancies.zip.
Zip 

If testing: install docker and docker-compose, cd to docker folder and run
```
docker-compose up -d
```

Connect to Spark Cluster and run:
```
spark-submit \
    --driver-memory 2g \
    --executor-memory  2g \
    --py-files dependancies.zip, lib.zip \
    spark_app.py
    --source_folder /path/to/source/dir \
    --pg_host your_pg_host \
    --pg_database your_pg_database \
    --pg_table your_pg_table \
    --pgusername your_pg_username \
    --pg_password your_pg_password
```

NOTE: App is ment to be used for testing, thus run it on Spark running on local machine. For cluster mode, update config file to use YARN.
### Unit Testing
Unit test tests for data presence and consistency, i.e. are there parquet files in source directory and is the report generated not empty.
### Things to consider
Unit Testing for functions, testing of ariving data quality, orchestration, incremental load actions, to append new data, modify if exists.
To have a propper testing setup, make a Docker container with spark and dependancies installed and run app from there.
