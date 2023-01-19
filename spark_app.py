# TODO 1

from pyspark.sql import *
from pyspark.sql.functions import col, count, input_file_name, udf, to_date, hour, minute, second
from lib.utils import get_spark_app_config, extract_timestamp, find_files_of_type
import os, shutil, csv
import psycopg2
import argparse

# Parse CLI parameters from spark-submit
parser = argparse.ArgumentParser()

parser.add_argument('--source_folder', help='Source folder containing initial data')
parser.add_argument('--pg_host', help='PostgreSQL host name')
parser.add_argument('--pg_database', help='PostgreSQL database name')
parser.add_argument('--pg_table', help='PostgreSQL database table')
parser.add_argument('--pg_username', help='PostgreSQL database user name')
parser.add_argument('--pg_password', help='PostgreSQL database user password')

args = parser.parse_args()

source_folder = args.source_folder
pg_host = args.pg_host
pg_database = args.pg_database
pg_table = args.pg_table
pg_username = args.pg_username
pg_password = args.pg_password

# Initiate Spark sesion
if __name__ == '__main__':
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .enableHiveSupport() \
        .config(conf=conf) \
        .getOrCreate()


# Set UDF for extracting timestamp
udf_extract_ts = udf(extract_timestamp)

# Read impressions Dataframe
impressions_df = spark.read \
    .option('inferSchema' , 'true') \
    .parquet(f'{source_folder}/impressions*') \
    .select(col('device_settings')['user_agent']) \
    .withColumnRenamed('device_settings.user_agent', 'user_agent') \
    .withColumn('timestamp', udf_extract_ts(input_file_name())) \
    .withColumn('date', to_date(col('timestamp'))) \
    .withColumn('hour', hour(col('timestamp'))) \
    .withColumn('min', minute(col('timestamp'))) \
    .withColumn('s', second(col('timestamp')))

# Read clicks Dataframe
clicks_df = spark.read \
    .option('inferSchema' , 'true') \
    .parquet(f'{source_folder}/clicks*') \
    .select(col('device_settings')['user_agent']) \
    .withColumnRenamed('device_settings.user_agent', 'c_user_agent') \
    .withColumn('c_timestamp', udf_extract_ts(input_file_name())) \
    .withColumn('c_date', to_date(col('c_timestamp'))) \
    .withColumn('c_hour', hour(col('c_timestamp'))) \
    .withColumn('c_min', minute(col('c_timestamp'))) \
    .withColumn('c_s', second(col('c_timestamp')))

# Left join impressions and clicks, count, group by date and hour
join_df = impressions_df.join(clicks_df, (impressions_df['date'] == clicks_df['c_date']) & (impressions_df['hour'] == clicks_df['c_hour']), 'left') \
    .groupBy(col('date'), col('hour')) \
    .agg(count('hour').alias('impressions_count'),count('c_hour').alias('click_count')) \
    .orderBy('date', 'hour')

# Write report data to staging
join_df.write \
    .option('header',True) \
    .mode('overwrite') \
    .csv('./staging/')  

# TODO 2

# Connect to Docker PostgreSQL Database
# Connect to database
conn = psycopg2.connect(
    database = pg_database,
    user= pg_username,
    password = pg_password,
    host = pg_host
)

# Open cursor to perform database operation
cur = conn.cursor()

# Create table, or drop if exist for regular report updates
create_db_qry = (f'''
    DROP TABLE IF EXISTS click_rate;
    CREATE TABLE {pg_table}
    (
    datetime TIMESTAMP,
    impression_count BIGINT,
    click_count BIGINT,
    audit_loaded_datetime TIMESTAMP DEFAULT NOW()   
    )
    '''   
)
conn.commit()

# find csv file in staging directory
csv_file = find_files_of_type('./staging', '.csv')[0]

# Write custom report to Postgresql DB
with open(f'./staging/{csv_file}', 'r') as f:

    # Read csv file, skip header
    reader = csv.reader(f, delimiter=',')
    next(reader, None)

    # Create new list with specific columns
    load_list = [[x for i,x in enumerate(line) if i!=1] for line in reader]
  
# Insert values to click_rate table       
for item in load_list:
    cur.execute(f'INSERT INTO {pg_table} (datetime, impression_count, click_count) VALUES (%s, %s, %s)', item)
    
# Close communications with database
cur.close()
conn.close()

# OPTION USED JDBC CONNECTION

# join_df.write \
#     .format('jdbc') \
#     .option('url', f'jdbc:postgresql://{pg_host}:5432/{pg_database}') \
#     .option('dbtable', pg_table) \
#     .option('user', pg_user) \
#     .option('password', pg_password) \
#     .option('driver', 'com.postgresql.Driver') \
#     .save()


# Delete source files
for filename in os.listdir(source_folder):
    file_path = os.path.join(source_folder, filename)
    try:
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)
    except Exception as e:
        print(f'Failed to delete {file_path}. Reason: {e}')

# Stop Spark Session
spark.stop()

