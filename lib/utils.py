import configparser
from os import listdir
from pyspark import SparkConf

def get_spark_app_config():
    '''
    Function parses configuration parameters from 
    spark.conf file
    '''
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read('spark.conf')
    
    for (k, v) in config.items('SPARK_APP_CONFIGS'):
        spark_conf.set(k, v)
    return spark_conf
    
def extract_timestamp(file_name):
    '''
    Function takes a source file name as a string 
    and returns it time stamp as string in 
    format YYYY-mm-dd hh:mm
    '''
    extract = file_name.split('_')[5]
    year = extract[:4]
    month = extract[4:6]
    day = extract[6:8]
    hours = extract[8:10]
    minutes = extract[10:12]
    return f'{year}-{month}-{day} {hours}:{minutes}'


def find_files_of_type(path, suffix):
    '''
    Function takes a path and a suffix of the file type
    and returns list of file names in path directory
    '''
    filenames = listdir(path)
    return [filename for filename in filenames if filename.endswith(suffix)]