import os

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

hadoop_home = os.path.abspath("hadoop")  # <-- absolute path
os.makedirs(os.path.join(hadoop_home, "bin"), exist_ok=True)

urls_and_paths = {
    "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/master/test.txt": os.path.join(data_dir, "test.txt"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/winutils.exe": os.path.join(hadoop_home, "bin",
                                                                                       "winutils.exe"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/hadoop.dll": os.path.join(hadoop_home, "bin", "hadoop.dll")
}

# Create an unverified SSL context

# ======================================================================================

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import os
import urllib.request
import ssl

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] = hadoop_home
os.environ['JAVA_HOME'] = r'C:\Users\DELL\.jdks\corretto-1.8.0_462'  #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.4 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 pyspark-shell'


conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host", "localhost").set(
    "spark.default.parallelism", "1")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()
#=======================================================================
# need to extract data from text file and convert it into the rdd and then convert it ino df

usdata = sc.textFile("dt.txt")
usdata.foreach(print)  #forech to print each element line by line

#split the data from the ,
usrdd=usdata.map(lambda x: x.split(","))
usrdd.foreach(print)
# SET SCHEMA
from collections import namedtuple
columns =namedtuple('columns',['ID', 'TDATE', 'AMOUNT', 'CATEGORY', 'PRODUCT', 'MODE'])
schemardd=usrdd.map(lambda x:columns(x[0],x[1],x[2],x[3],x[4],x[5]))
#CHECK THE VALUE IN COLUMN
prodfilrdd=schemardd.filter(lambda x: 'Gymnastic' in x.PRODUCT)
prodfilrdd.foreach(print)
#CONVERT RDD TO DF
newdf=prodfilrdd.toDF()
newdf.show()
#WRITE TO PARQUET
newdf.write.parquet(prodfilrdd)