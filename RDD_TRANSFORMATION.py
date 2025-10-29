import os
import urllib.request
import ssl
from collections import namedtuple
from collections.abc import Collection
from itertools import groupby

import pyspark.sql
from encodings.aliases import aliases
from typing import NamedTuple

from scenario1 import RDDLIS

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
# =====================================================================
#HAVING A LIST
l=[1,2,3,4,5,6,]
# WE USE PARALLELIZE METHOD TO CONVERT LIST TO RDD
#STEP1: CREATE LIST TO RDD
RDD_L=sc.parallelize(l)
print(RDD_L.collect())
# ============================================

#add 2 to each element of rdd. for that we use map which applies function to each element
# and lambda function which is python anonymus function.it can take any no. of arguments but single expression
ADD_L=RDD_L.map(lambda x : x+2)
print(ADD_L.collect())

# filter the rdd element which are greater than 2
#======================================
FilRDD=RDD_L.filter(lambda x : x>2)
print(FilRDD.collect())

#===============
# string rdd
listr=['analytics','Dataengineer','Datascientist']
strRdd=sc.parallelize(listr)
print(strRdd.collect())

#============================
#replace Rdd value 'analytics' with 'data'
ReplaceRDD=RDD_L.map(lambda x : x.replace('analytics','data'))
print(ReplaceRDD.collect())

#=============================================
#remove data from rdd
RDDremove =RDD_L.map(lambda x : x.replace("data",""))
print(RDDremove.collect())
#===================================================
#concatenate rdd with data
conRdd=RDD_L.map(lambda x : x+"data")
print(RDD_L.collect())
#===================================================
# check analytics present in rdd
InRdd=RDD_L.map(lambda x: "analytics" in x)
print(RDD_L.collect())
#======================================================
