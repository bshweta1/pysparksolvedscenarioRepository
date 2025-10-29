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
# =====================================================================
# inputRdd
rawdata=["State->TN~City->Chennai",
         "State->Kerala~City->Trivandrum"]
# step1: conver list to rdd
inputRdd=sc.parallelize(rawdata)
print(inputRdd.collect())

#===================================================
#step 2 : split the string from ~
splitRDD=inputRdd.flatMap(lambda x :x.split("~"))
print(splitRDD.collect())
#============================================
#step 3: extract city from rdd
cityrdd=splitRDD.filter(lambda x : "City" in x)
print(cityrdd.collect())
#=====================================================
#step4 extract state from rdd
staterdd=splitRDD.filter(lambda x :"State" in x)
print(staterdd.collect())
#====================================================
#step5 remove city-> and state->
finalcityrdd=cityrdd.map(lambda x : x.replace("City->", ""))
print(finalcityrdd.collect())
finalstaterdd=staterdd.map(lambda x:x.replace("State->",""))
print(finalstaterdd.collect())
#==============================
#step 6 save the output in textfil;e
cityrdd.saveAsTextFile("file:///E:/city_output")
staterdd.saveAsTextFile("file:///E:/state_output")



