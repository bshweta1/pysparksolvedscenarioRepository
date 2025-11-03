import os
import urllib.request
import ssl

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

hadoop_home = os.path.abspath("hadoop")   # <-- absolute path
os.makedirs(os.path.join(hadoop_home, "bin"), exist_ok=True)

urls_and_paths = {
    "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/master/test.txt":os.path.join(data_dir, "test.txt"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/winutils.exe":os.path.join(hadoop_home, "bin", "winutils.exe"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/hadoop.dll":os.path.join(hadoop_home, "bin", "hadoop.dll")
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
os.environ['JAVA_HOME'] = r'C:\Users\DELL\.jdks\corretto-1.8.0_462'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.4 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 pyspark-shell'


conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

#spark.read.format("csv").load("data/test.txt").toDF("Success").show(20, False)

data = [("001", "Monika", "Arora", 100000, "2014-02-20 09:00:00", "HR"),("002", "Niharika", "Verma", 300000, "2014-06-11 09:00:00", "Admin"),("003", "Vishal", "Singhal", 300000, "2014-02-20 09:00:00", "HR"),("004", "Amitabh", "Singh", 500000, "2014-02-20 09:00:00", "Admin"),("005", "Vivek", "Bhati", 500000, "2014-06-11 09:00:00", "Admin")]
myschema = ["workerid","firstname","lastname","salary","joiningdate","depart"]
df = spark.createDataFrame(data,schema=myschema)

#Through SQL
df.createOrReplaceTempView("worktab")
spark.sql("select a.workerid,a.firstname,a.lastname,a.salary,a.joiningdate,a.depart from worktab a, worktab b where a.salary=b.salary and a.workerid !=b.workerid").show()

#Through Spark DSL
# finaldf = df.alias("a").join(df.alias("b"), (col("a.salary") == col("b.salary")) & (col("a.workerid") != col("b.workerid")), "inner").select(col("a.workerid"), col("a.firstname"), col("a.lastname"), col("a.salary"), col("a.joiningdate"), col("a.depart")).show()


select_df=df.alias("a").join(df.alias("b"), (col("a.workerid")!=col("b.workerid")) & (col("a.salary")==col("b.salary")), "inner") \
    .select(col("a.workerid"),col("a.firstname"),col("a.lastname"),col("a.salary") ,col("a.joiningdate"),col("a.depart") )

select_df.show()
