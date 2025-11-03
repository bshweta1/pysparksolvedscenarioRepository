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
from pyspark.sql import SparkSession
spark=SparkSession.builder.getOrCreate()

data = [
    (1, "Mark Ray", "AB"),
    (2, "Peter Smith", "CD"),
    (1, "Mark Ray", "EF"),
    (2, "Peter Smith", "GH"),
    (2, "Peter Smith", "CD"),
    (3, "Kate", "IJ"),
]
schema=["id","name","address"]

df=spark.createDataFrame(data,schema=schema)
df.show()
address_df=df.groupby(col("id"),col("name")).agg(collect_set("address").alias("address")).orderBy("id")

address_df.show()