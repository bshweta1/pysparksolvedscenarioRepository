import os
import urllib.request
import ssl

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

hadoop_home = os.path.abspath("hadoop")   # <-- absolute path
os.makedirs(os.path.join(hadoop_home, "bin"), exist_ok=True)

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
conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

#spark.read.format("csv").load("data/test.txt").toDF("Success").show(20, False)

##################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ####################################y

data1 = [
    (1, "abc", 31, "abc@gmail.com"),
    (2, "def", 23, "defyahoo.com"),
    (3, "xyz", 26, "xyz@gmail.com"),
    (4, "qwe", 34, "qwegmail.com"),
    (5, "iop", 24, "iop@gmail.com"),
]

schema=["id","name" ,"age" ,"email"]


data2 = [
    (11, "jkl", 22, "abc@gmail.com", 1000),
    (12, "vbn", 33, "vbn@yahoo.com", 3000),
    (13, "wer", 27, "wer", 2000),
    (14, "zxc", 30, "zxc.com", 2000),
    (15, "lkj", 29, "lkj@outlook.com", 2000),
]
schema2=["id","name" ,"age" ,"email","salary"]
df1=spark.createDataFrame(data1,schema=schema)
df2=spark.createDataFrame(data2,schema=schema2)

# Add missing 'salary' column to df1 so both have same schema
df1 = df1.withColumn("salary", lit(1000))

# Now union by name
df3 = df1.unionByName(df2)

print(" Combined DataFrame:")
df3.show()

# Filter valid emails without regex — basic validation
df_valid = df3.filter(
    (col("email").contains("@")) & (col("email").contains("."))
)

print(" Valid Emails:")
df_valid.show()