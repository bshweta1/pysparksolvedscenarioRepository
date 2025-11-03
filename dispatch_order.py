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
spark = SparkSession.builder.getOrCreate()
# Need the dates when the status gets changed like ordered to dispatched
# from pyspark.sql import SparkSession
# spark=SparkSession.builder.appName("dispachorder").getOrCreate()
data=[(1, "1-Jan", "Ordered"),
      (1, "2-Jan", "dispatched"),
      (1, "3-Jan", "dispatched"),
      (1, "4-Jan", "Shipped"),
      (1, "5-Jan", "Shipped"),
      (1, "6-Jan", "Delivered"),
      (2, "1-Jan", "Ordered"),
      (2, "2-Jan", "dispatched"),
      (2, "3-Jan", "shipped")]
schema=["orderid","status_date","status"]
df=spark.createDataFrame(data,schema=schema)
# df.show()

from pyspark.sql.functions import col

df = df.alias("o1").join(
    df.alias("o2"),
    (col("o1.orderid") == col("o2.orderid")) &
    (col("o1.status_date") != col("o2.status_date")),
    "inner"
).select(
    col("o2.orderid"),
    col("o2.status_date"),
    col("o1.status"),
    col("o2.status")
)



df_status = df.select(
    col("o2.orderid"),
    col("o2.status_date"),
    col("o2.status")
).filter(
    (col("o1.status") != col("o2.status")) &
    (col("o2.status") == "dispatched")).distinct()


df_status.show()
# ==================================optimise way-by window function===================================================
# Define a window partitioned by orderid, ordered by status_date
windowSpec = Window.partitionBy("orderid").orderBy("status_date")

# Get previous status per order
df_with_prev = df.withColumn("prev_status", lag("status").over(windowSpec))

# Filter only where previous status is different and current status is "dispatched"
df_status = df_with_prev.filter(
    (col("status") != col("prev_status")) &
    (col("status") == "dispatched")
).select(
    "orderid",
    "status_date",
    "status"
).distinct()

df_status.show()