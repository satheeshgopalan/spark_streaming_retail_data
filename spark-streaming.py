
# Import Dependencies 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import from_json
from pyspark.sql.window import Window

spark = SparkSession  \
        .builder  \
        .appName("StructuredSocketRead")  \
        .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# Read Input 
raw_order = spark  \
        .readStream  \
        .format("kafka")  \
        .option("kafka.bootstrap.servers","18.211.252.152:9092")  \
        .option("subscribe","real-time-project")  \
        .option("startingOffsets", "latest")  \
        .load()

# Define Schema
JSON_Schema = StructType() \
        .add("invoice_no", LongType()) \
	    .add("country",StringType()) \
        .add("timestamp", TimestampType()) \
        .add("type", StringType()) \
        .add("total_items",IntegerType())\
        .add("is_order",IntegerType()) \
        .add("is_return",IntegerType()) \
        .add("items", ArrayType(StructType([
        StructField("SKU", StringType()),
        StructField("title", StringType()),
        StructField("unit_price", FloatType()),
        StructField("quantity", IntegerType()) 
        ])))


order_stream = raw_order.select(from_json(col("value").cast("string"), JSON_Schema).alias("data")).select("data.*")


# Utility functions

def is_a_order(type):
   if type=="ORDER":
       return 1
   else:
       return 0

def is_a_return(type):
   if type=="RETURN":
       return 1
   else:
       return 0
       
def total_item_count(items):
   total_count = 0
   for item in items:
       total_count = total_count + item['quantity']
   return total_count

def total_cost(items,type):
   total_price = 0
   for item in items:
       total_price = total_price + item['unit_price'] * item['quantity']
   if type=="RETURN":
       return total_price * (-1)
   else:
       return total_price



# Define the UDFs with the utility functions
is_order = udf(is_a_order, IntegerType())
is_return = udf(is_a_return, IntegerType())
add_total_item_count = udf(total_item_count, IntegerType())
add_total_cost = udf(total_cost, FloatType())


# Console Output
order_extended_stream = order_stream \
       .withColumn("total_items", add_total_item_count(order_stream.items)) \
       .withColumn("total_cost", add_total_cost(order_stream.items,order_stream.type)) \
       .withColumn("is_order", is_order(order_stream.type)) \
       .withColumn("is_return", is_return(order_stream.type))


order_query_console = order_extended_stream \
       .select("invoice_no", "country", "timestamp","type","total_items","total_cost","is_order","is_return") \
       .writeStream \
       .outputMode("append") \
       .format("console") \
       .option("truncate", "false") \
       .trigger(processingTime="1 minute") \
       .start()

# Calculate time based KPIs
agg_time = order_extended_stream \
    .withWatermark("timestamp","1 minutes") \
    .groupby(window("timestamp", "1 minute")) \
    .agg(sum("total_cost").alias("total_volume_of_sales"),
        avg("total_cost").alias("average_transaction_size"),
        avg("is_Return").alias("rate_of_return")) \
    .select("window.start","window.end","total_volume_of_sales","average_transaction_size","rate_of_return")

# Calculate time and country based KPIs
agg_time_country = order_extended_stream \
    .withWatermark("timestamp", "1 minutes") \
    .groupBy(window("timestamp", "1 minutes"), "country") \
    .agg(sum("total_cost").alias("total_volume_of_sales"),
        count("invoice_no").alias("OPM"),
        avg("is_Return").alias("rate_of_return")) \
    .select("window.start","window.end","country", "OPM","total_volume_of_sales","rate_of_return")


# Write time based KPI values
ByTime = agg_time.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("path", "timeKPI/") \
    .option("checkpointLocation", "timeKPI/cp/") \
    .trigger(processingTime="1 minutes") \
    .start()


# Write time and country based KPI values
ByTime_country = agg_time_country.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("path", "time_countryKPI/") \
    .option("checkpointLocation", "time_countryKPI/cp/") \
    .trigger(processingTime="1 minutes") \
    .start()

ByTime_country.awaitTermination()
