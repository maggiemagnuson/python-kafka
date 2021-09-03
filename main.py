from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, TimestampType
from pyspark.sql.functions import from_json, col, window, desc, count

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    spark = (SparkSession
             .builder
             .master("local[2]")
             .appName("Words")
             .getOrCreate())

    spark.sparkContext.setLogLevel('warn')

    schema = StructType().add("word", StringType()).add("event_time", TimestampType())
    words = (spark
             .readStream
             .format('kafka')
             .option('kafka.bootstrap.servers', 'localhost:9092')
             .option("startingOffsets", "earliest")
             .option('subscribe', 'word-count')
             .option("failOnDataLoss", "false")
             .load())

    df = (words
          .selectExpr("CAST(value as STRING)", "CAST(timestamp AS TIMESTAMP)")
          .select(from_json(col("value"), schema).alias("data"), col("timestamp"))
          .select("data.*", "timestamp")
          )

    counts = df \
        .withWatermark("event_time", "0 seconds")\
        .groupby(window("event_time", "1 minutes"), "word") \
        .count()

    counts = counts.select("window", "word", "count").filter("count > 1")

    # counts.writeStream \
    #     .outputMode("update")\
    #     .option("truncate", "false")\
    #     .option("numRows", 500)\
    #     .format("console")\
    #     .start()\
    #     .awaitTermination(360)

    counts.repartition(1, "window")\
        .writeStream.format("json")\
        .outputMode("append")\
        .option("path", "json-sink")\
        .option("checkPointLocation", "checkpoint")\
        .option("truncate", "false")\
        .start()\
        .awaitTermination(360)
    #counts.writeStream.foreachBatch(foreach_batch_function).trigger("30 seconds").start().awaitTermination(360)
