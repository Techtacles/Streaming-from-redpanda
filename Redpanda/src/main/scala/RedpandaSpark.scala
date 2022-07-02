object RedpandaSpark {
  import org.apache.spark.sql.SparkSession

  def main(args:Array[String]):Unit= {
    val spark = SparkSession
      .builder
      .appName("Redpanda Stream")
      .config("spark.master", "local")
      .getOrCreate()

    val redpanda_read = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "sensor_stream")
      .option("startingOffsets", "earliest") // From starting
      .load()
    val redpanda_write = redpanda_read.selectExpr("CAST(key AS String)","CAST(value AS String)",
    "CAST(topic AS String)","CAST(partition AS String)","CAST(offset as String)")
      .writeStream.format("csv")
      .option("path", "C:/Users/USER/Desktop/Redpanda article/output").outputMode("append")
      .option("checkpointLocation", "C:/Users/USER/Desktop/Redpanda article/checkpoint").start().awaitTermination()

  }
}
