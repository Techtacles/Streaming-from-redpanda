
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.from_json

import scala.List

object RedpandaSpark {
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.functions._





  def main(args:Array[String]):Unit= {
    val spark = SparkSession
      .builder
      .appName("Redpanda Stream")
      .config("spark.master", "local")
      .getOrCreate()
    import spark.implicits._


    val redpanda_read = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "sensor_stream")
      .load()


    val sensor_data=redpanda_read.selectExpr( "CAST(value AS STRING)")
    val stream_1=sensor_data.withColumn("value", split(col("value"), ",")).select(col("value")(0).as("id"),col("value")(3).as("temperature")
    )
    val stream_2=sensor_data.withColumn("value",split(col("value"),",")).select(col("value")(0).as("id"),col("value")(3).as("temperature"))
    val joined_stream=stream_1.union(stream_2)
    /*
        val neww=agg.withColumn("id",split(col("id"),",")).select(col("id").getItem(0))
    */
    /*val redpanda_write = sensor_data
      .writeStream.format("csv")
      .option("path", "C:/Users/USER/Desktop/Redpanda article/Streaming-from-redpanda/output").outputMode("append")
      .option("checkpointLocation", "C:/Users/USER/Desktop/Redpanda article/Streaming-from-redpanda/checkpoint")
      .start().awaitTermination()*/
    val to_write=joined_stream.writeStream.format("csv")
      .option("path", "C:/Users/USER/Desktop/Redpanda article/Streaming-from-redpanda/output").outputMode("append")
      .option("checkpointLocation", "C:/Users/USER/Desktop/Redpanda article/Streaming-from-redpanda/checkpoint").start()
      .awaitTermination()

  }
}
