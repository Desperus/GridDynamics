package net.griddynamics.aggregation

import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * @author Aleksandr_Meterko
  */
object SessionAggregator {

  def enrich(spark: SparkSession, filePath: String): Unit = {
    val events = spark.read
      .format("csv")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .load(filePath)

    val seconds = 5 * 60
    val window = Window.partitionBy("category").orderBy("eventTimeCast").rangeBetween(-seconds, Window.currentRow)

    events
      .withColumn("eventTimeCast", unix_timestamp(col("eventTime")))
      .withColumn("sessionStartTime", first("eventTime").over(window))
      .withColumn("sessionEndTime", last("eventTime").over(window))
      .show(30)

  }

}
