package net.griddynamics.aggregation

import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * @author Aleksandr_Meterko
  */
object SqlSessionEnricher {

  private val DefaultTimeoutSeconds = 5 * 60

  def enrich(spark: SparkSession, filePath: String): Unit = {
    val events = spark.read
      .format("csv")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .load(filePath)

    val rowWindow = Window.partitionBy("category")
      .orderBy("eventTimeCast")
    val timeWindow = rowWindow.rangeBetween(-DefaultTimeoutSeconds, Window.currentRow)
    val sessionIdWindow = Window.orderBy("category", "eventTimeCast")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val result = events
      .withColumn("eventTimeCast", unix_timestamp(col("eventTime")))
      .withColumn("prevTimestamp", lag("eventTimeCast", 1).over(rowWindow))
      .withColumn("isNewSession", when(lag("eventTime", 1).over(rowWindow).isNull, 1).otherwise(0))
      .withColumn("sessionId", sum("isNewSession").over(sessionIdWindow))
      .withColumn("sessionStartTime", first("eventTime").over(timeWindow))
      .withColumn("sessionEndTime", last("eventTime").over(timeWindow))
      .select(col("category"), col("eventTime"), col("sessionId"))

    result.show(30)
    println(result.count())
  }

}
