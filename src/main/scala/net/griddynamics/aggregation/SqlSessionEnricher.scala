package net.griddynamics.aggregation

import java.util.UUID

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * @author Aleksandr_Meterko
  */
object SqlSessionEnricher extends SessionEnricher {

  private val DefaultTimeoutSeconds = 5 * 60

  override def enrich(events: DataFrame): DataFrame = {
    val rowWindow = Window.partitionBy("category")
      .orderBy("eventTimeCast")
      .rowsBetween(-1, -1)
    val sessionIdWindow = Window.orderBy("category", "eventTimeCast")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val sessionTimeWindow = Window.partitionBy("sessionId")

    events
      .withColumn("eventTimeCast", unix_timestamp(col("eventTime")))
      .withColumn("prevTimestamp", lag("eventTimeCast", 1).over(rowWindow))
      .withColumn("isNewSession", when(col("prevTimestamp").isNull
        .or(col("eventTimeCast") - col("prevTimestamp") > DefaultTimeoutSeconds), 1).otherwise(0))
      .withColumn("sessionId", sum("isNewSession").over(sessionIdWindow))
      .withColumn("sessionStartTime", first("eventTime").over(sessionTimeWindow))
      .withColumn("sessionEndTime", last("eventTime").over(sessionTimeWindow))
      .select(col("category"), col("product"), col("userId"), col("eventTime"), col("eventType"), col("sessionId"),
        col("sessionStartTime"), col("sessionEndTime"))
  }

}
