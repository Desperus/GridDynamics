package net.griddynamics.aggregation

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Base trait for every events processor which provides sessionization.
  *
  * @author Aleksandr_Meterko
  */
trait SessionEnricher {

  val DefaultTimeoutSeconds: Int = 5 * 60

  def enrich(events: DataFrame, timeFrame: Int = DefaultTimeoutSeconds): DataFrame

}
