package net.griddynamics.aggregation

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Base trait for every events processor which provides sessionization.
  *
  * @author Aleksandr_Meterko
  */
trait SessionEnricher {

  def enrich(events: DataFrame): DataFrame

}
