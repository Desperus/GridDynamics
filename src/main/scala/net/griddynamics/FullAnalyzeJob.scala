package net.griddynamics

import net.griddynamics.aggregation.PureSqlSessionEnricher
import org.apache.spark.sql.SparkSession

/**
  * Main entry class intended to be run on cluster.
  *
  * @author Aleksandr_Meterko
  */
class FullAnalyzeJob {

  def main(args: Array[String]): Unit = {
    // just a code sample as main method not used
    if (args.length != 2) {
      print("Please provide 2 parameters: input and output paths")
      return
    }
    val sparkSession = SparkSession.builder().getOrCreate()
    PureSqlSessionEnricher.enrich(sparkSession, args(0))
      .write
      .csv(args(1))
  }

}
