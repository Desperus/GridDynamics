package net.griddynamics

import org.apache.spark.sql.SparkSession

/**
  * Main entry class intended to be run on cluster.
  *
  * @author Aleksandr_Meterko
  */
class FullAnalyzeJob {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().getOrCreate()
  }

}
