package net.griddynamics

import java.nio.file.Paths

import net.griddynamics.aggregation.{PureSqlSessionEnricher, SqlSessionEnricher}
import net.griddynamics.statistics.StatisticsCalculator
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Main entry class intended to be run on cluster.
  *
  * @author Aleksandr_Meterko
  */
object FullAnalyzeJob {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      print("Please provide 2 parameters: input and output paths")
      return
    }
    val sparkSession = SparkSession.builder().getOrCreate()

    def saveDF(dataFrame: DataFrame, fileName: String): Unit = {
      dataFrame.write
        .csv(Paths.get(args(1)).resolve(fileName).toAbsolutePath.toString)
    }

    val events = loadFromCsv(sparkSession, args(0))
    val sessions = PureSqlSessionEnricher.enrich(events)
    saveDF(sessions, "pureSqlEnricher")
    saveDF(SqlSessionEnricher.enrich(events), "apiEnricher")
    saveDF(StatisticsCalculator.median(sessions), "median")
    saveDF(StatisticsCalculator.rank(events), "rank")
    saveDF(StatisticsCalculator.timeGroups(sessions), "timeGroups")
  }

  def loadFromCsv(spark: SparkSession, filePath: String): DataFrame = {
    spark.read
      .format("csv")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .option("quote", "\"")
      .option("escape", "\"")
      .load(filePath)
  }

}
