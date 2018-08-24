package net.griddynamics.aggregation

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

/**
  * @author Aleksandr_Meterko
  */
class SessionAggregatorTest extends FunSuite {

  private val InputPath = getClass.getResource("/events/input.csv").getFile

  protected def sparkSession(): SparkSession = {
    val sparkConf = new SparkConf()
      .set("spark.sql.session.timeZone", "UTC")
      .setMaster("local[*]")
    SparkSession.builder().config(sparkConf).getOrCreate()
  }

  test("Something is run") {
    SessionAggregator.enrich(sparkSession(), InputPath)
  }

}
