package net.griddynamics

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * @author Aleksandr_Meterko
  */
class BaseTest extends FunSuite with BeforeAndAfterAll {

  protected val InputPath: String = getClass.getResource("/events/input.csv").getFile

  override protected def beforeAll() {
    sparkSession()
  }

  override protected def afterAll() {
    sparkSession().close()
  }

  protected def sparkSession(): SparkSession = {
    val sparkConf = new SparkConf()
      .set("spark.sql.session.timeZone", "UTC")
      .setMaster("local[*]")
    SparkSession.builder().config(sparkConf).getOrCreate()
  }

}
