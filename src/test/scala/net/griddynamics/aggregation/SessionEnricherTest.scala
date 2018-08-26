package net.griddynamics.aggregation

import net.griddynamics.BaseTest
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.prop.TableDrivenPropertyChecks._

/**
  * @author Aleksandr_Meterko
  */
class SessionEnricherTest extends BaseTest {

  private val enrichers = Table("Specific enricher", SqlSessionEnricher, PureSqlSessionEnricher)

  test("Different enrichers should yield same result") {
    val frame = defaultDataFrame()
    val left = SqlSessionEnricher.enrich(frame)
    val right = PureSqlSessionEnricher.enrich(frame)

    assert(left.except(right).count() == 0)
  }

  test("Consecutive events from same category are enriched with same session") {
    val startTime = "2018-03-01 12:00:02"
    val endTime = "2018-03-01 12:01:02"
    val events = createDataFrame(Seq(
      (defaultCategory, "prod", "user", startTime, "evt"),
      (defaultCategory, "prod", "user", endTime, "evt")
    ))
    testEnrichers(events, sessions => {
      val result = sessions.select("sessionId", "sessionStartTime", "sessionEndTime").collect()
      assert(result.length == 2)
      result.foreach(row => {
        assert(row.toSeq === Array(1, startTime, endTime))
      })
    })
  }

  test("Consecutive events from different categories are enriched with different sessions") {
    val events = createDataFrame(Seq(
      (defaultCategory, "prod", "user", "2018-03-01 12:00:02", "evt"),
      ("someOtherCategory", "prod", "user", "2018-03-01 12:41:02", "evt")
    ))
    testEnrichers(events, sessions => {
      val result = sessions.select("sessionId", "sessionStartTime", "sessionEndTime").collect()
      assert(result.length == 2)
      val firstSession = result(0).getLong(0)
      val secondSession = result(1).getLong(0)
      assert(firstSession != secondSession)
    })
  }

  test("Far standing events from same category are enriched with different sessions") {
    val events = createDataFrame(Seq(
      (defaultCategory, "prod", "user", "2018-03-01 12:00:02", "evt"),
      (defaultCategory, "prod", "user", "2018-03-01 12:41:02", "evt")
    ))
    testEnrichers(events, sessions => {
      val result = sessions.select("sessionId", "sessionStartTime", "sessionEndTime").collect()
      assert(result.length == 2)
      val firstSession = result(0).getLong(0)
      val secondSession = result(1).getLong(0)
      assert(firstSession != secondSession)
    })
  }

  def testEnrichers(events: DataFrame, assertion: DataFrame => Unit): Unit = {
    forAll(enrichers) { (enricher: SessionEnricher) =>
      val sessions = enricher.enrich(events)
      assertion(sessions)
    }
  }
}
