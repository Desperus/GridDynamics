package net.griddynamics.statistics

import net.griddynamics.BaseTest
import org.apache.spark.sql.DataFrame

/**
  * @author Aleksandr_Meterko
  */
class StatisticsCalculatorTest extends BaseTest {

  test("Median is calculated by rules") {
    val sessions = createSessionDataFrame(Seq(
      (defaultCategory, "prod", "user", "2018-03-01 12:00:00", "evt", 1, "2018-03-01 12:00:00", "2018-03-01 12:00:01"),
      (defaultCategory, "prod", "user", "2018-03-01 12:01:02", "evt", 2, "2018-03-01 12:00:10", "2018-03-01 12:00:11"),
      (defaultCategory, "prod", "user", "2018-03-01 12:01:02", "evt", 3, "2018-03-01 12:00:20", "2018-03-01 12:00:41")
    ))
    val median = StatisticsCalculator.median(sessions).collect()
    assert(median.length == 1)
    assert(median(0).getDouble(1) == 1)
  }

  test("Time groups are calculated by requirements") {
    val sessions = createSessionDataFrame(Seq(
      (defaultCategory, "prod", "user1", "2018-03-01 12:00:00", "in", 1, "2018-03-01 12:00:00", "2018-03-01 12:00:01"),
      (defaultCategory, "prod", "user1", "2018-03-01 12:00:01", "out", 1, "2018-03-01 12:00:00", "2018-03-01 12:00:01"),
      (defaultCategory, "prod", "user2", "2018-03-01 12:00:10", "in", 2, "2018-03-01 12:00:10", "2018-03-01 12:01:11"),
      (defaultCategory, "prod", "user2", "2018-03-01 12:01:11", "out", 2, "2018-03-01 12:00:10", "2018-03-01 12:01:11"),
      (defaultCategory, "prod", "user3", "2018-03-01 12:02:00", "in", 3, "2018-03-01 12:02:00", "2018-03-01 12:08:00"),
      (defaultCategory, "prod", "user3", "2018-03-01 12:08:00", "out", 3, "2018-03-01 12:02:00", "2018-03-01 12:08:00"),
      (defaultCategory, "prod", "user4", "2018-03-01 12:09:00", "in", 4, "2018-03-01 12:09:00", "2018-03-01 12:15:00"),
      (defaultCategory, "prod", "user4", "2018-03-01 12:15:00", "out", 4, "2018-03-01 12:09:00", "2018-03-01 12:15:00")
    ))
    val groups = StatisticsCalculator.timeGroups(sessions).orderBy("timeGroup").collect()
    assert(groups.length == 3)
    assert(groups(0).getLong(1) == 1)
    assert(groups(1).getLong(1) == 1)
    assert(groups(2).getLong(1) == 2)
  }

  test("Only top N ranked items should be returned") {
    val topRankedProd = "3_prod"
    val secondTopRankedProd = "2_prod"
    val sessions = createDataFrame(Seq(
      (defaultCategory, "a", "user", "2018-03-01 12:00:00", "in"),
      (defaultCategory, "a", "user", "2018-03-01 12:00:05", "out"),
      (defaultCategory, secondTopRankedProd, "user", "2018-03-01 12:00:06", "in"),
      (defaultCategory, secondTopRankedProd, "user", "2018-03-01 12:00:26", "out"),
      (defaultCategory, topRankedProd, "user", "2018-03-01 12:00:27", "in"),
      (defaultCategory, topRankedProd, "user", "2018-03-01 12:01:00", "out"),
      (defaultCategory, "someOtherProd", "user", "2018-03-01 12:01:01", "in")
    ))
    val rank = StatisticsCalculator.rank(sessions, 2).collect()
    assert(rank.length == 2)
    assert(rank(0).getString(1) === topRankedProd)
    assert(rank(1).getString(1) === secondTopRankedProd)
  }

  private def createSessionDataFrame(rows: Seq[(String, String, String, String, String, Long, String, String)]): DataFrame = {
    val spark = sparkSession()
    import spark.implicits._
    rows.toDF("category", "product", "userId", "eventTime", "eventType", "sessionId", "sessionStartTime",
      "sessionEndTime")
  }

}
