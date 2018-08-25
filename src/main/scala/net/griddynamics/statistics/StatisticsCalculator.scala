package net.griddynamics.statistics

import org.apache.spark.sql.DataFrame

/**
  * Used to calculate DataFrames with various statistics.
  *
  * @author Aleksandr_Meterko
  */
object StatisticsCalculator {

  private val FirstGroupBound = 1 * 60
  private val SecondGroupBound = 5 * 60
  private val MaxRank = 10

  def median(sessions: DataFrame): DataFrame = {
    sessions.createOrReplaceTempView("sessions")
    sessions.sqlContext
      .sql(
        """SELECT category,
           percentile_approx(sessionDuration, 0.5) as medianDuration
           FROM
             (SELECT DISTINCT category, sessionId,
              UNIX_TIMESTAMP(sessionEndTime, 'yyyy-MM-dd HH:mm:ss') -
                UNIX_TIMESTAMP(sessionStartTime, 'yyyy-MM-dd HH:mm:ss') as sessionDuration
             FROM sessions)
           GROUP BY category
        """.stripMargin)
  }

  def timeGroups(sessions: DataFrame): DataFrame = {
    sessions.createOrReplaceTempView("sessions")
    sessions.sqlContext.sql(
      s"""SELECT category, count(*) as users, timeGroup
          FROM
            (SELECT category, userId,
            CASE WHEN sessionDuration < $FirstGroupBound
              THEN 1
            WHEN sessionDuration < $SecondGroupBound
              THEN 2
            ELSE 3
            END as timeGroup
           FROM
             (SELECT category, userId,
              UNIX_TIMESTAMP(MAX(eventTime), 'yyyy-MM-dd HH:mm:ss') -
               UNIX_TIMESTAMP(MIN(eventTime), 'yyyy-MM-dd HH:mm:ss') as sessionDuration
             FROM sessions
             GROUP BY category, userId, sessionId))
          GROUP BY category, timeGroup
          ORDER BY category, timeGroup
      """.stripMargin
    )
  }

  def rank(sessions: DataFrame): DataFrame = {
    sessions.createOrReplaceTempView("sessions")
    sessions.sqlContext.sql(
      s"""SELECT category, product
          FROM
            (SELECT category, product,
              dense_rank() OVER (PARTITION BY category ORDER BY sessionDuration) as rank
            FROM
             (SELECT category, product, userId, sessionId, sessionDuration
             FROM
               (SELECT category, product, userId, sessionId,
                MAX(eventTimeCast) - MIN(eventTimeCast) as sessionDuration,
                ROW_NUMBER() OVER (PARTITION BY category, product ORDER BY sessionId) as curRow
               FROM
                 (SELECT *,
                    SUM(newSession) OVER (ORDER BY userId, eventTimeCast
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as sessionId
                 FROM
                    (SELECT *,
                     CASE WHEN LAG(product, 1) OVER (PARTITION BY userId ORDER BY eventTimeCast) IS NULL
                      THEN 1
                     WHEN LAG(product, 1) OVER (PARTITION BY userId ORDER BY eventTimeCast) <> product
                      THEN 1
                     ELSE 0
                     END as newSession
                    FROM (SELECT *, UNIX_TIMESTAMP(eventTime, 'yyyy-MM-dd HH:mm:ss') as eventTimeCast from events)
                    )
                 )
               GROUP BY category, product, userId, sessionId
             ) uniqueProducts
             WHERE uniqueProducts.curRow = 1))
          WHERE rank < $MaxRank
      """.stripMargin)
  }

}
