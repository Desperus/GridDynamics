package net.griddynamics.aggregation

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author Aleksandr_Meterko
  */
object PureSqlSessionEnricher extends SessionEnricher {

  private val DefaultTimeoutSeconds = 5 * 60

  override def enrich(events: DataFrame): DataFrame = {
    events.createOrReplaceTempView("events")
    events.sqlContext.sql(
      s"""SELECT category, product, userId, eventTime, eventType, sessionId,
            MIN(eventTime) OVER (PARTITION BY sessionId) as sessionStartTime,
            MAX(eventTime) OVER (PARTITION BY sessionId) as sessionEndTime
          FROM
            (SELECT *,
              SUM(newSession) OVER (ORDER BY category, eventTimeCast
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as sessionId
            FROM
              (SELECT *,
                CASE WHEN timeDiff IS NULL OR timeDiff > $DefaultTimeoutSeconds
                THEN 1
                ELSE 0
                END
                as newSession
              FROM
                (SELECT *, eventTimeCast - LAG(eventTimeCast, 1)
                  OVER (PARTITION BY category ORDER BY eventTimeCast ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)
                  as timeDiff
                FROM (SELECT *, UNIX_TIMESTAMP(eventTime, 'yyyy-MM-dd HH:mm:ss') as eventTimeCast from events)
                )
              )
          )
        """.stripMargin)
  }

}
