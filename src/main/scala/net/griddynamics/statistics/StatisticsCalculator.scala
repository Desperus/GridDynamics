package net.griddynamics.statistics

import org.apache.spark.sql.DataFrame

/**
  * Used to calculate DataFrames with various statistics.
  *
  * @author Aleksandr_Meterko
  */
object StatisticsCalculator {

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

}
