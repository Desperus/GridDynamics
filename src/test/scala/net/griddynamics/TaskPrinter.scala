package net.griddynamics

import net.griddynamics.aggregation.PureSqlSessionEnricher
import net.griddynamics.statistics.StatisticsCalculator

/**
  * Used to dump results into both console and files to be analyzed manually.
  *
  * @author Aleksandr_Meterko
  */
class TaskPrinter extends BaseTest {

  private val RowsInBaseCsv = 27

  test("Print results and check that jobs did not fail") {
    val sessions = PureSqlSessionEnricher.enrich(defaultDataFrame())

    sessions.show(RowsInBaseCsv, truncate = false)
    StatisticsCalculator.median(sessions).show()
    StatisticsCalculator.timeGroups(sessions).show(RowsInBaseCsv, truncate = false)
    StatisticsCalculator.rank(sessions).show(RowsInBaseCsv, truncate = false)
  }

}
