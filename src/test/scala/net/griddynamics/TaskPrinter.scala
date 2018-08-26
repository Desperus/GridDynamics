package net.griddynamics

import java.nio.file.Paths

import net.griddynamics.aggregation.PureSqlSessionEnricher
import net.griddynamics.statistics.StatisticsCalculator

/**
  * Used to dump results into both console and files to be analyzed manually. Tests provided without asserts here
  * as their main purpose is to demonstrate user that everything is calculated and that no exception are raised.
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

  test("Save results to csv by main job provides results") {
    val output = Paths.get("results").toAbsolutePath.toString
    FullAnalyzeJob.main(Array(InputPath, output))
  }

}
