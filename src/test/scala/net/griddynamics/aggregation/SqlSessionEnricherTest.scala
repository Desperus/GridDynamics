package net.griddynamics.aggregation

import net.griddynamics.BaseTest

/**
  * @author Aleksandr_Meterko
  */
class SqlSessionEnricherTest extends BaseTest {

  test("Something is run") {
    SqlSessionEnricher.enrich(sparkSession(), InputPath)
  }

  test("Something else is run") {
    PureSqlSessionEnricher.enrich(sparkSession(), InputPath)
  }

}
