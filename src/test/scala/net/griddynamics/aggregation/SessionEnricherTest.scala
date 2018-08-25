package net.griddynamics.aggregation

import net.griddynamics.BaseTest

/**
  * @author Aleksandr_Meterko
  */
class SessionEnricherTest extends BaseTest {

  test("Different enrichers should yield same result") {
    val frame = defaultDataFrame()
    val left = SqlSessionEnricher.enrich(frame)
    val right = PureSqlSessionEnricher.enrich(frame)

    assert(left.except(right).count() == 0)
  }

  test("Something is run") {
    SqlSessionEnricher.enrich(defaultDataFrame()).show()
  }

  test("Something else is run") {
    PureSqlSessionEnricher.enrich(defaultDataFrame()).show()
  }

}
