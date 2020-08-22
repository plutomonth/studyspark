package study.spark

import study.spark.sql.execution.SparkPlan

package object sql {
  /**
   * Converts a logical plan into zero or more SparkPlans.  This API is exposed for experimenting
   * with the query planner and is not designed to be stable across spark releases.  Developers
   * writing libraries should instead consider using the stable APIs provided in
   * [[study.spark.sql.sources]]
   */
  type Strategy = study.spark.sql.catalyst.planning.GenericStrategy[SparkPlan]

}
