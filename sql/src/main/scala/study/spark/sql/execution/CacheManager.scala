package study.spark.sql.execution

import study.spark.Logging
import study.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Provides support in a SQLContext for caching query results and automatically using these cached
 * results when subsequent queries are executed.  Data is cached using byte buffers stored in an
 * InMemoryRelation.  This relation is automatically substituted query plans that return the
 * `sameResult` as the originally cached query.
 *
 * Internal to Spark SQL.
 */
private[sql] class CacheManager extends Logging {


  /** Replaces segments of the given logical plan with cached versions where possible. */
  private[sql] def useCachedData(plan: LogicalPlan): LogicalPlan = {
    plan transformDown {
      case currentFragment =>
        lookupCachedData(currentFragment)
          .map(_.cachedRepresentation.withOutput(currentFragment.output))
          .getOrElse(currentFragment)
    }
  }
}
