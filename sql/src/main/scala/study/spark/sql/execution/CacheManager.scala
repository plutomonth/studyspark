package study.spark.sql.execution

import java.util.concurrent.locks.ReentrantReadWriteLock

import study.spark.Logging
import study.spark.sql.catalyst.plans.logical.LogicalPlan
import study.spark.sql.execution.columnar.InMemoryRelation

/** Holds a cached logical plan and its data */
private[sql] case class CachedData(plan: LogicalPlan, cachedRepresentation: InMemoryRelation)

/**
 * Provides support in a SQLContext for caching query results and automatically using these cached
 * results when subsequent queries are executed.  Data is cached using byte buffers stored in an
 * InMemoryRelation.  This relation is automatically substituted query plans that return the
 * `sameResult` as the originally cached query.
 *
 * Internal to Spark SQL.
 */
private[sql] class CacheManager extends Logging {


  @transient
  private val cachedData = new scala.collection.mutable.ArrayBuffer[CachedData]

  @transient
  private val cacheLock = new ReentrantReadWriteLock

  /** Acquires a read lock on the cache for the duration of `f`. */
  private def readLock[A](f: => A): A = {
    val lock = cacheLock.readLock()
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  /** Optionally returns cached data for the given [[Queryable]] */
  private[sql] def lookupCachedData(query: Queryable): Option[CachedData] = readLock {
    lookupCachedData(query.queryExecution.analyzed)
  }

  /** Optionally returns cached data for the given [[LogicalPlan]]. */
  private[sql] def lookupCachedData(plan: LogicalPlan): Option[CachedData] = readLock {
    cachedData.find(cd => plan.sameResult(cd.plan))
  }

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
