package study.spark.sql

import study.spark.sql.catalyst.analysis.{Analyzer, Catalog, FunctionRegistry, SimpleCatalog}
import study.spark.sql.catalyst.optimizer.{DefaultOptimizer, Optimizer}
import study.spark.sql.catalyst.plans.logical.LogicalPlan
import study.spark.sql.catalyst.rules.RuleExecutor
import study.spark.sql.execution._
import study.spark.sql.execution.datasources.{PreInsertCastAndRename, ResolveDataSource}
import study.spark.sql.execution.ui.SQLListener
import study.spark.{Logging, SparkContext}

class SQLContext private[sql](
    @transient val sparkContext: SparkContext,
    @transient protected[sql] val cacheManager: CacheManager,
    @transient private[sql] val listener: SQLListener,
    val isRootContext: Boolean)
  extends Logging with Serializable {
  self =>

  /**
    * @return Spark SQL configuration
    */
  protected[sql] lazy val conf = new SQLConf

  protected[sql] def executePlan(plan: LogicalPlan) =
    new QueryExecution(this, plan)

  @transient
  protected[sql] lazy val catalog: Catalog = new SimpleCatalog(conf)

  @transient
  protected[sql] lazy val functionRegistry: FunctionRegistry = FunctionRegistry.builtin.copy()

  @transient
  protected[sql] lazy val analyzer: Analyzer =
    new Analyzer(catalog, functionRegistry, conf) {
      override val extendedResolutionRules =
          PreInsertCastAndRename ::
          (if (conf.runSQLOnFile) new ResolveDataSource(self) :: Nil else Nil)

      override val extendedCheckRules = Seq(
        datasources.PreWriteCheck(catalog)
      )
    }

  /**
   * Prepares a planned SparkPlan for execution by inserting shuffle operations and internal
   * row format conversions as needed.
   */
  @transient
  protected[sql] val prepareForExecution = new RuleExecutor[SparkPlan] {
    val batches = Seq(
      Batch("Add exchange", Once, EnsureRequirements(self)),
      Batch("Add row converters", Once, EnsureRowFormats)
    )
  }

  @transient
  protected[sql] val planner: SparkPlanner = new SparkPlanner(this)

  @transient
  val experimental: ExperimentalMethods = new ExperimentalMethods(this)

  @transient
  protected[sql] lazy val optimizer: Optimizer = DefaultOptimizer(conf)
}

/**
 * This SQLContext object contains utility functions to create a singleton SQLContext instance,
 * or to get the created SQLContext instance.
 *
 * It also provides utility functions to support preference for threads in multiple sessions
 * scenario, setActive could set a SQLContext for current thread, which will be returned by
 * getOrCreate instead of the global one.
 */
object SQLContext {
  /**
   * The active SQLContext for the current thread.
   */
  private val activeContext: InheritableThreadLocal[SQLContext] =
    new InheritableThreadLocal[SQLContext]

  /**
   * Changes the SQLContext that will be returned in this thread and its children when
   * SQLContext.getOrCreate() is called. This can be used to ensure that a given thread receives
   * a SQLContext with an isolated session, instead of the global (first created) context.
   *
   * @since 1.6.0
   */
  def setActive(sqlContext: SQLContext): Unit = {
    activeContext.set(sqlContext)
  }

  private[sql] def getActive(): Option[SQLContext] = {
    Option(activeContext.get())
  }

}