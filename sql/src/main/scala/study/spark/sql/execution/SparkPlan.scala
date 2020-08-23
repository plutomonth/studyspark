package study.spark.sql.execution

import java.util.concurrent.atomic.AtomicBoolean

import study.spark.Logging
import study.spark.rdd.{RDD, RDDOperationScope}
import study.spark.sql.SQLContext
import study.spark.sql.catalyst.InternalRow
import study.spark.sql.catalyst.plans.QueryPlan
import study.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}

/**
 * The base class for physical operators.
 */
abstract class SparkPlan extends QueryPlan[SparkPlan] with Logging with Serializable {

  /**
   * A handle to the SQL Context that was used to create this plan.   Since many operators need
   * access to the sqlContext for RDD operations or configuration this field is automatically
   * populated by the query planning infrastructure.
   */
  @transient
  protected[spark] final val sqlContext = SQLContext.getActive().getOrElse(null)

  protected def sparkContext = sqlContext.sparkContext

  // TODO: Move to `DistributedPlan`
  /** Specifies how data is partitioned across different nodes in the cluster. */
  def outputPartitioning: Partitioning = UnknownPartitioning(0) // TODO: WRONG WIDTH!

  /** Specifies whether this operator outputs UnsafeRows */
  def outputsUnsafeRows: Boolean = false

  /** Specifies whether this operator is capable of processing UnsafeRows */
  def canProcessUnsafeRows: Boolean = false

  /**
   * Specifies whether this operator is capable of processing Java-object-based Rows (i.e. rows
   * that are not UnsafeRows).
   */
  def canProcessSafeRows: Boolean = true


  /**
   * Whether the "prepare" method is called.
   */
  private val prepareCalled = new AtomicBoolean(false)

  /**
   * Returns the result of this query as an RDD[InternalRow] by delegating to doExecute
   * after adding query plan information to created RDDs for visualization.
   * Concrete implementations of SparkPlan should override doExecute instead.
   */
  final def execute(): RDD[InternalRow] = {
    if (children.nonEmpty) {
      val hasUnsafeInputs = children.exists(_.outputsUnsafeRows)
      val hasSafeInputs = children.exists(!_.outputsUnsafeRows)
      assert(!(hasSafeInputs && hasUnsafeInputs),
        "Child operators should output rows in the same format")
      assert(canProcessSafeRows || canProcessUnsafeRows,
        "Operator must be able to process at least one row format")
      assert(!hasSafeInputs || canProcessSafeRows,
        "Operator will receive safe rows as input but cannot process safe rows")
      assert(!hasUnsafeInputs || canProcessUnsafeRows,
        "Operator will receive unsafe rows as input but cannot process unsafe rows")
    }
    RDDOperationScope.withScope(sparkContext, nodeName, false, true) {
      prepare()
      doExecute()
    }
  }

  /**
   * Prepare a SparkPlan for execution. It's idempotent.
   */
  final def prepare(): Unit = {
    if (prepareCalled.compareAndSet(false, true)) {
      doPrepare()
      children.foreach(_.prepare())
    }
  }

  /**
   * Overridden by concrete implementations of SparkPlan. It is guaranteed to run before any
   * `execute` of SparkPlan. This is helpful if we want to set up some state before executing the
   * query, e.g., `BroadcastHashJoin` uses it to broadcast asynchronously.
   *
   * Note: the prepare method has already walked down the tree, so the implementation doesn't need
   * to call children's prepare methods.
   */
  protected def doPrepare(): Unit = {}

  /**
   * Overridden by concrete implementations of SparkPlan.
   * Produces the result of the query as an RDD[InternalRow]
   */
  protected def doExecute(): RDD[InternalRow]

}

private[sql] trait UnaryNode extends SparkPlan {
  def child: SparkPlan

  override def children: Seq[SparkPlan] = child :: Nil

  override def outputPartitioning: Partitioning = child.outputPartitioning
}