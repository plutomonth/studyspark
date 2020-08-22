package study.spark.sql

import study.spark.sql.catalyst.analysis.{Analyzer, Catalog, FunctionRegistry, SimpleCatalog}
import study.spark.sql.catalyst.plans.logical.LogicalPlan
import study.spark.sql.execution._
import study.spark.sql.execution.datasources.{PreInsertCastAndRename, ResolveDataSource}
import study.spark.{Logging, SparkContext}

class SQLContext private[sql](
                               @transient val sparkContext: SparkContext)
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


}
