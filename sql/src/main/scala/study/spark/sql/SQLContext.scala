package study.spark.sql

import study.spark.sql.catalyst.analysis.Analyzer
import study.spark.sql.catalyst.plans.logical.LogicalPlan
import study.spark.sql.execution.QueryExecution
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
  protected[sql] lazy val analyzer: Analyzer =
    new Analyzer(catalog, functionRegistry, conf) {
      override val extendedResolutionRules =
        ExtractPythonUDFs ::
          PreInsertCastAndRename ::
          (if (conf.runSQLOnFile) new ResolveDataSource(self) :: Nil else Nil)

      override val extendedCheckRules = Seq(
        datasources.PreWriteCheck(catalog)
      )
    }


}
