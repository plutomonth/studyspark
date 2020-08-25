package study.spark.sql.catalyst

private[spark] trait CatalystConf {
  def caseSensitiveAnalysis: Boolean

  protected[spark] def specializeSingleDistinctAggPlanning: Boolean
}

