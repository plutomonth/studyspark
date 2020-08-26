package study.spark.scheduler


/**
 * :: DeveloperApi ::
 * A result of a job in the DAGScheduler.
 */
sealed trait JobResult

case object JobSucceeded extends JobResult

private[spark] case class JobFailed(exception: Exception) extends JobResult
