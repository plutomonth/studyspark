package study.spark.scheduler

/**
 * An object that waits for a DAGScheduler job to complete. As tasks finish, it passes their
 * results to the given handler function.
 */
private[spark] class JobWaiter[T](
   dagScheduler: DAGScheduler,
   val jobId: Int,
   totalTasks: Int,
   resultHandler: (Int, T) => Unit)
  extends JobListener {

}
