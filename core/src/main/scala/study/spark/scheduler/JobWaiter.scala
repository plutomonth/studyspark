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

  private var finishedTasks = 0

  // Is the job as a whole finished (succeeded or failed)?
  @volatile
  private var _jobFinished = totalTasks == 0

  def jobFinished: Boolean = _jobFinished

  // If the job is finished, this will be its result. In the case of 0 task jobs (e.g. zero
  // partition RDDs), we set the jobResult directly to JobSucceeded.
  private var jobResult: JobResult = if (jobFinished) JobSucceeded else null

  override def taskSucceeded(index: Int, result: Any): Unit = synchronized {
    if (_jobFinished) {
      throw new UnsupportedOperationException("taskSucceeded() called on a finished JobWaiter")
    }
    resultHandler(index, result.asInstanceOf[T])
    finishedTasks += 1
    if (finishedTasks == totalTasks) {
      _jobFinished = true
      jobResult = JobSucceeded
      this.notifyAll()
    }
  }

  override def jobFailed(exception: Exception): Unit = synchronized {
    _jobFinished = true
    jobResult = JobFailed(exception)
    this.notifyAll()
  }

  def awaitResult(): JobResult = synchronized {
    while (!_jobFinished) {
      this.wait()
    }
    return jobResult
  }


}
