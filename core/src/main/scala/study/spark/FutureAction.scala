package study.spark

import study.spark.scheduler.{JobFailed, JobSucceeded, JobWaiter}

import scala.concurrent.duration.Duration
import scala.concurrent.{CanAwait, ExecutionContext, Future, TimeoutException}
import scala.util.Try

/**
 * A future for the result of an action to support cancellation. This is an extension of the
 * Scala Future interface to support cancellation.
 */
trait FutureAction[T] extends Future[T] {
}

/**
 * A [[FutureAction]] holding the result of an action that triggers a single job. Examples include
 * count, collect, reduce.
 */
class SimpleFutureAction[T] private[spark](jobWaiter: JobWaiter[_], resultFunc: => T)
  extends FutureAction[T] {

  private def awaitResult(): Try[T] = {
    jobWaiter.awaitResult() match {
      case JobSucceeded => scala.util.Success(resultFunc)
      case JobFailed(e: Exception) => scala.util.Failure(e)
    }
  }

  override def isCompleted: Boolean = jobWaiter.jobFinished

  override def value: Option[Try[T]] = {
    if (jobWaiter.jobFinished) {
      Some(awaitResult())
    } else {
      None
    }
  }

  override def onComplete[U](func: (Try[T]) => U)(implicit executor: ExecutionContext) {
    executor.execute(new Runnable {
      override def run() {
        func(awaitResult())
      }
    })
  }

  override def ready(atMost: Duration)(implicit permit: CanAwait): SimpleFutureAction.this.type = {
    if (!atMost.isFinite()) {
      awaitResult()
    } else jobWaiter.synchronized {
      val finishTime = System.currentTimeMillis() + atMost.toMillis
      while (!isCompleted) {
        val time = System.currentTimeMillis()
        if (time >= finishTime) {
          throw new TimeoutException
        } else {
          jobWaiter.wait(finishTime - time)
        }
      }
    }
    this
  }

  @throws(classOf[Exception])
  override def result(atMost: Duration)(implicit permit: CanAwait): T = {
    ready(atMost)(permit)
    awaitResult() match {
      case scala.util.Success(res) => res
      case scala.util.Failure(e) => throw e
    }
  }

}