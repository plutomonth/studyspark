package study.spark

import java.lang.ref.{ReferenceQueue, WeakReference}

import scala.collection.mutable.{ArrayBuffer, SynchronizedBuffer}


/**
 * Classes that represent cleaning tasks.
 */
private sealed trait CleanupTask
private case class CleanAccum(accId: Long) extends CleanupTask

/**
 * A WeakReference associated with a CleanupTask.
 *
 * When the referent object becomes only weakly reachable, the corresponding
 * CleanupTaskWeakReference is automatically added to the given reference queue.
 */
private class CleanupTaskWeakReference(
    val task: CleanupTask,
    referent: AnyRef,
    referenceQueue: ReferenceQueue[AnyRef])
  extends WeakReference(referent, referenceQueue)


/**
 * An asynchronous cleaner for RDD, shuffle, and broadcast state.
 *
 * This maintains a weak reference for each RDD, ShuffleDependency, and Broadcast of interest,
 * to be processed when the associated object goes out of scope of the application. Actual
 * cleanup is performed in a separate daemon thread.
 */
private[spark] class ContextCleaner(sc: SparkContext) extends Logging {

  private val referenceBuffer = new ArrayBuffer[CleanupTaskWeakReference]
    with SynchronizedBuffer[CleanupTaskWeakReference]

  private val referenceQueue = new ReferenceQueue[AnyRef]

  private def registerForCleanup(objectForCleanup: AnyRef, task: CleanupTask): Unit = {
    referenceBuffer += new CleanupTaskWeakReference(task, objectForCleanup, referenceQueue)
  }

  def registerAccumulatorForCleanup(a: Accumulable[_, _]): Unit = {
    registerForCleanup(a, CleanAccum(a.id))
  }
}
