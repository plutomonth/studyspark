package study.spark.scheduler

import java.util.Properties

import study.spark.ShuffleDependency
import study.spark.util.CallSite


/**
 * Types of events that can be handled by the DAGScheduler. The DAGScheduler uses an event queue
 * architecture where any thread can post an event (e.g. a task finishing or a new job being
 * submitted) but there is a single "logic" thread that reads these events and takes decisions.
 * This greatly simplifies synchronization.
 */
private[scheduler] sealed trait DAGSchedulerEvent


/** A map stage as submitted to run as a separate job */
private[scheduler] case class MapStageSubmitted(
   jobId: Int,
   dependency: ShuffleDependency[_, _, _],
   callSite: CallSite,
   listener: JobListener,
   properties: Properties = null)
  extends DAGSchedulerEvent
