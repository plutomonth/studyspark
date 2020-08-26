package study.spark.scheduler

import study.spark.util.AsynchronousListenerBus

/**
 * Asynchronously passes SparkListenerEvents to registered SparkListeners.
 *
 * Until start() is called, all posted events are only buffered. Only after this listener bus
 * has started will events be actually propagated to all attached listeners. This listener bus
 * is stopped when it receives a SparkListenerShutdown event, which is posted using stop().
 */
private[spark] class LiveListenerBus
  extends AsynchronousListenerBus[SparkListener, SparkListenerEvent]("SparkListenerBus")
    with SparkListenerBus {

}
