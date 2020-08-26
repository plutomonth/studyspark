package study.spark.scheduler

import study.spark.util.ListenerBus

/**
 * A [[SparkListenerEvent]] bus that relays [[SparkListenerEvent]]s to its listeners
 */
private[spark] trait SparkListenerBus extends ListenerBus[SparkListener, SparkListenerEvent] {

}
