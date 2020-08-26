package study.spark

/**
 * Class that keeps track of the location of the map output of
 * a stage. This is abstract because different versions of MapOutputTracker
 * (driver and executor) use different HashMap to store its metadata.
 */
private[spark] abstract class MapOutputTracker(conf: SparkConf) extends Logging {
}

/**
 * MapOutputTracker for the driver. This uses TimeStampedHashMap to keep track of map
 * output information, which allows old output information based on a TTL.
 */
private[spark] class MapOutputTrackerMaster(conf: SparkConf)
  extends MapOutputTracker(conf) {

  }