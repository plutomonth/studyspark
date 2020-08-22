package study.spark

/**
 * Utility trait for classes that want to log data. Creates a SLF4J logger for the class and allows
 * logging messages at different levels using methods that only evaluate parameters lazily if the
 * log level is enabled.
 *
 * NOTE: DO NOT USE this class outside of Spark. It is intended as an internal utility.
 *       This will likely be changed or removed in future releases.
 */
trait Logging {

}

private object Logging {
  @volatile private var initialized = false
  val initLock = new Object()

}
