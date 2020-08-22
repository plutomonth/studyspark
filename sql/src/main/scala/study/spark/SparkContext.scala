package study.spark

import java.util.Properties

import org.apache.commons.lang.SerializationUtils

class SparkContext(config: SparkConf) extends Logging {

  // Thread Local variable that can be used by users to pass information down the stack
  protected[spark] val localProperties = new InheritableThreadLocal[Properties] {
    override protected def childValue(parent: Properties): Properties = {
      // Note: make a clone such that changes in the parent properties aren't reflected in
      // the those of the children threads, which has confusing semantics (SPARK-10563).
      SerializationUtils.clone(parent).asInstanceOf[Properties]
    }
    override protected def initialValue(): Properties = new Properties()
  }

  /**
   * Get a local property set in this thread, or null if it is missing. See
   * [[study.spark.SparkContext.setLocalProperty]].
   */
  def getLocalProperty(key: String): String =
    Option(localProperties.get).map(_.getProperty(key)).orNull

  /**
   * Set a local property that affects jobs submitted from this thread, such as the
   * Spark fair scheduler pool.
   */
  def setLocalProperty(key: String, value: String) {
    if (value == null) {
      localProperties.get.remove(key)
    } else {
      localProperties.get.setProperty(key, value)
    }
  }
}

object SparkContext extends Logging {
  private[spark] val RDD_SCOPE_KEY = "spark.rdd.scope"
  private[spark] val RDD_SCOPE_NO_OVERRIDE_KEY = "spark.rdd.scope.noOverride"
}