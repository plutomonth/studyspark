package study.spark.rdd

import study.spark.{Dependency, Logging, OneToOneDependency, Partition, SparkContext, SparkException, TaskContext}

import scala.reflect.ClassTag

/**
 * A Resilient Distributed Dataset (RDD), the basic abstraction in Spark. Represents an immutable,
 * partitioned collection of elements that can be operated on in parallel. This class contains the
 * basic operations available on all RDDs, such as `map`, `filter`, and `persist`. In addition,
 * study.spark.rdd.PairRDDFunctions contains operations available only on RDDs of key-value
 * pairs, such as `groupByKey` and `join`;
 * study.spark.rdd.DoubleRDDFunctions contains operations available only on RDDs of
 * Doubles; and
 * study.spark.rdd.SequenceFileRDDFunctions contains operations available on RDDs that
 * can be saved as SequenceFiles.
 * All operations are automatically available on any RDD of the right type (e.g. RDD[(Int, Int)]
 * through implicit.
 *
 * Internally, each RDD is characterized by five main properties:
 *
 *  - A list of partitions
 *  - A function for computing each split
 *  - A list of dependencies on other RDDs
 *  - Optionally, a Partitioner for key-value RDDs (e.g. to say that the RDD is hash-partitioned)
 *  - Optionally, a list of preferred locations to compute each split on (e.g. block locations for
 *    an HDFS file)
 *
 * All of the scheduling and execution in Spark is done based on these methods, allowing each RDD
 * to implement its own way of computing itself. Indeed, users can implement custom RDDs (e.g. for
 * reading data from a new storage system) by overriding these functions. Please refer to the
 * [[http://www.cs.berkeley.edu/~matei/papers/2012/nsdi_spark.pdf Spark paper]] for more details
 * on RDD internals.
 */
abstract class RDD[T: ClassTag](
     @transient private var _sc: SparkContext,
     @transient private var deps: Seq[Dependency[_]]
   ) extends Serializable with Logging {

   @transient private var partitions_ : Array[Partition] = null

   private[spark] var checkpointData: Option[RDDCheckpointData[T]] = None

   /** An Option holding our checkpoint RDD, if we are checkpointed */
   private def checkpointRDD: Option[CheckpointRDD[T]] = checkpointData.flatMap(_.checkpointRDD)

   private def sc: SparkContext = {
      if (_sc == null) {
         throw new SparkException(
            "RDD transformations and actions can only be invoked by the driver, not inside of other " +
              "transformations; for example, rdd1.map(x => rdd2.values.count() * x) is invalid because " +
              "the values transformation and count action cannot be performed inside of the rdd1.map " +
              "transformation. For more information, see SPARK-5063.")
      }
      _sc
   }

   /** Construct an RDD with just a one-to-one dependency on one parent */
   def this(@transient oneParent: RDD[_]) =
      this(oneParent.context , List(new OneToOneDependency(oneParent)))

   /** The [[study.spark.SparkContext]] that this RDD was created on. */
   def context: SparkContext = sc

   /**
    * Return a new RDD by applying a function to each partition of this RDD.
    *
    * `preservesPartitioning` indicates whether the input function preserves the partitioner, which
    * should be `false` unless this is a pair RDD and the input function doesn't modify the keys.
    */
   def mapPartitions[U: ClassTag](
        f: Iterator[T] => Iterator[U],
        preservesPartitioning: Boolean = false): RDD[U] = withScope {
      val cleanedF = sc.clean(f)
      new MapPartitionsRDD(
         this,
         (context: TaskContext, index: Int, iter: Iterator[T]) => cleanedF(iter),
         preservesPartitioning)
   }

   /**
    * Implemented by subclasses to return the set of partitions in this RDD. This method will only
    * be called once, so it is safe to implement a time-consuming computation in it.
    */
   protected def getPartitions: Array[Partition]

   /**
    * Get the array of partitions of this RDD, taking into account whether the
    * RDD is checkpointed or not.
    */
   final def partitions: Array[Partition] = {
      checkpointRDD.map(_.partitions).getOrElse {
         if (partitions_ == null) {
            partitions_ = getPartitions
         }
         partitions_
      }
   }

   /**
    * Execute a block of code in a scope such that all new RDDs created in this body will
    * be part of the same scope. For more detail, see {{org.apache.spark.rdd.RDDOperationScope}}.
    *
    * Note: Return statements are NOT allowed in the given body.
    */
   private[spark] def withScope[U](body: => U): U = RDDOperationScope.withScope[U](sc)(body)
}
