package study.spark.sql.execution


import java.util.Random

import org.apache.commons.lang3.tuple.MutablePair
import study.spark.{Partitioner, ShuffleDependency}
import study.spark.rdd.RDD
import study.spark.sql.SQLContext
import study.spark.sql.catalyst.InternalRow
import study.spark.sql.catalyst.expressions.Attribute
import study.spark.sql.catalyst.plans.physical.Partitioning
import study.spark.sql.catalyst.rules.Rule
import study.spark.sql.catalyst.errors.attachTree

/**
 * Performs a shuffle that will result in the desired `newPartitioning`.
 */
case class Exchange(
   var newPartitioning: Partitioning,
   child: SparkPlan,
   @transient coordinator: Option[ExchangeCoordinator]) extends UnaryNode {

  override def output: Seq[Attribute] = child.output

  protected override def doExecute(): RDD[InternalRow] = attachTree(this , "execute") {
    coordinator match {
      case Some(exchangeCoordinator) =>
        val shuffleRDD = exchangeCoordinator.postShuffleRDD(this)
        assert(shuffleRDD.partitions.length == newPartitioning.numPartitions)
        shuffleRDD
      case None =>
        val shuffleDependency = prepareShuffleDependency()
        preparePostShuffleRDD(shuffleDependency)
    }
  }


  /**
   * Returns a [[ShuffleDependency]] that will partition rows of its child based on
   * the partitioning scheme defined in `newPartitioning`. Those partitions of
   * the returned ShuffleDependency will be the input of shuffle.
   */
  private[sql] def prepareShuffleDependency(): ShuffleDependency[Int, InternalRow, InternalRow] = {
    val rdd = child.execute()
    val part: Partitioner = newPartitioning match {
      case RoundRobinPartitioning(numPartitions) => new HashPartitioner(numPartitions)
      case HashPartitioning(expressions, numPartitions) => new HashPartitioner(numPartitions)
      case RangePartitioning(sortingExpressions, numPartitions) =>
        // Internally, RangePartitioner runs a job on the RDD that samples keys to compute
        // partition bounds. To get accurate samples, we need to copy the mutable keys.
        val rddForSampling = rdd.mapPartitionsInternal { iter =>
          val mutablePair = new MutablePair[InternalRow, Null]()
          iter.map(row => mutablePair.update(row.copy(), null))
        }
        // We need to use an interpreted ordering here because generated orderings cannot be
        // serialized and this ordering needs to be created on the driver in order to be passed into
        // Spark core code.
        implicit val ordering = new InterpretedOrdering(sortingExpressions, child.output)
        new RangePartitioner(numPartitions, rddForSampling, ascending = true)
      case SinglePartition =>
        new Partitioner {
          override def numPartitions: Int = 1
          override def getPartition(key: Any): Int = 0
        }
      case _ => sys.error(s"Exchange not implemented for $newPartitioning")
      // TODO: Handle BroadcastPartitioning.
    }
    def getPartitionKeyExtractor(): InternalRow => Any = newPartitioning match {
      case RoundRobinPartitioning(numPartitions) =>
        // Distributes elements evenly across output partitions, starting from a random partition.
        var position = new Random(TaskContext.get().partitionId()).nextInt(numPartitions)
        (row: InternalRow) => {
          // The HashPartitioner will handle the `mod` by the number of partitions
          position += 1
          position
        }
      case HashPartitioning(expressions, _) => newMutableProjection(expressions, child.output)()
      case RangePartitioning(_, _) | SinglePartition => identity
      case _ => sys.error(s"Exchange not implemented for $newPartitioning")
    }
    val rddWithPartitionIds: RDD[Product2[Int, InternalRow]] = {
      if (needToCopyObjectsBeforeShuffle(part, serializer)) {
        rdd.mapPartitionsInternal { iter =>
          val getPartitionKey = getPartitionKeyExtractor()
          iter.map { row => (part.getPartition(getPartitionKey(row)), row.copy()) }
        }
      } else {
        rdd.mapPartitionsInternal { iter =>
          val getPartitionKey = getPartitionKeyExtractor()
          val mutablePair = new MutablePair[Int, InternalRow]()
          iter.map { row => mutablePair.update(part.getPartition(getPartitionKey(row)), row) }
        }
      }
    }

    // Now, we manually create a ShuffleDependency. Because pairs in rddWithPartitionIds
    // are in the form of (partitionId, row) and every partitionId is in the expected range
    // [0, part.numPartitions - 1]. The partitioner of this is a PartitionIdPassthrough.
    val dependency =
    new ShuffleDependency[Int, InternalRow, InternalRow](
      rddWithPartitionIds,
      new PartitionIdPassthrough(part.numPartitions),
      Some(serializer))

    dependency
  }

}


/**
 * Ensures that the [[study.spark.sql.catalyst.plans.physical.Partitioning Partitioning]]
 * of input data meets the
 * [[study.spark.sql.catalyst.plans.physical.Distribution Distribution]] requirements for
 * each operator by inserting [[Exchange]] Operators where required.  Also ensure that the
 * input partition ordering requirements are met.
 */
private[sql] case class EnsureRequirements(sqlContext: SQLContext) extends Rule[SparkPlan] {
}