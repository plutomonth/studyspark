package study.spark.rdd

import scala.reflect.ClassTag

/**
 * This class contains all the information related to RDD checkpointing. Each instance of this
 * class is associated with a RDD. It manages process of checkpointing of the associated RDD,
 * as well as, manages the post-checkpoint state by providing the updated partitions,
 * iterator and preferred locations of the checkpointed RDD.
 */
private[spark] abstract class RDDCheckpointData[T: ClassTag](@transient private val rdd: RDD[T])
  extends Serializable {

  // The RDD that contains our checkpointed data
  private var cpRDD: Option[CheckpointRDD[T]] = None

  /**
   * Return the RDD that contains our checkpointed data.
   * This is only defined if the checkpoint state is `Checkpointed`.
   */
  def checkpointRDD: Option[CheckpointRDD[T]] = RDDCheckpointData.synchronized { cpRDD }
}


/**
 * Global lock for synchronizing checkpoint operations.
 */
private[spark] object RDDCheckpointData
