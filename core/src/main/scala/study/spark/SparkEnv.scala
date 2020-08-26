package study.spark

import study.spark.serializer.Serializer

/**
 * :: DeveloperApi ::
 * Holds all the runtime environment objects for a running Spark instance (either master or worker),
 * including the serializer, Akka actor system, block manager, map output tracker, etc. Currently
 * Spark code finds the SparkEnv through a global variable, so all the threads can access the same
 * SparkEnv. It can be accessed by SparkEnv.get (e.g. after creating a SparkContext).
 *
 * NOTE: This is not intended for external use. This is exposed for Shark and may be made private
 *       in a future release.
 */

class SparkEnv (
   val executorId: String,
/*
   private[spark] val rpcEnv: RpcEnv,
   _actorSystem: ActorSystem, // TODO Remove actorSystem
*/
   val serializer: Serializer,
   val closureSerializer: Serializer,
//   val cacheManager: CacheManager,
   val mapOutputTracker: MapOutputTracker,
/*
   val shuffleManager: ShuffleManager,
   val broadcastManager: BroadcastManager,
   val blockTransferService: BlockTransferService,
   val blockManager: BlockManager,
*/
   val securityManager: SecurityManager,
   val sparkFilesDir: String,
/*
   val metricsSystem: MetricsSystem,
   val memoryManager: MemoryManager,
   val outputCommitCoordinator: OutputCommitCoordinator,
*/
   val conf: SparkConf) extends Logging {

}
