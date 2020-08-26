package study.spark.storage

import study.spark.rpc.RpcEndpointRef
import study.spark.{Logging, SparkConf}

private[spark]
class BlockManagerMaster(
    var driverEndpoint: RpcEndpointRef,
    conf: SparkConf,
    isDriver: Boolean)
  extends Logging {

}
