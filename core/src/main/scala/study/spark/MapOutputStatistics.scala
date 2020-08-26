package study.spark


/**
 * Holds statistics about the output sizes in a map stage. May become a DeveloperApi in the future.
 *
 * @param shuffleId ID of the shuffle
 * @param bytesByPartitionId approximate number of output bytes for each map output partition
 *   (may be inexact due to use of compressed map statuses)
 */
private[spark] class MapOutputStatistics(val shuffleId: Int, val bytesByPartitionId: Array[Long])

