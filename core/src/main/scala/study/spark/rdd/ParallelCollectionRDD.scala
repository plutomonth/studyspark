package study.spark.rdd

import java.io.{IOException, ObjectInputStream, ObjectOutputStream, Serializable}

import com.esotericsoftware.kryo.serializers.JavaSerializer
import study.spark.util.Utils
import study.spark.{Partition, SparkContext}

import scala.collection.Map
import scala.collection.immutable.NumericRange
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


private[spark] class ParallelCollectionPartition[T: ClassTag](
                                                               var rddId: Long,
                                                               var slice: Int,
                                                               var values: Seq[T])
  extends Partition with Serializable {

  def iterator: Iterator[T] = values.iterator

  override def hashCode(): Int = (41 * (41 + rddId) + slice).toInt

  override def equals(other: Any): Boolean = other match {
    case that: ParallelCollectionPartition[_] =>
      this.rddId == that.rddId && this.slice == that.slice
    case _ => false
  }

  override def index: Int = slice


}


private[spark] class ParallelCollectionRDD[T: ClassTag](
     sc: SparkContext,
     @transient private val data: Seq[T],
     numSlices: Int,
     locationPrefs: Map[Int, Seq[String]])
  extends RDD[T](sc, Nil){

  // TODO: Right now, each split sends along its full data, even if later down the RDD chain it gets
  // cached. It might be worthwhile to write the data to a file in the DFS and read it in the split
  // instead.
  // UPDATE: A parallel collection can be checkpointed to HDFS, which achieves this goal.

  override def getPartitions: Array[Partition] = {
    val slices = ParallelCollectionRDD.slice(data, numSlices).toArray
    slices.indices.map(i => new ParallelCollectionPartition(id, i, slices(i))).toArray
  }
}



private object ParallelCollectionRDD {
  /**
   * Slice a collection into numSlices sub-collections. One extra thing we do here is to treat Range
   * collections specially, encoding the slices as other Ranges to minimize memory cost. This makes
   * it efficient to run Spark over RDDs representing large sets of numbers. And if the collection
   * is an inclusive Range, we use inclusive range for the last slice.
   */
  def slice[T: ClassTag](seq: Seq[T], numSlices: Int): Seq[Seq[T]] = {
    if (numSlices < 1) {
      throw new IllegalArgumentException("Positive number of slices required")
    }
    // Sequences need to be sliced at the same set of index positions for operations
    // like RDD.zip() to behave as expected
    def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
      (0 until numSlices).iterator.map(i => {
        val start = ((i * length) / numSlices).toInt
        val end = (((i + 1) * length) / numSlices).toInt
        (start, end)
      })
    }
    seq match {
      case r: Range => {
        positions(r.length, numSlices).zipWithIndex.map({ case ((start, end), index) =>
          // If the range is inclusive, use inclusive range for the last slice
          if (r.isInclusive && index == numSlices - 1) {
            new Range.Inclusive(r.start + start * r.step, r.end, r.step)
          }
          else {
            new Range(r.start + start * r.step, r.start + end * r.step, r.step)
          }
        }).toSeq.asInstanceOf[Seq[Seq[T]]]
      }
      case nr: NumericRange[_] => {
        // For ranges of Long, Double, BigInteger, etc
        val slices = new ArrayBuffer[Seq[T]](numSlices)
        var r = nr
        for ((start, end) <- positions(nr.length, numSlices)) {
          val sliceSize = end - start
          slices += r.take(sliceSize).asInstanceOf[Seq[T]]
          r = r.drop(sliceSize)
        }
        slices
      }
      case _ => {
        val array = seq.toArray // To prevent O(n^2) operations for List etc
        positions(array.length, numSlices).map({
          case (start, end) =>
            array.slice(start, end).toSeq
        }).toSeq
      }
    }
  }
}

