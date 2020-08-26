package study.spark

/**
 * :: DeveloperApi ::
 * A set of functions used to aggregate data.
 *
 * @param createCombiner function to create the initial value of the aggregation.
 * @param mergeValue function to merge a new value into the aggregation result.
 * @param mergeCombiners function to merge outputs from multiple mergeValue function.
 */
case class Aggregator[K, V, C] (
   createCombiner: V => C,
   mergeValue: (C, V) => C,
   mergeCombiners: (C, C) => C) {

}
