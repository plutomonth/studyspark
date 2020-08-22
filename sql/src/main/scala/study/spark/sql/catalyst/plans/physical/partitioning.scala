package study.spark.sql.catalyst.plans.physical

/**
 * Describes how an operator's output is split across partitions. The `compatibleWith`,
 * `guarantees`, and `satisfies` methods describe relationships between child partitionings,
 * target partitionings, and [[Distribution]]s. These relations are described more precisely in
 * their individual method docs, but at a high level:
 *
 *  - `satisfies` is a relationship between partitionings and distributions.
 *  - `compatibleWith` is relationships between an operator's child output partitionings.
 *  - `guarantees` is a relationship between a child's existing output partitioning and a target
 *     output partitioning.
 *
 *  Diagrammatically:
 *
 *            +--------------+
 *            | Distribution |
 *            +--------------+
 *                    ^
 *                    |
 *               satisfies
 *                    |
 *            +--------------+                  +--------------+
 *            |    Child     |                  |    Target    |
 *       +----| Partitioning |----guarantees--->| Partitioning |
 *       |    +--------------+                  +--------------+
 *       |            ^
 *       |            |
 *       |     compatibleWith
 *       |            |
 *       +------------+
 *
 */
sealed trait Partitioning {
}

case class UnknownPartitioning(numPartitions: Int) extends Partitioning {

}