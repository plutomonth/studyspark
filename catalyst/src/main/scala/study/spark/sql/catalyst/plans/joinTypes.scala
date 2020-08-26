package study.spark.sql.catalyst.plans

object JoinType {
  def apply(typ: String): JoinType = typ.toLowerCase.replace("_", "") match {
    case "inner" => Inner
    case "outer" | "full" | "fullouter" => FullOuter
    case "leftouter" | "left" => LeftOuter
    case "rightouter" | "right" => RightOuter
    case "leftsemi" => LeftSemi
    case _ =>
      val supported = Seq(
        "inner",
        "outer", "full", "fullouter",
        "leftouter", "left",
        "rightouter", "right",
        "leftsemi")

      throw new IllegalArgumentException(s"Unsupported join type '$typ'. " +
        "Supported join types include: " + supported.mkString("'", "', '", "'") + ".")
  }
}

sealed abstract class JoinType

case object Inner extends JoinType

case object LeftOuter extends JoinType

case object RightOuter extends JoinType

case object FullOuter extends JoinType

case object LeftSemi extends JoinType