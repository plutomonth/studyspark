package study.spark.sql.types


/**
 * :: DeveloperApi ::
 *
 * Metadata is a wrapper over Map[String, Any] that limits the value type to simple ones: Boolean,
 * Long, Double, String, Metadata, Array[Boolean], Array[Long], Array[Double], Array[String], and
 * Array[Metadata]. JSON is used for serialization.
 *
 * The default constructor is private. User should use either [[MetadataBuilder]] or
 * [[Metadata.fromJson()]] to create Metadata instances.
 *
 * @param map an immutable map that stores the data
 */
sealed class Metadata private[types] (private[types] val map: Map[String, Any])
  extends Serializable {

  }

object Metadata {

  /** Returns an empty Metadata. */
  def empty: Metadata = new Metadata(Map.empty)

}
