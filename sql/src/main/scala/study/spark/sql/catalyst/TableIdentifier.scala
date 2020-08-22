package study.spark.sql.catalyst

/**
 * Identifies a `table` in `database`.  If `database` is not defined, the current database is used.
 */
private[sql] case class TableIdentifier(table: String, database: Option[String]) {
  def this(table: String) = this(table, None)

  override def toString: String = quotedString

  def quotedString: String = database.map(db => s"`$db`.`$table`").getOrElse(s"`$table`")

  def unquotedString: String = database.map(db => s"$db.$table").getOrElse(table)
}

private[sql] object TableIdentifier {
  def apply(tableName: String): TableIdentifier = new TableIdentifier(tableName)
}
