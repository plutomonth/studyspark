package study.spark.sql.catalyst.util

import scala.util.parsing.combinator.syntactical.StandardTokenParsers

/**
 * This is a data type parser that can be used to parse string representations of data types
 * provided in SQL queries. This parser is mixed in with DDLParser and SqlParser.
 */
private[sql] trait DataTypeParser extends StandardTokenParsers {

}
