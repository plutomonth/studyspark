package study.spark.sql.catalyst

import scala.util.parsing.combinator.syntactical.StandardTokenParsers
import scala.util.parsing.combinator.PackratParsers
import scala.util.parsing.combinator.lexical.StdLexical

private[sql] abstract class AbstractSparkSQLParser
  extends StandardTokenParsers with PackratParsers {

  /* One time initialization of lexical.This avoid reinitialization of  lexical in parse method */
  protected lazy val initLexical: Unit = lexical.initialize(reservedWords)

  protected case class Keyword(str: String) {
    def normalize: String = lexical.normalizeKeyword(str)
    def parser: Parser[String] = normalize
  }

  // Set the keywords as empty by default, will change that later.
  override val lexical = new SqlLexical

  // By default, use Reflection to find the reserved words defined in the sub class.
  // NOTICE, Since the Keyword properties defined by sub class, we couldn't call this
  // method during the parent class instantiation, because the sub class instance
  // isn't created yet.
  protected lazy val reservedWords: Seq[String] =
  this
    .getClass
    .getMethods
    .filter(_.getReturnType == classOf[Keyword])
    .map(_.invoke(this).asInstanceOf[Keyword].normalize)
}

class SqlLexical extends StdLexical {
  /* This is a work around to support the lazy setting */
  def initialize(keywords: Seq[String]): Unit = {
    reserved.clear()
    reserved ++= keywords
  }

  /* Normal the keyword string */
  def normalizeKeyword(str: String): String = str.toLowerCase
}