package study.spark.sql.catalyst.expressions.codegen

import org.apache.commons.lang3.StringUtils

/**
 * An utility class that indents a block of code based on the curly braces and parentheses.
 * This is used to prettify generated code when in debug mode (or exceptions).
 *
 * Written by Matei Zaharia.
 */
object CodeFormatter {
  def format(code: CodeAndComment): String = {
    new CodeFormatter().addLines(
      StringUtils.replaceEach(
        code.body,
        code.comment.keys.toArray,
        code.comment.values.toArray)
    ).result
  }
}

private class CodeFormatter {
  private val code = new StringBuilder
  private var indentLevel = 0
  private val indentSize = 2
  private var indentString = ""
  private var currentLine = 1

  private def addLine(line: String): Unit = {
    val indentChange =
      line.count(c => "({".indexOf(c) >= 0) - line.count(c => ")}".indexOf(c) >= 0)
    val newIndentLevel = math.max(0, indentLevel + indentChange)
    // Lines starting with '}' should be de-indented even if they contain '{' after;
    // in addition, lines ending with ':' are typically labels
    val thisLineIndent = if (line.startsWith("}") || line.startsWith(")") || line.endsWith(":")) {
      " " * (indentSize * (indentLevel - 1))
    } else {
      indentString
    }
    code.append(f"/* ${currentLine}%03d */ ")
    code.append(thisLineIndent)
    code.append(line)
    code.append("\n")
    indentLevel = newIndentLevel
    indentString = " " * (indentSize * newIndentLevel)
    currentLine += 1
  }

  private def addLines(code: String): CodeFormatter = {
    code.split('\n').foreach(s => addLine(s.trim()))
    this
  }

  private def result(): String = code.result()
}