package study.spark.sql.catalyst.rules

import study.spark.Logging
import study.spark.sql.catalyst.trees.TreeNode

abstract class Rule[TreeType <: TreeNode[_]] extends Logging {

}
