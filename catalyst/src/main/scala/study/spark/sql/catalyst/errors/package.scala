package study.spark.sql.catalyst

import study.spark.sql.catalyst.trees.TreeNode

package object errors {
  class TreeNodeException[TreeType <: TreeNode[_]](
        tree: TreeType, msg: String, cause: Throwable)
    extends Exception(msg, cause) {

    // Yes, this is the same as a default parameter, but... those don't seem to work with SBT
    // external project dependencies for some reason.
    def this(tree: TreeType, msg: String) = this(tree, msg, null)
  }

  /**
   *  Wraps any exceptions that are thrown while executing `f` in a
   *  TreeNodeException, attaching the provided `tree`.
   */
  def attachTree[TreeType <: TreeNode[_], A](tree: TreeType, msg: String = "")(f: => A): A = {
    try f catch {
      case e: Exception => throw new TreeNodeException(tree, msg, e)
    }
  }
}
