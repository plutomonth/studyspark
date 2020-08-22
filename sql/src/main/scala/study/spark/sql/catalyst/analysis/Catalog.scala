package study.spark.sql.catalyst.analysis

import study.spark.sql.catalyst.CatalystConf

/**
 * An interface for looking up relations by name.  Used by an [[Analyzer]].
 */
trait Catalog {

}


class SimpleCatalog(val conf: CatalystConf) extends Catalog {

}