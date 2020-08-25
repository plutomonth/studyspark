package study.spark.sql.catalyst.util



class ArrayBasedMapData(val keyArray: ArrayData, val valueArray: ArrayData) extends MapData {

}

object ArrayBasedMapData {
  def apply(keys: Array[Any], values: Array[Any]): ArrayBasedMapData = {
    new ArrayBasedMapData(new GenericArrayData(keys), new GenericArrayData(values))
  }
}
