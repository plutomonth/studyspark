package study.spark.sql.catalyst.expressions;

import study.spark.sql.types.DataType;

public interface SpecializedGetters {
    boolean isNullAt(int ordinal);

    Object get(int ordinal, DataType dataType);

}
