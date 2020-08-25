package study.spark.sql.catalyst.expressions;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface ExpressionDescription {
    String usage() default "_FUNC_ is undocumented";
    String extended() default "No example for _FUNC_.";
}
