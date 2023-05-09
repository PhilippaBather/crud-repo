package com.bather.philippa.peopledb.annotation;

// Java uses interface to implement annotations

import com.bather.philippa.peopledb.model.CrudOperation;

import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
@Repeatable(MultiSQL.class)
public @interface SQL {
    String value();  // as only one param no need to explicitly refer to value attribute in annotation
    // int age() default 30;  // note: default attribute

    CrudOperation operationType();
}
