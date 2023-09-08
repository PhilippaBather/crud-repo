package com.batherphilippa.peopledb.annotation;

import com.batherphilippa.peopledb.domain.CrudOperation;

import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

// annotation allows our annotations to be found at runtime
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(MultiSQL.class)
public @interface SQL {
    String value();
    CrudOperation operationType();
}
