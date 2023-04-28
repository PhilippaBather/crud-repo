package com.bather.philippa.peopledb.exception;

// runtime exceptions: don't have to be handled by calling code: may not be anticipated, and might not be able to do anything about
// checked exceptions: can anticipate and might be able to do something about
// example scenarios:
// - connection terminated
// - table name renamed - can't recover from this
public class UnableToSaveException extends RuntimeException {

    public UnableToSaveException(String message) {
        super(message);
    }
}
