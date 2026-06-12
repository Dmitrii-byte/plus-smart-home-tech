package ru.practirum.collector.exceptions;

public class CollectorServiceException extends RuntimeException {

    public CollectorServiceException(String message) {
        super(message);
    }

    public CollectorServiceException(String message, Throwable cause) {
        super(message, cause);
    }
}