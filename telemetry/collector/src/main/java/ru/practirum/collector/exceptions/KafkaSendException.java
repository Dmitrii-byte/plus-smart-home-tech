package ru.practirum.collector.exceptions;

public class KafkaSendException extends CollectorServiceException {

    public KafkaSendException(String message, Throwable cause) {
        super(message, cause);
    }
}