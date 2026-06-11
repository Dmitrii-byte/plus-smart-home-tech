package ru.practirum.collector.exceptions;

public class SerializationException extends RuntimeException {
    public SerializationException(String message, Exception ex) {
        super(message, ex);
    }
}
