package ru.practirum.collector.exceptions;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.advice.GrpcAdvice;
import net.devh.boot.grpc.server.advice.GrpcExceptionHandler;

@Slf4j
@GrpcAdvice
public class GrpcExceptionAdvice {

    @GrpcExceptionHandler(EventProcessingException.class)
    public StatusRuntimeException handleEventProcessingException(EventProcessingException e) {
        log.error("Event processing error: {}", e.getMessage(), e);
        return Status.INVALID_ARGUMENT
                .withDescription("Invalid event data: " + e.getMessage())
                .withCause(e)
                .asRuntimeException();
    }

    @GrpcExceptionHandler(EventConversionException.class)
    public StatusRuntimeException handleEventConversionException(EventConversionException e) {
        log.error("Event conversion error: {}", e.getMessage(), e);
        return Status.INTERNAL
                .withDescription("Failed to convert event: " + e.getMessage())
                .withCause(e)
                .asRuntimeException();
    }

    @GrpcExceptionHandler(KafkaSendException.class)
    public StatusRuntimeException handleKafkaSendException(KafkaSendException e) {
        log.error("Kafka send error: {}", e.getMessage(), e);
        return Status.UNAVAILABLE
                .withDescription("Kafka service unavailable: " + e.getMessage())
                .withCause(e)
                .asRuntimeException();
    }

    @GrpcExceptionHandler(IllegalArgumentException.class)
    public StatusRuntimeException handleIllegalArgumentException(IllegalArgumentException e) {
        log.error("Illegal argument: {}", e.getMessage(), e);
        return Status.INVALID_ARGUMENT
                .withDescription(e.getMessage())
                .withCause(e)
                .asRuntimeException();
    }

    @GrpcExceptionHandler(NullPointerException.class)
    public StatusRuntimeException handleNullPointerException(NullPointerException e) {
        log.error("Null pointer exception: {}", e.getMessage(), e);
        return Status.INVALID_ARGUMENT
                .withDescription("Required field is missing: " + e.getMessage())
                .withCause(e)
                .asRuntimeException();
    }

    @GrpcExceptionHandler(Exception.class)
    public StatusRuntimeException handleGenericException(Exception e) {
        log.error("Unexpected error: {}", e.getMessage(), e);
        return Status.INTERNAL
                .withDescription("Internal server error: " + e.getMessage())
                .withCause(e)
                .asRuntimeException();
    }
}