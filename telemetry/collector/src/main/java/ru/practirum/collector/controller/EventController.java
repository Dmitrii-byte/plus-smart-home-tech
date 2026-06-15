package ru.practirum.collector.controller;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import ru.practirum.collector.service.CollectorService;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc.CollectorControllerImplBase;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

@Slf4j
@GrpcService
@RequiredArgsConstructor
public class EventController extends CollectorControllerImplBase {

    private final CollectorService collectorService;

    @Override
    public void collectSensorEvent(SensorEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            log.info("Received sensor event via gRPC: id={}, hubId={}, payloadCase={}",
                    request.getId(), request.getHubId(), request.getPayloadCase());

            collectorService.processSensorEvent(request);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();

            log.debug("Sensor event processed successfully: id={}", request.getId());

        } catch (Exception e) {
            log.error("Error processing sensor event: id={}, hubId={}",
                    request.getId(), request.getHubId(), e);

            responseObserver.onError(new StatusRuntimeException(
                    Status.INTERNAL
                            .withDescription("Failed to process sensor event: " + e.getMessage())
                            .withCause(e)
            ));
        }
    }

    @Override
    public void collectHubEvent(HubEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            log.info("Received hub event via gRPC: hubId={}, payloadCase={}",
                    request.getHubId(), request.getPayloadCase());
            collectorService.processHubEvent(request);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();

            log.debug("Hub event processed successfully: hubId={}", request.getHubId());

        } catch (Exception e) {
            log.error("Error processing hub event: hubId={}", request.getHubId(), e);

            responseObserver.onError(new StatusRuntimeException(
                    Status.INTERNAL
                            .withDescription("Failed to process hub event: " + e.getMessage())
                            .withCause(e)
            ));
        }
    }
}