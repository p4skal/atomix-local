package de.p4skal.atomix.local;

import com.google.common.collect.Maps;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.utils.Managed;
import io.atomix.utils.serializer.Serializer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class LocalServerContext implements Managed<Void> {
  private final Serializer serializer;
  private final PrimitiveTypeRegistry primitiveTypes;
  private final ClusterMembershipService clusterMembershipService;
  private final ClusterCommunicationService clusterCommunicationService;
  private final Map<String, CompletableFuture<LocalServiceContext>> services =
      Maps.newConcurrentMap();
  private final AtomicBoolean running = new AtomicBoolean();

  public LocalServerContext(
      PrimitiveType primitiveType, PartitionManagementService managementService) {
    this(
        Serializer.using(primitiveType.namespace()),
        managementService.getPrimitiveTypes(),
        managementService.getMembershipService(),
        managementService.getMessagingService());
  }

  public LocalServerContext(
      PrimitiveType primitiveType,
      PrimitiveTypeRegistry primitiveTypes,
      ClusterMembershipService clusterMembershipService,
      ClusterCommunicationService clusterCommunicationService) {
    this(
        Serializer.using(primitiveType.namespace()),
        primitiveTypes,
        clusterMembershipService,
        clusterCommunicationService);
  }

  private CompletableFuture<LocalResponse> execute(LocalRequest request) {
    return getService(request).thenCompose(service -> service.execute(request));
  }

  private CompletableFuture<LocalServiceContext> getService(LocalRequest request) {
    return services.computeIfAbsent(
        request.getName(),
        n ->
            CompletableFuture.supplyAsync(
                () -> {
                  final PrimitiveType primitiveType =
                      primitiveTypes.getPrimitiveType(request.getType());
                  return new LocalServiceContext(
                      PrimitiveId.from(request.getName()),
                      primitiveType,
                      this.clusterMembershipService.getLocalMember().id());
                }));
  }

  @Override
  public CompletableFuture<Void> start() {
    this.running.set(true);
    return this.clusterCommunicationService.<LocalRequest, LocalResponse>subscribe(
        "local", serializer::decode, this::execute, serializer::encode);
  }

  @Override
  public boolean isRunning() {
    return this.running.get();
  }

  @Override
  public CompletableFuture<Void> stop() {
    this.running.set(false);
    return CompletableFuture.runAsync(() -> this.clusterCommunicationService.unsubscribe("local"));
  }
}
