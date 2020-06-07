package de.p4skal.atomix.local;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.primitive.service.ServiceContext;
import io.atomix.primitive.session.Session;
import io.atomix.utils.time.LogicalClock;
import io.atomix.utils.time.WallClock;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.experimental.Accessors;

@Getter
@Accessors(fluent = true)
public class LocalServiceContext implements ServiceContext {
  private final PrimitiveId serviceId;
  private final String serviceName;
  private final PrimitiveType serviceType;
  private final MemberId localMemberId;
  private final PrimitiveService service;
  private final ServiceConfig serviceConfig;
  private final long currentIndex = -1L;
  private final Session currentSession = null;
  private final OperationType currentOperation = null;
  private final LogicalClock logicalClock = null;
  private final WallClock wallClock = null;

  public LocalServiceContext(PrimitiveId serviceId, PrimitiveType primitiveType,
      MemberId localMemberId) {
    this.serviceId = serviceId;
    this.serviceName = primitiveType.name();
    this.serviceType = primitiveType;
    this.localMemberId = localMemberId;
    this.serviceConfig = new ServiceConfig();
    this.service = primitiveType.newService(this.serviceConfig);
    this.service.init(this);
  }

  public CompletableFuture<LocalResponse> execute(LocalRequest request) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        final byte[] result = this.service
            .apply(new SimpleCommit<>(request.getOperation().id(), request.getOperation().value()));
        return LocalResponse.ok(result);
      } catch (Exception exception) {
        exception.printStackTrace();
        return LocalResponse.error();
      }
    });
  }
}
