package de.p4skal.atomix.local.map;

import static com.google.common.base.MoreObjects.toStringHelper;

import io.atomix.core.iterator.impl.IteratorBatch;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.operation.impl.DefaultOperationId;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;
import java.util.UUID;
import de.p4skal.atomix.local.LocalRequest;
import de.p4skal.atomix.local.LocalResponse;

public class LocalMapType<K, V> implements
    PrimitiveType<LocalMapBuilder<K, V>, LocalMapConfig, LocalMap<K, V>> {
  private static final String NAME = "local-map";

  private static final LocalMapType INSTANCE = new LocalMapType();

  @SuppressWarnings("unchecked")
  public static <K, V> LocalMapType<K, V> instance() {
    return INSTANCE;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public Namespace namespace() {
    return Namespace.builder()
        .register(PrimitiveType.super.namespace())
        .register(Namespaces.BASIC)
        .nextId(Namespaces.BEGIN_USER_CUSTOM_ID)
        .register(IteratorBatch.class)
        .register(byte[].class)
        .register(byte[].class)
        .register(LocalRequest.class)
        .register(LocalResponse.class)
        .register(PrimitiveOperation.class)
        .register(DefaultOperationId.class)
        .register(OperationType.class)
        .register(UUID.class)
        .build();
  }

  @Override
  public PrimitiveService newService(ServiceConfig config) {
    return new DefaultLocalMapService(this);
  }

  @Override
  public LocalMapConfig newConfig() {
    return new LocalMapConfig();
  }

  @Override
  public LocalMapBuilder<K, V> newBuilder(String name, LocalMapConfig config, PrimitiveManagementService managementService) {
    return new LocalMapBuilder<>(name, config, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .toString();
  }
}
