package de.p4skal.atomix.local;

import com.google.common.base.Defaults;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.operation.Operations;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.utils.serializer.Serializer;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.Getter;

public class LocalCommunication<S> {
  @Getter private final MemberId memberId;
  private final String name;
  private final PrimitiveType primitiveType;
  private final Serializer serializer;
  private final ClusterCommunicationService communicationService;
  private final ClusterMembershipService membershipService;
  private final ServiceProxy<S> proxy;

  public LocalCommunication(
      MemberId memberId,
      String name,
      PrimitiveType primitiveType,
      Class<S> serviceType,
      Serializer serializer,
      ClusterCommunicationService communicationService,
      ClusterMembershipService membershipService) {
    this.name = name;
    this.memberId = memberId;
    this.primitiveType = primitiveType;
    this.serializer = serializer;
    this.communicationService = communicationService;
    this.membershipService = membershipService;
    ServiceProxyHandler serviceProxyHandler = new ServiceProxyHandler(serviceType);
    S serviceProxy =
        (S)
            Proxy.newProxyInstance(
                serviceType.getClassLoader(), new Class[] {serviceType}, serviceProxyHandler);
    proxy = new ServiceProxy<>(serviceProxy, serviceProxyHandler);
  }

  public CommunicationResult<S, Void> accept(Consumer<S> operation) {
    return proxy.accept(operation);
  }

  public <R> CommunicationResult<S, R> apply(Function<S, R> operation) {
    return proxy.apply(operation);
  }

  public boolean isLocalMember() {
    return this.memberId.equals(this.membershipService.getLocalMember().id());
  }

  protected Serializer serializer() {
    return serializer;
  }

  protected <T> byte[] encode(T object) {
    return object != null ? serializer().encode(object) : null;
  }

  protected <T> T decode(byte[] bytes) {
    return bytes != null ? serializer().decode(bytes) : null;
  }

  private class ServiceProxy<S> {
    private final S proxy;
    private final ServiceProxyHandler handler;

    ServiceProxy(S proxy, ServiceProxyHandler handler) {
      this.proxy = proxy;
      this.handler = handler;
    }

    CommunicationResult<S, Void> accept(Consumer<S> operation) {
      operation.accept(proxy);
      return new CommunicationResult<>(
          LocalCommunication.this, (CompletableFuture) handler.getResultFuture());
    }

    <T> CommunicationResult<S, T> apply(Function<S, T> operation) {
      operation.apply(proxy);
      return new CommunicationResult<>(
          LocalCommunication.this, (CompletableFuture) handler.getResultFuture());
    }
  }

  private class ServiceProxyHandler implements InvocationHandler {
    private final ThreadLocal<CompletableFuture> future = new ThreadLocal<>();
    private final Map<Method, OperationId> operations = new ConcurrentHashMap<>();

    private ServiceProxyHandler(Class<?> type) {
      this.operations.putAll(Operations.getMethodMap(type));
    }

    @Override
    public Object invoke(Object object, Method method, Object[] args) throws Throwable {
      OperationId operationId = operations.get(method);
      if (operationId != null) {
        final LocalRequest request =
            new LocalRequest(
                name,
                primitiveType.name(),
                PrimitiveOperation.operation(operationId, encode(args)));
        future.set(
            communicationService
                .<LocalRequest, LocalResponse>send(
                    "local",
                    request,
                    LocalCommunication.this::encode,
                    LocalCommunication.this::decode,
                    memberId)
                .thenApply(response -> response != null ? response.getResult() : null)
                .thenApply(LocalCommunication.this::decode).handle(
                (result, exception) -> {
                  if (exception != null) {
                    return Defaults.defaultValue(method.getReturnType());
                  }
                  return result;
                }));
      } else {
        throw new PrimitiveException("Unknown primitive operation: " + method.getName());
      }
      return Defaults.defaultValue(method.getReturnType());
    }

    <T> CompletableFuture<T> getResultFuture() {
      return future.get();
    }
  }
}
