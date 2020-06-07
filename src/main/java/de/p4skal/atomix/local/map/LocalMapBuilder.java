package de.p4skal.atomix.local.map;

import com.google.common.io.BaseEncoding;
import io.atomix.core.map.MapBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyCompatibleBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.concurrent.Futures;
import java.util.concurrent.CompletableFuture;
import de.p4skal.atomix.local.LocalCommunicator;
import de.p4skal.atomix.local.LocalProtocol;

public class LocalMapBuilder<K, V>
    extends MapBuilder<LocalMapBuilder<K, V>, LocalMapConfig, LocalMap<K, V>, K, V>
    implements ProxyCompatibleBuilder<LocalMapBuilder<K, V>> {

  public LocalMapBuilder(
      String name, LocalMapConfig config, PrimitiveManagementService managementService) {
    super(LocalMapType.instance(), name, config, managementService);
  }

  @Override
  @Deprecated
  public LocalMapBuilder<K, V> withProtocol(ProxyProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }

  public LocalMapBuilder<K, V> withProtocol(LocalProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<LocalMap<K, V>> buildAsync() {
    return this.buildMapProxy()
        .thenApply(
            rawMap -> {
              AsyncLocalMap<K, V> map =
                  new TranscodingAsyncLocalMap<>(
                      rawMap,
                      key -> BaseEncoding.base16().encode(this.serializer().encode(key)),
                      string -> this.serializer().decode(BaseEncoding.base16().decode(string)),
                      this.serializer()::encode,
                      this.serializer()::decode);

              if (this.config.isReadOnly()) map = new UnmodifiableAsyncLocalMap<>(map);

              if (this.config.getCacheConfig().isEnabled())
                throw new UnsupportedOperationException("Caching not supported!");

              return map.sync();
            });
  }

  private CompletableFuture<AsyncLocalMap<String, byte[]>> buildMapProxy() {
    final PrimitiveProtocol protocol = protocol();
    if (protocol instanceof ProxyProtocol) {
      return this.newProxy((ProxyProtocol) protocol, new ServiceConfig())
          .thenCompose(
              proxy ->
                  new PartitionedLocalMapProxy(
                          (ProxyClient) proxy,
                          this.managementService.getPrimitiveRegistry(),
                          this.managementService.getMembershipService(),
                          this.managementService.getPartitionService())
                      .connect());
    } else if (protocol instanceof LocalProtocol) {
      return this.newCommunicator((LocalProtocol) protocol)
          .thenApply(communicator -> new ClusteredLocalMapProxy((LocalCommunicator) communicator));
    }
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  protected CompletableFuture<LocalCommunicator<LocalMapService>> newCommunicator(
      LocalProtocol protocol) {
    try {
      return CompletableFuture.completedFuture(
          protocol.newCommunicator(
              name,
              type,
              LocalMapService.class,
              managementService.getCommunicationService(),
              managementService.getMembershipService()));
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  protected CompletableFuture<ProxyClient<LocalMapService>> newProxy(
      ProxyProtocol protocol, ServiceConfig config) {
    try {
      return CompletableFuture.completedFuture(
          protocol.newProxy(
              name, type, LocalMapService.class, config, managementService.getPartitionService()));
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }
}
