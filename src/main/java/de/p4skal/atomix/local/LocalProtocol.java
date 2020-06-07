package de.p4skal.atomix.local;

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.utils.serializer.Serializer;
import java.util.List;
import java.util.stream.Collectors;

public class LocalProtocol implements PrimitiveProtocol {
  public static final LocalProtocol.Type TYPE = new LocalProtocol.Type();

  public static LocalProtocol instance() {
    return new LocalProtocol(new LocalProtocolConfig());
  }

  public static LocalProtocolBuilder builder() {
    return new LocalProtocolBuilder(new LocalProtocolConfig());
  }

  public static final class Type implements PrimitiveProtocol.Type<LocalProtocolConfig> {
    private static final String NAME = "local";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public LocalProtocolConfig newConfig() {
      return new LocalProtocolConfig();
    }

    @Override
    public PrimitiveProtocol newProtocol(LocalProtocolConfig config) {
      return new LocalProtocol(config);
    }
  }

  protected final LocalProtocolConfig config;

  protected LocalProtocol(LocalProtocolConfig config) {
    this.config = config;
  }

  @Override
  public PrimitiveProtocol.Type type() {
    return TYPE;
  }

  public <S> LocalCommunicator<S> newCommunicator(
      String primitiveName,
      PrimitiveType primitiveType,
      Class<S> serviceType,
      ClusterCommunicationService communicationService,
      ClusterMembershipService membershipService) {
    final Serializer serializer = Serializer.using(primitiveType.namespace());
    final List<LocalCommunication<S>> communications =
        this.config.getMembers().stream()
            .map(MemberId::from)
            .map(
                memberId ->
                    new LocalCommunication<>(
                        memberId, primitiveName, primitiveType, serviceType, serializer, communicationService,
                        membershipService))
            .collect(Collectors.toList());
    return new LocalCommunicator<>(primitiveName, primitiveType, this, membershipService, communications, serializer);
  }
}
