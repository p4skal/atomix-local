package de.p4skal.atomix.local;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Sets;
import io.atomix.cluster.Member;
import io.atomix.cluster.MemberId;
import io.atomix.primitive.protocol.PrimitiveProtocolBuilder;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LocalProtocolBuilder extends
    PrimitiveProtocolBuilder<LocalProtocolBuilder, LocalProtocolConfig, LocalProtocol> {

  protected LocalProtocolBuilder(LocalProtocolConfig config) {
    super(config);
  }

  public LocalProtocolBuilder withMembers(String... members) {
    return withMembers(Arrays.asList(members));
  }

  public LocalProtocolBuilder withMembers(MemberId... members) {
    return withMembers(Stream.of(members).map(nodeId -> nodeId.id()).collect(Collectors.toList()));
  }

  public LocalProtocolBuilder withMembers(Member... members) {
    return withMembers(Stream.of(members).map(node -> node.id().id()).collect(Collectors.toList()));
  }

  public LocalProtocolBuilder withMembers(Collection<String> members) {
    config.setMembers(Sets.newHashSet(checkNotNull(members, "members cannot be null")));
    return this;
  }

  @Override
  public LocalProtocol build() {
    return new LocalProtocol(config);
  }
}
