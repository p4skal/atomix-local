package de.p4skal.atomix.local;

import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.PrimitiveProtocolConfig;
import java.util.Set;

public class LocalProtocolConfig extends PrimitiveProtocolConfig<LocalProtocolConfig> {
  private Set<String> members;

  @Override
  public PrimitiveProtocol.Type getType() {
    return LocalProtocol.TYPE;
  }

  public Set<String> getMembers() {
    return members;
  }

  public void setMembers(Set<String> members) {
    this.members = members;
  }

}
