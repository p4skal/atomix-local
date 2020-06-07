package de.p4skal.atomix.local;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.MemberId;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.utils.serializer.Serializer;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.Getter;

public class LocalCommunicator<S> {
  @Getter private final String name;
  @Getter private final PrimitiveType type;
  @Getter private final PrimitiveProtocol protocol;
  private final Serializer serializer;
  private final ClusterMembershipService membershipService;
  @Getter private final List<MemberId> memberIds = new CopyOnWriteArrayList<>();
  private final Map<MemberId, LocalCommunication<S>> members = Maps.newHashMap();

  public LocalCommunicator(
      String name,
      PrimitiveType type,
      PrimitiveProtocol protocol,
      ClusterMembershipService membershipService,
      Collection<LocalCommunication<S>> members,
      Serializer serializer) {
    this.name = name;
    this.type = type;
    this.protocol = protocol;
    this.serializer = serializer;
    this.membershipService = membershipService;
    members.forEach(
        member -> {
          this.memberIds.add(member.getMemberId());
          this.members.put(member.getMemberId(), member);
        });
    Collections.sort(this.memberIds);
  }

  public Collection<LocalCommunication<S>> getMembers() {
    return this.members.values();
  }

  public LocalCommunication<S> getMember(MemberId memberId) {
    return this.members.get(memberId);
  }

  public LocalCommunication<S> getMember(String key) {
    return getMember(getMemberId(key));
  }

  public LocalCommunication<S> getMember(Object key) {
    return getMember(getMemberId(key));
  }

  public CommunicationResults<S, Void> acceptAll(Consumer<S> operation) {
    return new CommunicationResults<>(
        this.getMembers().stream().map(proxy -> proxy.accept(operation)));
  }

  public <R> CommunicationResults<S, R> applyAll(Function<S, R> operation) {
    return new CommunicationResults<>(
        this.getMembers().stream().map(proxy -> proxy.apply(operation)));
  }

  public CommunicationResults<S, Void> acceptAll(BiConsumer<LocalCommunication<S>, S> operation) {
    return new CommunicationResults<>(
        this.getMembers().stream()
            .map(proxy -> proxy.accept(service -> operation.accept(proxy, service))));
  }

  public <R> CommunicationResults<S, R> applyAll(
      BiFunction<LocalCommunication<S>, S, R> operation) {
    return new CommunicationResults<>(
        this.getMembers().stream()
            .map(proxy -> proxy.apply(service -> operation.apply(proxy, service))));
  }

  public CommunicationResult<S, Void> acceptOn(MemberId memberId, Consumer<S> operation) {
    return getMember(memberId).accept(operation);
  }

  public <R> CommunicationResult<S, R> applyOn(MemberId memberId, Function<S, R> operation) {
    return getMember(memberId).apply(operation);
  }

  public CommunicationResult<S, Void> acceptBy(String key, Consumer<S> operation) {
    return getMember(key).accept(operation);
  }

  public <R> CommunicationResult<S, R> applyBy(String key, Function<S, R> operation) {
    return getMember(key).apply(operation);
  }

  public CommunicationResult<S, Void> acceptOnLocal(MemberId backUp, Consumer<S> operation) {
    return getMember(this.getLocalMember().orElse(backUp)).accept(operation);
  }

  public <R> CommunicationResult<S, R> applyOnLocal(MemberId backUp, Function<S, R> operation) {
    return getMember(this.getLocalMember().orElse(backUp)).apply(operation);
  }

  public CommunicationResult<S, Void> acceptByLocal(String key, Consumer<S> operation) {
    return getLocalCommunication().orElse(getMember(key)).accept(operation);
  }

  public <R> CommunicationResult<S, R> applyByLocal(String key, Function<S, R> operation) {
    return getLocalCommunication().orElse(getMember(key)).apply(operation);
  }

  private Optional<LocalCommunication<S>> getLocalCommunication() {
    return this.getLocalMember().map(this::getMember);
  }

  private Optional<MemberId> getLocalMember() {
    return Optional.ofNullable(this.membershipService.getLocalMember().id())
        .filter(this.memberIds::contains);
  }

  public MemberId getLocalOrMemberId(String key) {
    return this.getLocalMember().orElse(this.getMemberId(key));
  }

  public MemberId getMemberId(String key) {
    return this.partition(key, Lists.newArrayList(this.members.keySet()));
  }

  public MemberId getMemberId(Object key) {
    return this.partition(
        BaseEncoding.base16().encode(serializer.encode(key)),
        Lists.newArrayList(this.members.keySet()));
  }

  private MemberId partition(String key, List<MemberId> members) {
    int hash = Math.abs(Hashing.murmur3_32().hashUnencodedChars(key).asInt());
    return members.get(Hashing.consistentHash(hash, members.size()));
  }
}
