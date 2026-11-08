/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.engine.state.multiinstance;

import static org.assertj.core.api.Assertions.assertThat;

import io.camunda.zeebe.db.DbValue;
import io.camunda.zeebe.db.TransactionContext;
import io.camunda.zeebe.db.ZeebeDb;
import io.camunda.zeebe.db.impl.DbLong;
import io.camunda.zeebe.engine.state.mutable.MutableMultiInstanceState;
import io.camunda.zeebe.engine.state.mutable.MutableProcessingState;
import io.camunda.zeebe.engine.util.ProcessingStateExtension;
import io.camunda.zeebe.msgpack.UnpackedObject;
import io.camunda.zeebe.msgpack.property.ArrayProperty;
import io.camunda.zeebe.msgpack.value.StringValue;
import io.camunda.zeebe.protocol.ZbColumnFamilies;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ProcessingStateExtension.class)
public class MultiInstanceStateTest {
  private static final DirectBuffer FOO = new UnsafeBuffer("foo".getBytes());
  private static final DirectBuffer BAR = new UnsafeBuffer("bar".getBytes());
  private static final DirectBuffer BAZ = new UnsafeBuffer("baz".getBytes());

  private ZeebeDb<ZbColumnFamilies> zeebeDb;
  private TransactionContext transactionContext;
  private MutableProcessingState processingState;
  private MutableMultiInstanceState multiInstanceState;

  @BeforeEach
  public void setup() {
    multiInstanceState = processingState.getMultiInstanceState();
  }

  @Test
  void shouldGetSizeOfStoredInputCollection() {
    // given
    final long key = 123L;

    // when
    multiInstanceState.upsertInputCollection(key, List.of(FOO, BAR));

    // then
    assertThat(multiInstanceState.getInputCollectionSize(key)).hasValue(2);
  }

  @Test
  void shouldGetElementOfStoredInputCollectionByIndex() {
    // given
    final long key = 124L;

    // when
    multiInstanceState.upsertInputCollection(key, List.of(FOO, BAR));

    // then
    assertThat(multiInstanceState.getInputCollectionElement(key, 0)).contains(FOO);
    assertThat(multiInstanceState.getInputCollectionElement(key, 1)).contains(BAR);
  }

  @Test
  void shouldReturnEmptyForIndexOutOfBounds() {
    // given
    final long key = 125L;
    multiInstanceState.upsertInputCollection(key, List.of(FOO));

    // when
    final var element = multiInstanceState.getInputCollectionElement(key, 1);

    // then
    assertThat(element).isEmpty();
  }

  @Test
  void shouldDeleteInputCollection() {
    // given
    final long key = 456L;
    multiInstanceState.upsertInputCollection(key, List.of(BAZ));

    // when
    multiInstanceState.deleteInputCollection(key);

    // then
    assertThat(multiInstanceState.getInputCollectionSize(key)).isEmpty();
    assertThat(multiInstanceState.getInputCollectionElement(key, 0)).isEmpty();
  }

  @Test
  void shouldNotAffectOtherInputCollectionsOnDelete() {
    // given
    final long key = 457L;
    final long otherKey = 458L;
    multiInstanceState.upsertInputCollection(key, List.of(FOO));
    multiInstanceState.upsertInputCollection(otherKey, List.of(BAR));

    // when
    multiInstanceState.deleteInputCollection(key);

    // then
    assertThat(multiInstanceState.getInputCollectionSize(otherKey)).hasValue(1);
    assertThat(multiInstanceState.getInputCollectionElement(otherKey, 0)).contains(BAR);
  }

  @Test
  void shouldUpdateExistingInputCollectionOnDuplicateUpsert() {
    // given
    final long key = 321L;
    multiInstanceState.upsertInputCollection(key, List.of(FOO, BAR));

    // when - calling upsert again with the same key (e.g. on incident resolution re-activation)
    multiInstanceState.upsertInputCollection(key, List.of(BAZ));

    // then - the value is overwritten and no element of the previous collection remains
    assertThat(multiInstanceState.getInputCollectionSize(key)).hasValue(1);
    assertThat(multiInstanceState.getInputCollectionElement(key, 0)).contains(BAZ);
    assertThat(multiInstanceState.getInputCollectionElement(key, 1)).isEmpty();
  }

  @Test
  void shouldReturnEmptyForMissingInputCollection() {
    // given
    final long key = 789L;

    // when
    final var size = multiInstanceState.getInputCollectionSize(key);

    // then
    assertThat(size).isEmpty();
    assertThat(multiInstanceState.getInputCollectionElement(key, 0)).isEmpty();
  }

  @Test
  void shouldReadInputCollectionStoredBeforeElementsWereStoredIndividually() {
    // given - an input collection persisted by a previous version as a single value
    final long key = 999L;
    writeLegacyInputCollection(key, List.of(FOO, BAR));

    // when
    final var size = multiInstanceState.getInputCollectionSize(key);

    // then
    assertThat(size).hasValue(2);
    assertThat(multiInstanceState.getInputCollectionElement(key, 0)).contains(FOO);
    assertThat(multiInstanceState.getInputCollectionElement(key, 1)).contains(BAR);
    assertThat(multiInstanceState.getInputCollectionElement(key, 2)).isEmpty();
  }

  @Test
  void shouldDeleteInputCollectionStoredBeforeElementsWereStoredIndividually() {
    // given
    final long key = 998L;
    writeLegacyInputCollection(key, List.of(FOO));

    // when
    multiInstanceState.deleteInputCollection(key);

    // then
    assertThat(multiInstanceState.getInputCollectionSize(key)).isEmpty();
  }

  private void writeLegacyInputCollection(final long key, final List<DirectBuffer> collection) {
    final var dbKey = new DbLong();
    final var value = new LegacyPersistedInputCollection();
    zeebeDb
        .createColumnFamily(
            ZbColumnFamilies.MULTI_INSTANCE_INPUT_COLLECTION, transactionContext, dbKey, value)
        .upsert(withKey(dbKey, key), value.set(collection));
  }

  private static DbLong withKey(final DbLong dbKey, final long key) {
    dbKey.wrapLong(key);
    return dbKey;
  }

  /** The value layout of {@link PersistedInputCollection} before elements were stored per row. */
  private static final class LegacyPersistedInputCollection extends UnpackedObject
      implements DbValue {
    private final ArrayProperty<StringValue> inputCollectionProperty =
        new ArrayProperty<>("inputCollection", StringValue::new);

    LegacyPersistedInputCollection() {
      super(1);
      declareProperty(inputCollectionProperty);
    }

    LegacyPersistedInputCollection set(final List<DirectBuffer> collection) {
      inputCollectionProperty.reset();
      collection.forEach(element -> inputCollectionProperty.add().wrap(element));
      return this;
    }
  }
}
