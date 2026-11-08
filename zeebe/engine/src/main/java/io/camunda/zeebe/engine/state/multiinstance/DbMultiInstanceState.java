/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.engine.state.multiinstance;

import io.camunda.zeebe.db.ColumnFamily;
import io.camunda.zeebe.db.TransactionContext;
import io.camunda.zeebe.db.ZeebeDb;
import io.camunda.zeebe.db.impl.DbCompositeKey;
import io.camunda.zeebe.db.impl.DbInt;
import io.camunda.zeebe.db.impl.DbLong;
import io.camunda.zeebe.engine.state.mutable.MutableMultiInstanceState;
import io.camunda.zeebe.protocol.ZbColumnFamilies;
import io.camunda.zeebe.util.buffer.BufferUtil;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import org.agrona.DirectBuffer;

public class DbMultiInstanceState implements MutableMultiInstanceState {

  private final DbLong multiInstanceKey;
  private final PersistedInputCollection inputCollection;
  private final ColumnFamily<DbLong, PersistedInputCollection> inputCollectionColumnFamily;

  private final DbInt elementIndex;
  private final DbCompositeKey<DbLong, DbInt> multiInstanceKeyAndElementIndex;
  private final PersistedInputElement inputElement;
  private final ColumnFamily<DbCompositeKey<DbLong, DbInt>, PersistedInputElement>
      inputElementColumnFamily;

  public DbMultiInstanceState(
      final ZeebeDb<ZbColumnFamilies> zeebeDb, final TransactionContext transactionContext) {
    multiInstanceKey = new DbLong();
    inputCollection = new PersistedInputCollection();
    inputCollectionColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.MULTI_INSTANCE_INPUT_COLLECTION,
            transactionContext,
            multiInstanceKey,
            inputCollection);

    elementIndex = new DbInt();
    multiInstanceKeyAndElementIndex = new DbCompositeKey<>(multiInstanceKey, elementIndex);
    inputElement = new PersistedInputElement();
    inputElementColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.MULTI_INSTANCE_INPUT_COLLECTION_ELEMENT,
            transactionContext,
            multiInstanceKeyAndElementIndex,
            inputElement);
  }

  @Override
  public void upsertInputCollection(
      final long multiInstanceKey, final List<DirectBuffer> inputCollection) {
    deleteInputCollection(multiInstanceKey);

    this.multiInstanceKey.wrapLong(multiInstanceKey);
    for (int index = 0; index < inputCollection.size(); index++) {
      elementIndex.wrapInt(index);
      inputElement.setElement(inputCollection.get(index));
      inputElementColumnFamily.insert(multiInstanceKeyAndElementIndex, inputElement);
    }

    this.inputCollection.setSize(inputCollection.size());
    inputCollectionColumnFamily.insert(this.multiInstanceKey, this.inputCollection);
  }

  @Override
  public void deleteInputCollection(final long multiInstanceKey) {
    this.multiInstanceKey.wrapLong(multiInstanceKey);
    final var persistedInputCollection = inputCollectionColumnFamily.get(this.multiInstanceKey);
    if (persistedInputCollection == null) {
      return;
    }

    if (!persistedInputCollection.isLegacy()) {
      final int size = persistedInputCollection.getSize();
      for (int index = 0; index < size; index++) {
        elementIndex.wrapInt(index);
        inputElementColumnFamily.deleteExisting(multiInstanceKeyAndElementIndex);
      }
    }
    inputCollectionColumnFamily.deleteExisting(this.multiInstanceKey);
  }

  @Override
  public OptionalInt getInputCollectionSize(final long multiInstanceKey) {
    this.multiInstanceKey.wrapLong(multiInstanceKey);
    final var persistedInputCollection = inputCollectionColumnFamily.get(this.multiInstanceKey);
    return persistedInputCollection == null
        ? OptionalInt.empty()
        : OptionalInt.of(persistedInputCollection.getSize());
  }

  @Override
  public Optional<DirectBuffer> getInputCollectionElement(
      final long multiInstanceKey, final int index) {
    this.multiInstanceKey.wrapLong(multiInstanceKey);
    elementIndex.wrapInt(index);
    final var persistedElement = inputElementColumnFamily.get(multiInstanceKeyAndElementIndex);
    if (persistedElement != null) {
      return Optional.of(BufferUtil.cloneBuffer(persistedElement.getElement()));
    }

    // fall back to input collections stored inline by earlier versions
    final var persistedInputCollection = inputCollectionColumnFamily.get(this.multiInstanceKey);
    return Optional.ofNullable(persistedInputCollection)
        .flatMap(collection -> collection.getLegacyElement(index))
        .map(BufferUtil::cloneBuffer);
  }
}
