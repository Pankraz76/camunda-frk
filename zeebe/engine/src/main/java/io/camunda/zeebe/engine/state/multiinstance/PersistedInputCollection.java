/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.engine.state.multiinstance;

import io.camunda.zeebe.db.DbValue;
import io.camunda.zeebe.msgpack.UnpackedObject;
import io.camunda.zeebe.msgpack.property.ArrayProperty;
import io.camunda.zeebe.msgpack.property.IntegerProperty;
import io.camunda.zeebe.msgpack.value.StringValue;
import java.util.Optional;
import org.agrona.DirectBuffer;

/**
 * The size of an evaluated multi-instance input collection. The elements themselves are stored
 * individually, see {@link PersistedInputElement}.
 *
 * <p>Earlier versions stored all elements inline in {@code inputCollection} and no {@code size}.
 * Such entries are still read, but never written anymore.
 */
public class PersistedInputCollection extends UnpackedObject implements DbValue {
  private static final int NO_SIZE = -1;

  private final ArrayProperty<StringValue> legacyInputCollectionProperty =
      new ArrayProperty<>("inputCollection", StringValue::new);
  private final IntegerProperty sizeProperty = new IntegerProperty("size", NO_SIZE);

  public PersistedInputCollection() {
    super(2);
    declareProperty(legacyInputCollectionProperty).declareProperty(sizeProperty);
  }

  public int getSize() {
    return isLegacy() ? legacyInputCollectionProperty.size() : sizeProperty.getValue();
  }

  public PersistedInputCollection setSize(final int size) {
    legacyInputCollectionProperty.reset();
    sizeProperty.setValue(size);
    return this;
  }

  /**
   * @return true if the elements are stored inline, as written by earlier versions
   */
  public boolean isLegacy() {
    return sizeProperty.getValue() == NO_SIZE;
  }

  /**
   * @return the inline stored element at the given index, only present for legacy entries
   */
  public Optional<DirectBuffer> getLegacyElement(final int index) {
    if (!isLegacy() || index < 0 || index >= legacyInputCollectionProperty.size()) {
      return Optional.empty();
    }
    return Optional.of(legacyInputCollectionProperty.get(index).getValue());
  }
}
