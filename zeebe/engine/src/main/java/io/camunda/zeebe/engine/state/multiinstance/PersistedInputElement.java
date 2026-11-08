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
import io.camunda.zeebe.msgpack.property.BinaryProperty;
import org.agrona.DirectBuffer;

/** A single element of an evaluated multi-instance input collection. */
public class PersistedInputElement extends UnpackedObject implements DbValue {
  private final BinaryProperty elementProperty = new BinaryProperty("element");

  public PersistedInputElement() {
    super(1);
    declareProperty(elementProperty);
  }

  public DirectBuffer getElement() {
    return elementProperty.getValue();
  }

  public PersistedInputElement setElement(final DirectBuffer element) {
    elementProperty.setValue(element);
    return this;
  }
}
