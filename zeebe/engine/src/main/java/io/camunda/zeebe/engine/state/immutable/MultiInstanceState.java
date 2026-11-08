/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.engine.state.immutable;

import java.util.Optional;
import java.util.OptionalInt;
import org.agrona.DirectBuffer;

public interface MultiInstanceState {

  /**
   * Retrieves the size of the evaluated input collection of a multi-instance body.
   *
   * <p>Storing the input collection was introduced in later versions. It could be that existing
   * multi-instance bodies do not have an input collection stored in the state. Because of this the
   * method returns an optional.
   *
   * @param multiInstanceKey The key of the multi-instance element instance.
   * @return the size, or empty if no input collection is stored for this multi-instance body
   */
  OptionalInt getInputCollectionSize(long multiInstanceKey);

  /**
   * Retrieves a single element of the evaluated input collection of a multi-instance body, without
   * loading the other elements.
   *
   * @param multiInstanceKey The key of the multi-instance element instance.
   * @param index The zero-based index of the element.
   * @return the element, or empty if no input collection is stored or the index is out of bounds
   */
  Optional<DirectBuffer> getInputCollectionElement(long multiInstanceKey, int index);
}
