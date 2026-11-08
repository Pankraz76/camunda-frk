/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.engine.processing.bpmn.behavior;

import io.camunda.zeebe.engine.processing.bpmn.BpmnElementContext;
import io.camunda.zeebe.engine.processing.common.ExpressionProcessor;
import io.camunda.zeebe.engine.processing.common.Failure;
import io.camunda.zeebe.engine.processing.deployment.model.element.ExecutableMultiInstanceBody;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.StateWriter;
import io.camunda.zeebe.protocol.impl.record.value.multiinstance.MultiInstanceRecord;
import io.camunda.zeebe.protocol.record.intent.MultiInstanceIntent;
import io.camunda.zeebe.util.Either;
import java.util.List;
import java.util.Optional;
import org.agrona.DirectBuffer;

public class MultiInstanceInputCollectionBehavior {
  private final ExpressionProcessor expressionProcessor;
  private final BpmnStateBehavior stateBehavior;
  private final StateWriter stateWriter;

  public MultiInstanceInputCollectionBehavior(
      final ExpressionProcessor expressionProcessor,
      final BpmnStateBehavior stateBehavior,
      final StateWriter stateWriter) {
    this.expressionProcessor = expressionProcessor;
    this.stateBehavior = stateBehavior;
    this.stateWriter = stateWriter;
  }

  public Either<Failure, List<DirectBuffer>> initializeInputCollection(
      final ExecutableMultiInstanceBody element, final BpmnElementContext context) {
    return evaluateInputCollection(element, context)
        .map(
            inputCollection -> {
              final var record = new MultiInstanceRecord().setInputCollection(inputCollection);
              stateWriter.appendFollowUpEvent(
                  context.getElementInstanceKey(),
                  MultiInstanceIntent.INPUT_COLLECTION_EVALUATED,
                  record);
              return inputCollection;
            });
  }

  /**
   * Returns the size of the input collection. Reads the stored size, or evaluates the input
   * collection if the multi-instance body was activated before input collections were stored.
   */
  public Either<Failure, Integer> getInputCollectionSize(
      final ExecutableMultiInstanceBody element, final BpmnElementContext context) {
    final var storedSize = stateBehavior.getInputCollectionSize(context.getElementInstanceKey());
    if (storedSize.isPresent()) {
      return Either.right(storedSize.getAsInt());
    }
    return evaluateInputCollection(element, context).map(List::size);
  }

  /**
   * Returns the element at the given index of the input collection, or an empty optional if the
   * index is out of bounds. Reads only that element from the state, or evaluates the input
   * collection if the multi-instance body was activated before input collections were stored.
   */
  public Either<Failure, Optional<DirectBuffer>> getInputElement(
      final ExecutableMultiInstanceBody element,
      final BpmnElementContext context,
      final int index) {
    final var multiInstanceKey = context.getElementInstanceKey();
    if (stateBehavior.getInputCollectionSize(multiInstanceKey).isPresent()) {
      return Either.right(stateBehavior.getInputCollectionElement(multiInstanceKey, index));
    }
    return evaluateInputCollection(element, context)
        .map(
            collection ->
                index >= 0 && index < collection.size()
                    ? Optional.of(collection.get(index))
                    : Optional.empty());
  }

  private Either<Failure, List<DirectBuffer>> evaluateInputCollection(
      final ExecutableMultiInstanceBody element, final BpmnElementContext context) {
    final var inputCollectionExpression = element.getLoopCharacteristics().getInputCollection();
    return expressionProcessor.evaluateArrayExpression(
        inputCollectionExpression, context.getElementInstanceKey(), context.getTenantId());
  }
}
