/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PrimitiveLongHelper
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.BFSPruningVarLengthExpandPipe.bfsIterator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.runtime.slotted.pipes.BFSPruningVarLengthExpandSlottedPipe.createPredicates
import org.neo4j.cypher.internal.runtime.slotted.pipes.VarLengthExpandSlottedPipe.SlottedVariablePredicate
import org.neo4j.cypher.internal.runtime.slotted.pipes.VarLengthExpandSlottedPipe.predicateIsTrue
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor

import java.util.function.LongPredicate
import java.util.function.Predicate

case class BFSPruningVarLengthExpandSlottedPipe(
  source: Pipe,
  fromSlot: Slot,
  toOffset: Int,
  types: RelationshipTypes,
  dir: SemanticDirection,
  includeStartNode: Boolean,
  max: Int,
  slots: SlotConfiguration,
  nodePredicates: Seq[SlottedVariablePredicate],
  relationshipPredicates: Seq[SlottedVariablePredicate]
)(val id: Id = Id.INVALID_ID) extends PipeWithSource(source) with Pipe {
  self =>

  private val getFromNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(fromSlot, throwOnTypeError = false)

  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    input.flatMap {
      inputRow =>
        {
          val fromNode = getFromNodeFunction.applyAsLong(inputRow)
          if (entityIsNull(fromNode)) {
            ClosingIterator.empty
          } else {
            if (
              nodePredicates.forall(nodePred =>
                predicateIsTrue(
                  inputRow,
                  state,
                  nodePred.tempOffset,
                  nodePred.predicate,
                  state.query.nodeById(fromNode)
                )
              )
            ) {
              val (nP, rP) =
                createPredicates(
                  state,
                  inputRow,
                  nodePredicates,
                  relationshipPredicates
                )

              val memoryTracker = state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)
              val expand = bfsIterator(
                state.query,
                fromNode,
                types,
                dir,
                includeStartNode,
                max,
                nP,
                rP,
                memoryTracker
              )

              PrimitiveLongHelper.map(
                expand,
                endNode => {
                  val outputRow = SlottedRow(slots)
                  outputRow.copyAllFrom(inputRow)
                  outputRow.setLongAt(toOffset, endNode)
                  outputRow
                }
              )
            } else {
              ClosingIterator.empty
            }

          }
        }
    }
  }
}

object BFSPruningVarLengthExpandSlottedPipe {

  def createPredicates(
    state: QueryState,
    row: ReadableRow,
    nodePredicates: Seq[SlottedVariablePredicate],
    relationshipPredicates: Seq[SlottedVariablePredicate]
  ): (LongPredicate, Predicate[RelationshipTraversalCursor]) = {

    def toLongPredicate(f: Long => Boolean): LongPredicate = (value: Long) => f(value)
    def createNodePredicate =
      toLongPredicate(n =>
        nodePredicates.forall(nodePredicate =>
          predicateIsTrue(row, state, nodePredicate.tempOffset, nodePredicate.predicate, state.query.nodeById(n))
        )
      )

    def createRelationshipPredicate: Predicate[RelationshipTraversalCursor] =
      (t: RelationshipTraversalCursor) =>
        relationshipPredicates.forall(relPredicate =>
          predicateIsTrue(
            row,
            state,
            relPredicate.tempOffset,
            relPredicate.predicate,
            state.query.relationshipById(
              t.relationshipReference(),
              t.sourceNodeReference(),
              t.targetNodeReference(),
              t.`type`()
            )
          )
        )

    (createNodePredicate, createRelationshipPredicate)
  }
}
