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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ClosingIterator.JavaIteratorAsClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionPipe.CypherRowEntityTransformer
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionPipe.evaluateBatchSize
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.util.collection.EagerBuffer.createEagerBuffer

case class TransactionApplyPipe(source: Pipe, inner: Pipe, batchSize: Expression)(val id: Id = Id.INVALID_ID)
    extends PipeWithSource(source) with TransactionPipe {

  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    val batchSizeLong = evaluateBatchSize(batchSize, state)
    val entityTransformer = new CypherRowEntityTransformer(state.query.entityTransformer)
    val memoryTracker = state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)

    input
      .eagerGrouped(batchSizeLong, memoryTracker)
      .flatMap { batch =>
        val innerResult = createEagerBuffer[CypherRow](memoryTracker, math.min(batch.size(), 1024).toInt)
        createInnerResultsInNewTransaction(state, batch) { innerRow =>
          // Row based caching relies on the transaction state to avoid stale reads (see AbstractCachedProperty.apply).
          // Since we do not share the transaction state we must clear the cached properties.
          innerRow.invalidateCachedProperties()
          innerResult.add(entityTransformer.copyWithEntityWrappingValuesRebound(innerRow))
        }
        batch.close()
        innerResult.autoClosingIterator().asClosingIterator
      }
  }
}
