/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.rewriting

import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ShowTransactionsClause
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.TerminateTransactionsClause
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.factory.neo4j.JavaCCParser
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.rewriting.rewriters.expandShowWhere
import org.neo4j.cypher.internal.rewriting.rewriters.expandStar
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeWithAndReturnClauses
import org.neo4j.cypher.internal.rewriting.rewriters.rewriteShowQuery
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.devNullLogger
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ExpandStarTest extends CypherFunSuite with AstConstructionTestSupport {

  test("rewrites * in return") {
    assertRewrite(
      "match (n) return *",
      "match (n) return n"
    )

    assertRewrite(
      "match (n),(c) return *",
      "match (n),(c) return c,n"
    )

    assertRewrite(
      "match (n)-->(c) return *",
      "match (n)-->(c) return c,n"
    )

    assertRewrite(
      "match (n)-[r]->(c) return *",
      "match (n)-[r]->(c) return c,n,r"
    )

    assertRewrite(
      "create (n) return *",
      "create (n) return n"
    )

    assertRewrite(
      "match p = shortestPath((a)-[r*]->(x)) return *",
      "match p = shortestPath((a)-[r*]->(x)) return a,p,r,x"
    )

    assertRewrite(
      "match p=(a:Start)-->(b) return *",
      "match p=(a:Start)-->(b) return a, b, p"
    )
  }

  test("rewrites * in with") {
    assertRewrite(
      "match (n) with * return n",
      "match (n) with n return n"
    )

    assertRewrite(
      "match (n),(c) with * return n",
      "match (n),(c) with c,n return n"
    )

    assertRewrite(
      "match (n)-->(c) with * return n",
      "match (n)-->(c) with c,n return n"
    )

    assertRewrite(
      "match (n)-[r]->(c) with * return n",
      "match (n)-[r]->(c) with c,n,r return n"
    )

    assertRewrite(
      "match (n)-[r]->(c) with *, r.pi as x return n",
      "match (n)-[r]->(c) with c, n, r, r.pi as x return n"
    )

    assertRewrite(
      "create (n) with * return n",
      "create (n) with n return n"
    )

    assertRewrite(
      "match p = shortestPath((a)-[r*]->(x)) with * return p",
      "match p = shortestPath((a)-[r*]->(x)) with a,p,r,x return p"
    )
  }

  test("symbol shadowing should be taken into account") {
    assertRewrite(
      "match (a),(x),(y) with a match (b) return *",
      "match (a),(x),(y) with a match (b) return a, b"
    )
  }

  test("keeps listed items during expand") {
    assertRewrite(
      "MATCH (n) WITH *, 1 AS b RETURN *",
      "MATCH (n) WITH n, 1 AS b RETURN b, n"
    )
  }

  test("should rewrite show commands properly") {
    assertRewrite(
      "SHOW FUNCTIONS YIELD *",
      """SHOW FUNCTIONS
        |YIELD name, category, description, signature, isBuiltIn, argumentDescription, returnDescription, aggregating, rolesExecution, rolesBoostedExecution
        |RETURN name, category, description, signature, isBuiltIn, argumentDescription, returnDescription, aggregating, rolesExecution, rolesBoostedExecution""".stripMargin,
      rewriteShowCommand = true,
      showCommandReturnAddedInRewrite = true
    )

    assertRewrite(
      "SHOW FUNCTIONS YIELD * RETURN *",
      """SHOW FUNCTIONS
        |YIELD name, category, description, signature, isBuiltIn, argumentDescription, returnDescription, aggregating, rolesExecution, rolesBoostedExecution
        |RETURN name, category, description, signature, isBuiltIn, argumentDescription, returnDescription, aggregating, rolesExecution, rolesBoostedExecution""".stripMargin,
      rewriteShowCommand = true
    )

    assertRewrite(
      "SHOW TRANSACTIONS YIELD * RETURN *",
      """SHOW TRANSACTIONS
        |YIELD database, transactionId, currentQueryId, outerTransactionId, connectionId, clientAddress, username, metaData, currentQuery, parameters, planner, runtime,
        |indexes, startTime, currentQueryStartTime, protocol, requestUri, status, currentQueryStatus, statusDetails, resourceInformation, activeLockCount, currentQueryActiveLockCount,
        |elapsedTime, cpuTime, waitTime, idleTime, currentQueryElapsedTime, currentQueryCpuTime, currentQueryWaitTime, currentQueryIdleTime, currentQueryAllocatedBytes, allocatedDirectBytes,
        |estimatedUsedHeapMemory, pageHits, pageFaults, currentQueryPageHits, currentQueryPageFaults, initializationStackTrace
        |RETURN database, transactionId, currentQueryId, outerTransactionId, connectionId, clientAddress, username, metaData, currentQuery, parameters, planner, runtime,
        |indexes, startTime, currentQueryStartTime, protocol, requestUri, status, currentQueryStatus, statusDetails, resourceInformation, activeLockCount, currentQueryActiveLockCount,
        |elapsedTime, cpuTime, waitTime, idleTime, currentQueryElapsedTime, currentQueryCpuTime, currentQueryWaitTime, currentQueryIdleTime, currentQueryAllocatedBytes, allocatedDirectBytes,
        |estimatedUsedHeapMemory, pageHits, pageFaults, currentQueryPageHits, currentQueryPageFaults, initializationStackTrace""".stripMargin,
      rewriteShowCommand = true,
      moveYieldToWith = true
    )

    assertRewrite(
      "TERMINATE TRANSACTIONS 'db-transaction-123' YIELD *",
      """TERMINATE TRANSACTIONS 'db-transaction-123'
        |YIELD transactionId, username, message
        |RETURN transactionId, username, message""".stripMargin,
      rewriteShowCommand = true,
      showCommandReturnAddedInRewrite = true,
      moveYieldToWith = true
    )

    assertRewrite(
      "SHOW USERS YIELD *",
      """SHOW USERS
        |YIELD user, roles, passwordChangeRequired, suspended, home""".stripMargin,
      rewriteShowCommand = true
    )

    assertRewrite(
      "SHOW USERS YIELD * RETURN *",
      """SHOW USERS
        |YIELD user, roles, passwordChangeRequired, suspended, home
        |RETURN user, roles, passwordChangeRequired, suspended, home""".stripMargin,
      rewriteShowCommand = true
    )
  }

  test("uses the position of the clause for variables in new return items") {
    // This is quite important. If the position of a variable in new return item is a previous declaration,
    // that can destroy scoping. In a query like
    // MATCH (owner)
    // WITH HEAD(COLLECT(42)) AS sortValue, owner
    // RETURN *
    // owner would not be scoped/namespaced correctly after having done `isolateAggregation`.
    val wizz = "WITH 1 AS foo "
    val expressionPos = InputPosition(wizz.length, 1, wizz.length + 1)

    val original = prepRewrite(s"${wizz}RETURN *")
    val checkResult = original.semanticCheck(SemanticState.clean)
    val after = original.rewrite(expandStar(checkResult.state))
    val returnItem = after.asInstanceOf[Query].part.asInstanceOf[SingleQuery]
      .clauses.last.asInstanceOf[Return].returnItems.items.head.asInstanceOf[AliasedReturnItem]
    returnItem.expression.position should equal(expressionPos)
    returnItem.variable.position.offset should equal(expressionPos.offset)
  }

  private def assertRewrite(
    originalQuery: String,
    expectedQuery: String,
    rewriteShowCommand: Boolean = false,
    showCommandReturnAddedInRewrite: Boolean = false,
    moveYieldToWith: Boolean = false
  ): Unit = {
    val original = prepRewrite(originalQuery, rewriteShowCommand)
    val expected = prepRewrite(expectedQuery)
    val expectedUpdatedReturn =
      if (showCommandReturnAddedInRewrite) {
        updateClauses(
          expected,
          clauses => {
            // update `addedInRewrite` flag on the return
            val ret = clauses.last.asInstanceOf[Return]
            val newRet = ret.copy(addedInRewrite = true)(ret.position)
            clauses.dropRight(1) :+ newRet
          }
        )
      } else expected
    val expectedUpdatedYield =
      if (moveYieldToWith) {
        updateClauses(
          expectedUpdatedReturn,
          clauses => {
            // transaction commands parses YIELD as WITH *
            clauses.map {
              case s: ShowTransactionsClause =>
                s.copy(yieldAll = true, yieldItems = List.empty)(s.position)

              case t: TerminateTransactionsClause =>
                t.copy(yieldAll = true, yieldItems = List.empty)(t.position)

              case w: With =>
                val returnItems = w.returnItems.defaultOrderOnColumns.map(c =>
                  c.map(v => aliasedReturnItem(varFor(v)))
                ).getOrElse(List.empty)
                w.copy(returnItems =
                  w.returnItems.copy(
                    includeExisting = false,
                    items = returnItems,
                    defaultOrderOnColumns = None
                  )(w.returnItems.position)
                )(w.position)

              case c => c
            }
          }
        )
      } else expectedUpdatedReturn

    val checkResult = original.semanticCheck(SemanticState.clean)
    val rewriter = expandStar(checkResult.state)

    val result = original.rewrite(rewriter)
    assert(result === expectedUpdatedYield)
  }

  private def prepRewrite(q: String, rewriteShowCommand: Boolean = false) = {
    val exceptionFactory = OpenCypherExceptionFactory(None)
    val rewriter =
      if (rewriteShowCommand)
        inSequence(
          normalizeWithAndReturnClauses(exceptionFactory, devNullLogger),
          rewriteShowQuery.instance,
          expandShowWhere.instance
        )
      else
        inSequence(normalizeWithAndReturnClauses(exceptionFactory, devNullLogger))
    JavaCCParser.parse(q, exceptionFactory).endoRewrite(rewriter)
  }

  private def updateClauses(statement: Statement, updateClauses: Seq[Clause] => Seq[Clause]): Statement = {
    val query = statement.asInstanceOf[Query]
    val singleQuery = query.part.asInstanceOf[SingleQuery]
    val clauses = singleQuery.clauses
    val newClauses = updateClauses(clauses)
    val newSingleQuery = singleQuery.copy(newClauses)(singleQuery.position)
    query.copy(newSingleQuery)(query.position)
  }
}
