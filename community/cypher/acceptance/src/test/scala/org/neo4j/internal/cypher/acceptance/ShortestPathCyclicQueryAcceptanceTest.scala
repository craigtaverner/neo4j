/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.internal.compatibility.ExecutionResultWrapperFor3_0
import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport}
import org.neo4j.graphdb.Node

class ShortestPathCyclicQueryAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("shortestPath plans with varlength expand only when passed same start and end node") {
    setupModel()
    val result = executeUsingCostPlannerOnly("MATCH p = shortestPath((a:T {name:'a'})-[:isa*1..]-(a)) return length(p) as len")
    val results = result.columnAs("len").toList
    println(results)
    println(result.executionPlanDescription())
    val ids = results(0).asInstanceOf[Seq[_]].map(n => n.asInstanceOf[Node].getId)
    ids should be(List(3, 3))
    result should use("VarLengthExpand")
    result should not(use("ShortestPath"))
  }

  def executeUsingCostPlannerOnly(query: String) =
    eengine.execute(s"CYPHER planner=IDP $query", Map.empty[String, Any], graph.session()) match {
      case e: ExecutionResultWrapperFor3_0 => RewindableExecutionResult(e)
    }

  private def setupModel(): Unit = {
    executeUsingCostPlannerOnly("create (a:T {name:'a'})-[:isa]->(:T {name:'b'})-[:isa]->(:T {name:'c'})-[:isa]->(a)")
  }
}
