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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.frontend.helpers.InputDataStreamTestInitialState
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.Input
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.util.Random

class InputDataStreamPlanningTest extends CypherFunSuite with LogicalPlanningTestSupport2
    with AstConstructionTestSupport {

  test("INPUT DATA STREAM a, b, c RETURN *") {
    val ast = query(input(varFor("a"), varFor("b"), varFor("c")), returnAll)
    new given().getLogicalPlanForAst(createInitStateFromAst(ast))._1 should equal(Input(Seq("a", "b", "c")))
  }

  test("INPUT DATA STREAM a, b, c RETURN DISTINCT a") {
    val ast = query(input(varFor("a"), varFor("b"), varFor("c")), returnDistinct(returnItem(varFor("a"), "a")))
    new given().getLogicalPlanForAst(createInitStateFromAst(ast))._1 should equal(Distinct(
      Input(Seq("a", "b", "c")),
      Map("a" -> varFor("a"))
    ))
  }

  test("INPUT DATA STREAM a, b, c RETURN sum(a)") {
    val ast = query(input(varFor("a"), varFor("b"), varFor("c")), return_(returnItem(sum(varFor("a")), "sum(a)")))
    new given().getLogicalPlanForAst(createInitStateFromAst(ast))._1 should equal(
      Aggregation(Input(Seq("a", "b", "c")), Map.empty, Map("sum(a)" -> sum(varFor("a"))))
    )
  }

  test("INPUT DATA STREAM a, b, c WITH * WHERE a.pid = 99 RETURN *") {
    val ast =
      query(input(varFor("a"), varFor("b"), varFor("c")), withAll(Some(where(propEquality("a", "pid", 99)))), returnAll)

    new given().getLogicalPlanForAst(createInitStateFromAst(ast))._1 should equal(
      Selection(ands(propEquality("a", "pid", 99)), Input(Seq("a", "b", "c")))
    )
  }

  test("INPUT DATA STREAM a, b, c WITH * WHERE a:Employee RETURN a.name AS name ORDER BY name") {
    val ast = query(
      input(varFor("a"), varFor("b"), varFor("c")),
      withAll(Some(where(hasLabelsOrTypes("a", "Employee")))),
      return_(orderBy(sortItem(varFor("name"))), returnItem(prop("a", "name"), "name"))
    )

    new given().getLogicalPlanForAst(createInitStateFromAst(ast))._1 should equal(
      Sort(
        Projection(
          Selection(ands(hasLabelsOrTypes("a", "Employee")), Input(Seq("a", "b", "c"))),
          Map("name" -> prop("a", "name"))
        ),
        List(Ascending("name"))
      )
    )
  }

  test("INPUT DATA STREAM a, b, c WITH * MATCH (x) RETURN *") {
    val ast = query(input(varFor("a"), varFor("b"), varFor("c")), withAll(), match_(nodePat(Some("x"))), returnAll)

    new given().getLogicalPlanForAst(createInitStateFromAst(ast))._1 should equal(
      Apply(
        Input(Seq("a", "b", "c")),
        AllNodesScan("x", Set("a", "b", "c"))
      )
    )
  }

  test("INPUT DATA STREAM g, uid, cids, cid, p RETURN *") {
    val ast = query(input(varFor("g"), varFor("uid"), varFor("cids"), varFor("cid"), varFor("p")), returnAll)
    new given().getLogicalPlanForAst(createInitStateFromAst(ast))._1 should equal(
      Input(Seq("g", "uid", "cids", "cid", "p"))
    )
  }

  test("INPUT DATA STREAM with large number of columns") {
    val randomColumns = Random.shuffle(for (c <- 'a' to 'z'; n <- 1 to 10) yield s"$c$n")
    val ast = query(input(randomColumns.map(col => varFor(col)): _*), returnAll)
    new given().getLogicalPlanForAst(createInitStateFromAst(ast))._1 should equal(
      Input(randomColumns)
    )
  }

  private def createInitStateFromAst(ast: Statement): BaseState = {
    // As the test only checks ast -> planning, any valid query could be used.
    val fakeQueryString = "RETURN 1"
    InputDataStreamTestInitialState(
      fakeQueryString,
      None,
      IDPPlannerName,
      new AnonymousVariableNameGenerator(),
      maybeStatement = Some(ast)
    )
  }

}
