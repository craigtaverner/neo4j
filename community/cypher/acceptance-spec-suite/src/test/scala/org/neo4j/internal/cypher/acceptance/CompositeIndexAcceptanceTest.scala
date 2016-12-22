package org.neo4j.internal.cypher.acceptance

import java.lang.Iterable

import org.neo4j.cypher.internal.compiler.v3_2.executionplan.InternalExecutionResult
import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport}
import org.neo4j.graphdb.schema.IndexDefinition
import org.neo4j.graphdb.{GraphDatabaseService, Node}
import org.neo4j.kernel.GraphDatabaseQueryService
import org.scalatest.matchers.{MatchResult, Matcher}
import scala.collection.JavaConverters._

class CompositeIndexAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("should succeed in creating composite index") {
    // When
    executeWithCostPlannerOnly("CREATE INDEX ON :Person(firstname)")
    executeWithCostPlannerOnly("CREATE INDEX ON :Person(firstname,lastname)")
    executeWithCostPlannerOnly("CREATE INDEX ON :Person(firstname, lastname)")
    executeWithCostPlannerOnly("CREATE INDEX ON :Person(firstname , lastname)")

    // Then
    graph should haveIndexes(":Person(firstname)", ":Person(firstname,lastname)")

    // When
    executeWithCostPlannerOnly("DROP INDEX ON :Person(firstname , lastname)")

    // Then
    graph should haveIndexes(":Person(firstname)")
    graph should not(haveIndexes(":Person(firstname,lastname)"))
  }

  test("should be able to update composite index when only one property has changed") {
    executeWithCostPlannerOnly("CREATE INDEX ON :Person(firstname, lastname)")
    val n = executeWithCostPlannerOnly("CREATE (n:Person {firstname:'Joe', lastname:'Soap'}) RETURN n").columnAs("n").toList(0)
    executeWithCostPlannerOnly("MATCH (n:Person) SET n.lastname = 'Bloggs'")
    val result = executeWithCostPlannerOnly("MATCH (n:Person) where n.firstname = 'Joe' and n.lastname = 'Bloggs' RETURN n")
    result should use("NodeIndexSeek")
    result.columnAs("n").toList should be(List(n))
  }

  case class haveIndexes(expectedIndexes: String*) extends Matcher[GraphDatabaseQueryService] {
    def apply(graph: GraphDatabaseQueryService): MatchResult = {

      graph.inTx {
        val indexNames = graph.schema().getIndexes.asScala.toList.map(i => s":${i.getLabel}(${i.getPropertyKeys.asScala.toList.mkString(",")})")
        println("Found indexes" + indexNames.mkString(", "))

        val result = expectedIndexes.forall(i => indexNames.contains(i.toString))

        MatchResult(
          result,
          s"Expected graph to have indexes ${expectedIndexes.mkString(", ")}, but it was ${indexNames.mkString(", ")}",
          s"Expected graph to not have indexes ${expectedIndexes.mkString(", ")}, but it did."
        )
      }
    }
  }

}
