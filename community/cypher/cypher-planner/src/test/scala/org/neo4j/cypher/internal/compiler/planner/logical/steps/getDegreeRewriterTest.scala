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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery.CreateIrExpressions
import org.neo4j.cypher.internal.compiler.planner.logical.steps.getDegreeRewriterTest.relPattern
import org.neo4j.cypher.internal.expressions.CountExpression
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.EveryPath
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.GetDegree
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.HasDegree
import org.neo4j.cypher.internal.expressions.HasDegreeGreaterThan
import org.neo4j.cypher.internal.expressions.HasDegreeGreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.HasDegreeLessThan
import org.neo4j.cypher.internal.expressions.HasDegreeLessThanOrEqual
import org.neo4j.cypher.internal.expressions.LabelExpression.disjoinRelTypesToLabelExpression
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.functions.Exists
import org.neo4j.cypher.internal.expressions.functions.Size
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class getDegreeRewriterTest extends CypherFunSuite with AstConstructionTestSupport {

  // All pattern elements have been named at this point, so the outer scope of PatternExpression is used to signal which expressions come from the outside.
  // In the test names, empty names denote anonymous notes, i.e. ones that do not come from the outside.

  private def createIrExpressions = CreateIrExpressions(new AnonymousVariableNameGenerator())

  test("Rewrite exists( (a)-[:FOO]->() ) to GetDegree( (a)-[:FOO]->() ) > 0") {
    val incoming =
      createIrExpressions(Exists(patternExpression(from = Some("a"), relationships = Seq("FOO")))(pos))
    val expected = HasDegreeGreaterThan(varFor("a"), Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(0))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test("Rewrite exists( ()-[:FOO]->(a) ) to GetDegree( (a)<-[:FOO]-() ) > 0") {
    val incoming =
      createIrExpressions(Exists(patternExpression(to = Some("a"), relationships = Seq("FOO")))(pos))
    val expected = HasDegreeGreaterThan(varFor("a"), Some(RelTypeName("FOO")(pos)), INCOMING, literalInt(0))(pos)
    getDegreeRewriter(incoming) should equal(expected)
  }

  test(
    "Rewrite exists( (a)-[:FOO|:BAR]->() ) to HasDegreeGreaterThan( (a)-[:FOO]->(), 0) OR HasDegreeGreaterThan( (a)-[:BAR]->(), 0)"
  ) {
    val incoming = createIrExpressions(Exists.asInvocation(
      patternExpression(from = Some("a"), relationships = Seq("FOO", "BAR"))
    )(pos))
    val expected =
      ors(
        HasDegreeGreaterThan(varFor("a"), Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(0))(pos),
        HasDegreeGreaterThan(varFor("a"), Some(RelTypeName("BAR")(pos)), OUTGOING, literalInt(0))(pos)
      )

    getDegreeRewriter(incoming) should equal(expected)
  }

  test("Does not rewrite exists((a {prop: 5})-[:FOO]->()) ") {
    val incoming = createIrExpressions(Exists(patternExpression(
      from = Some("a"),
      to = Some("b"),
      relationships = Seq("FOO"),
      fromPropertyPredicate = Some(mapOf("prop" -> literalInt(5)))
    ))(pos))

    getDegreeRewriter(incoming) should equal(incoming)
  }

  test("Does not rewrite exists( (a)-[:FOO]->(b) )") {
    val incoming =
      createIrExpressions(Exists(patternExpression(from = Some("a"), to = Some("b"), relationships = Seq("FOO")))(
        pos
      ))

    getDegreeRewriter(incoming) should equal(incoming)
  }

  private def patternExpression(
    from: Option[String] = None,
    to: Option[String] = None,
    relationships: Seq[String] = Seq.empty,
    fromPropertyPredicate: Option[Expression] = None
  ) = {
    PatternExpression(
      pattern = relPattern(from, to, relationships, fromPropertyPredicate)
    )(
      outerScope = (from.toSet ++ to.toSet).map(varFor(_))
    )
  }
}

object getDegreeRewriterTest extends AstConstructionTestSupport {

  def relPattern(
    from: Option[String] = None,
    to: Option[String] = None,
    relationships: Seq[String] = Seq.empty,
    fromPropertyPredicate: Option[Expression] = None
  ): RelationshipsPattern = {
    RelationshipsPattern(RelationshipChain(
      NodePattern(Some(from.map(varFor(_)).getOrElse(varFor("DEFAULT"))), None, fromPropertyPredicate, None)(pos),
      RelationshipPattern(
        Some(varFor("r")),
        disjoinRelTypesToLabelExpression(relationships.map(r => RelTypeName(r)(pos))),
        None,
        None,
        None,
        SemanticDirection.OUTGOING
      )(pos),
      NodePattern(Some(to.map(varFor(_)).getOrElse(varFor("DEFAULT"))), None, None, None)(pos)
    )(pos))(pos)
  }
}

class GetDegreeRewriterSizeOfPatternComprehensionTest extends GetDegreeRewriterCountLikeTestBase {

  override def makeInputExpression(
    from: Option[String],
    to: Option[String],
    relationships: Seq[String],
    predicate: Option[Expression]
  ): Expression = {
    Size(
      PatternComprehension(
        namedPath = None,
        pattern = relPattern(from, to, relationships),
        predicate = predicate,
        projection = literalInt(1)
      )(
        position = pos,
        outerScope = (from.toSet ++ to.toSet).map(varFor(_))
      )
    )(pos)
  }

  override protected def testNameExpr(pattern: String): String = s"size([ $pattern | 1])"
}

class GetDegreeRewriterCountExpressionTest extends GetDegreeRewriterCountLikeTestBase {

  override def makeInputExpression(
    from: Option[String],
    to: Option[String],
    relationships: Seq[String],
    predicate: Option[Expression]
  ): Expression = {
    CountExpression(
      pattern = Pattern(Seq(EveryPath(relPattern(from, to, relationships).element)))(pos),
      optionalWhereExpression = predicate
    )(
      position = pos,
      outerScope = (from.toSet ++ to.toSet).map(varFor(_))
    )
  }

  override protected def testNameExpr(pattern: String): String = s"COUNT { $pattern }"
}

trait GetDegreeRewriterCountLikeTestBase extends CypherFunSuite with AstConstructionTestSupport {

  protected def makeInputExpression(
    from: Option[String] = None,
    to: Option[String] = None,
    relationships: Seq[String] = Seq.empty,
    predicate: Option[Expression] = None
  ): Expression

  protected def testNameExpr(pattern: String): String

  private def createIrExpressions = CreateIrExpressions(new AnonymousVariableNameGenerator())

  test(s"Rewrite ${testNameExpr("(a)-[:FOO]->()")} to GetDegree( (a)-[:FOO]->() )") {
    val incoming =
      createIrExpressions(makeInputExpression(from = Some("a"), relationships = Seq("FOO")))
    val expected = GetDegree(varFor("a"), Some(RelTypeName("FOO")(pos)), SemanticDirection.OUTGOING)(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite ${testNameExpr("()-[:FOO]->(a)")} to GetDegree( (a)<-[:FOO]-() )") {
    val incoming =
      createIrExpressions(makeInputExpression(to = Some("a"), relationships = Seq("FOO")))
    val expected = GetDegree(varFor("a"), Some(RelTypeName("FOO")(pos)), SemanticDirection.INCOMING)(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite ${testNameExpr("(a)-[:FOO|:BAR]->()")} to GetDegree( (a)-[:FOO]->() ) + GetDegree( (a)-[:BAR]->() )") {
    val incoming =
      createIrExpressions(makeInputExpression(from = Some("a"), relationships = Seq("FOO", "BAR")))
    val expected =
      add(
        GetDegree(varFor("a"), Some(RelTypeName("FOO")(pos)), OUTGOING)(pos),
        GetDegree(varFor("a"), Some(RelTypeName("BAR")(pos)), OUTGOING)(pos)
      )

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Does not rewrite ${testNameExpr("[ (a)-[:FOO]->(b)")}") {
    val incoming =
      createIrExpressions(makeInputExpression(from = Some("a"), to = Some("b"), relationships = Seq("FOO")))

    getDegreeRewriter(incoming) should equal(incoming)
  }

  test(s"Does not rewrite ${testNameExpr("(a)-[:FOO]->() WHERE a.prop")}") {
    val incoming = createIrExpressions(Size(makeInputExpression(
      from = Some("a"),
      to = Some("b"),
      relationships = Seq("FOO"),
      predicate = Some(prop("a", "prop"))
    ))(pos))

    getDegreeRewriter(incoming) should equal(incoming)
  }

  test(s"Rewrite ${testNameExpr("(a)-[:FOO]->()")} > 5 to HasDegreeGreaterThan( (a)-[:FOO]->() , 5)") {
    val incoming =
      createIrExpressions(GreaterThan(
        makeInputExpression(from = Some("a"), relationships = Seq("FOO")),
        literalInt(5)
      )(pos))
    val expected = HasDegreeGreaterThan(varFor("a"), Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite ${testNameExpr("()-[:FOO]->(a)")} > 5 to HasDegreeGreaterThan( (a)<-[:FOO]-() , 5)") {
    val incoming =
      createIrExpressions(GreaterThan(
        makeInputExpression(to = Some("a"), relationships = Seq("FOO")),
        literalInt(5)
      )(pos))
    val expected = HasDegreeGreaterThan(varFor("a"), Some(RelTypeName("FOO")(pos)), INCOMING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite 5 > ${testNameExpr("} (a)-[:FOO]->()")} to HasDegreeLessThan( (a)-[:FOO]->() , 5)") {
    val incoming =
      createIrExpressions(GreaterThan(
        literalInt(5),
        makeInputExpression(from = Some("a"), relationships = Seq("FOO"))
      )(pos))
    val expected = HasDegreeLessThan(varFor("a"), Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite 5 > ${testNameExpr("()-[:FOO]->(a)")} to HasDegreeLessThan( (a)<-[:FOO]-() , 5)") {
    val incoming =
      createIrExpressions(GreaterThan(
        literalInt(5),
        makeInputExpression(to = Some("a"), relationships = Seq("FOO"))
      )(pos))
    val expected = HasDegreeLessThan(varFor("a"), Some(RelTypeName("FOO")(pos)), INCOMING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite ${testNameExpr("(a)-[:FOO]->()")} >= 5 to HasDegreeGreaterThanOrEqual( (a)-[:FOO]->() , 5)") {
    val incoming = createIrExpressions(GreaterThanOrEqual(
      makeInputExpression(from = Some("a"), relationships = Seq("FOO")),
      literalInt(5)
    )(pos))
    val expected = HasDegreeGreaterThanOrEqual(varFor("a"), Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite ${testNameExpr("()-[:FOO]->(a)")} >= 5 to HasDegreeGreaterThanOrEqual( (a)<-[:FOO]-() , 5)") {
    val incoming = createIrExpressions(GreaterThanOrEqual(
      makeInputExpression(to = Some("a"), relationships = Seq("FOO")),
      literalInt(5)
    )(pos))
    val expected = HasDegreeGreaterThanOrEqual(varFor("a"), Some(RelTypeName("FOO")(pos)), INCOMING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite 5 >= ${testNameExpr("(a)-[:FOO]->()")} to HasDegreeLessThanOrEqual( (a)-[:FOO]->() , 5)") {
    val incoming = createIrExpressions(GreaterThanOrEqual(
      literalInt(5),
      makeInputExpression(from = Some("a"), relationships = Seq("FOO"))
    )(pos))
    val expected = HasDegreeLessThanOrEqual(varFor("a"), Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite 5 >= ${testNameExpr("()-[:FOO]->(a)")} to HasDegreeLessThanOrEqual( (a)<-[:FOO]-() , 5)") {
    val incoming = createIrExpressions(GreaterThanOrEqual(
      literalInt(5),
      makeInputExpression(to = Some("a"), relationships = Seq("FOO"))
    )(pos))
    val expected = HasDegreeLessThanOrEqual(varFor("a"), Some(RelTypeName("FOO")(pos)), INCOMING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite ${testNameExpr("(a)-[:FOO]->()")} = 5 to HasDegree( (a)-[:FOO]->() , 5)") {
    val incoming =
      createIrExpressions(Equals(
        makeInputExpression(from = Some("a"), relationships = Seq("FOO")),
        literalInt(5)
      )(pos))
    val expected = HasDegree(varFor("a"), Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite ${testNameExpr("()-[:FOO]->(a)")} = 5 to HasDegree( (a)<-[:FOO]-() , 5)") {
    val incoming =
      createIrExpressions(Equals(
        makeInputExpression(to = Some("a"), relationships = Seq("FOO")),
        literalInt(5)
      )(pos))
    val expected = HasDegree(varFor("a"), Some(RelTypeName("FOO")(pos)), INCOMING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite 5 = ${testNameExpr("(a)-[:FOO]->()")} to HasDegree( (a)-[:FOO]->() , 5)") {
    val incoming =
      createIrExpressions(Equals(
        literalInt(5),
        makeInputExpression(from = Some("a"), relationships = Seq("FOO"))
      )(pos))
    val expected = HasDegree(varFor("a"), Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite 5 = ${testNameExpr("()-[:FOO]->(a)")} to HasDegree( (a)<-[:FOO]-() , 5)") {
    val incoming =
      createIrExpressions(Equals(
        literalInt(5),
        makeInputExpression(to = Some("a"), relationships = Seq("FOO"))
      )(pos))
    val expected = HasDegree(varFor("a"), Some(RelTypeName("FOO")(pos)), INCOMING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite ${testNameExpr("(a)-[:FOO]->()")} <= 5 to HasDegreeLessThanOrEqual( (a)-[:FOO]->() , 5)") {
    val incoming = createIrExpressions(LessThanOrEqual(
      makeInputExpression(from = Some("a"), relationships = Seq("FOO")),
      literalInt(5)
    )(pos))
    val expected = HasDegreeLessThanOrEqual(varFor("a"), Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite ${testNameExpr("()-[:FOO]->(a)")} <= 5 to HasDegreeLessThanOrEqual( (a)<-[:FOO]-() , 5)") {
    val incoming =
      createIrExpressions(LessThanOrEqual(
        makeInputExpression(to = Some("a"), relationships = Seq("FOO")),
        literalInt(5)
      )(pos))
    val expected = HasDegreeLessThanOrEqual(varFor("a"), Some(RelTypeName("FOO")(pos)), INCOMING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite 5 <= ${testNameExpr("(a)-[:FOO]->()")} to HasDegreeGreaterThanOrEqual( (a)-[:FOO]->() , 5)") {
    val incoming = createIrExpressions(LessThanOrEqual(
      literalInt(5),
      makeInputExpression(from = Some("a"), relationships = Seq("FOO"))
    )(pos))
    val expected = HasDegreeGreaterThanOrEqual(varFor("a"), Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite 5 <= ${testNameExpr("()-[:FOO]->(a)")} to HasDegreeGreaterThanOrEqual( (a)<-[:FOO]-() , 0)") {
    val incoming =
      createIrExpressions(LessThanOrEqual(
        literalInt(5),
        makeInputExpression(to = Some("a"), relationships = Seq("FOO"))
      )(pos))
    val expected = HasDegreeGreaterThanOrEqual(varFor("a"), Some(RelTypeName("FOO")(pos)), INCOMING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite ${testNameExpr("(a)-[:FOO]->()")} < 5 to HasDegreeLessThan( (a)-[:FOO]->() , 0)") {
    val incoming =
      createIrExpressions(LessThan(
        makeInputExpression(from = Some("a"), relationships = Seq("FOO")),
        literalInt(5)
      )(
        pos
      ))
    val expected = HasDegreeLessThan(varFor("a"), Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite ${testNameExpr("()-[:FOO]->(a)")} < 5 to HasDegreeLessThan( (a)<-[:FOO]-() , 0)") {
    val incoming =
      createIrExpressions(LessThan(
        makeInputExpression(to = Some("a"), relationships = Seq("FOO")),
        literalInt(5)
      )(pos))
    val expected = HasDegreeLessThan(varFor("a"), Some(RelTypeName("FOO")(pos)), INCOMING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite 5 < ${testNameExpr("(a)-[:FOO]->()")} to HasDegreeGreaterThan( (a)-[:FOO]->() , 5)") {
    val incoming =
      createIrExpressions(LessThan(
        literalInt(5),
        makeInputExpression(from = Some("a"), relationships = Seq("FOO"))
      )(
        pos
      ))
    val expected = HasDegreeGreaterThan(varFor("a"), Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test(s"Rewrite 5 < ${testNameExpr("()-[:FOO]->(a)")} to HasDegreeGreaterThan( (a)<-[:FOO]-() , 5)") {
    val incoming =
      createIrExpressions(LessThan(
        literalInt(5),
        makeInputExpression(to = Some("a"), relationships = Seq("FOO"))
      )(pos))
    val expected = HasDegreeGreaterThan(varFor("a"), Some(RelTypeName("FOO")(pos)), INCOMING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }
}
