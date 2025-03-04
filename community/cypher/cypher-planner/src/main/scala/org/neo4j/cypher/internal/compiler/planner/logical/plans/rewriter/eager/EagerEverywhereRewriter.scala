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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager

import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.logical.plans.ApplyPlan
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.LogicalBinaryPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.UpdatingPlan
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.bottomUp

import scala.collection.immutable.ListSet

/**
 * Insert Eager between every read and write plan, and between every write and write plan.
 * Eagerizes Apply plans additionally by inserting Eager on the LHS and above if the RHS has updates.
 */
case class EagerEverywhereRewriter(attributes: Attributes[LogicalPlan]) extends EagerRewriter(attributes) {

  /**
   * Eagerize a plan with reason UpdateStrategyEager
   */
  private def eager(p: LogicalPlan): Eager = eagerOnTopOf(p, ListSet(EagernessReason.UpdateStrategyEager))

  /**
   * Eagerize a binary plan by inserting Eager on the LHS or RHS if the respective LHS or RHS children are updating plans.
   */
  private def eagerizeBinaryPlan(lp: LogicalBinaryPlan): LogicalBinaryPlan = {
    var res = lp
    lp.left match {
      case up: UpdatingPlan =>
        res = res.withLhs(eager(up))(SameId(lp.id))
      case _ =>
    }
    lp.right match {
      case up: UpdatingPlan =>
        res = res.withRhs(eager(up))(SameId(lp.id))
      case _ =>
    }
    res
  }

  /**
   * Eagerize an ApplyPlan like other binary plans, but in addition insert Eager on the LHS and above if the RHS has updates anywhere.
   */
  private def eagerizeApplyPlan(ap: ApplyPlan): LogicalPlan = {
    if (!ap.right.readOnly) {
      val eagerBinaryPlan = eagerizeBinaryPlan(ap)
      eager(eagerBinaryPlan.withLhs(eager(eagerBinaryPlan.left))(SameId(eagerBinaryPlan.id)))
    } else {
      eagerizeBinaryPlan(ap)
    }
  }

  override def eagerize(plan: LogicalPlan): LogicalPlan = {
    plan.endoRewrite(bottomUp(Rewriter.lift {
      case up: UpdatingPlan =>
        eager(up.withLhs(eager(up.source))(SameId(up.id)))

      case ap: ApplyPlan =>
        eagerizeApplyPlan(ap)

      case lp: LogicalBinaryPlan =>
        eagerizeBinaryPlan(lp)
    }))
  }
}
