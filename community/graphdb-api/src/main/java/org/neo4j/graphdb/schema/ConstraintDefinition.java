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
package org.neo4j.graphdb.schema;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

/**
 * Definition of a constraint.
 */
@PublicApi
public interface ConstraintDefinition {
    /**
     * This accessor method returns a label which this constraint is associated with if this constraint has type
     * {@link ConstraintType#UNIQUENESS} or {@link ConstraintType#NODE_PROPERTY_EXISTENCE}.
     * Type of the constraint can be examined by calling {@link #getConstraintType()} or
     * {@link #isConstraintType(ConstraintType)} methods.
     *
     * @return the {@link Label} this constraint is associated with.
     * @throws IllegalStateException when this constraint is associated with relationships.
     */
    Label getLabel();

    /**
     * This accessor method returns a relationship type which this constraint is associated with if this constraint
     * has type {@link ConstraintType#UNIQUENESS} or {@link ConstraintType#NODE_PROPERTY_EXISTENCE}.
     * Type of the constraint can be examined by calling {@link #getConstraintType()} or
     * {@link #isConstraintType(ConstraintType)} methods.
     *
     * @return the {@link RelationshipType} this constraint is associated with.
     * @throws IllegalStateException when this constraint is associated with nodes.
     */
    RelationshipType getRelationshipType();

    /**
     * @return the property keys this constraint is about.
     */
    Iterable<String> getPropertyKeys();

    /**
     * Drops this constraint.
     */
    void drop();

    /**
     * @return the {@link ConstraintType} of constraint.
     */
    ConstraintType getConstraintType();

    /**
     * @param type a constraint type
     * @return true if this constraint definition's type is equal to the provided type
     */
    boolean isConstraintType(ConstraintType type);

    /**
     * Get the name given to this constraint when it was created.
     * Constraints that were not explicitly given a name at creation, will have an auto-generated name.
     * @return the unique name of the constraint.
     */
    String getName();
}
