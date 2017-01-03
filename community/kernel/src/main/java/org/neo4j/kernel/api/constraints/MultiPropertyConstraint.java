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
package org.neo4j.kernel.api.constraints;

import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.index.DescriptorWithProperties;

/**
 * Base class describing a property constraint on multiple properties.
 */
public abstract class MultiPropertyConstraint implements PropertyConstraint, DescriptorWithProperties
{
    protected final int[] propertyKeyIds;

    public MultiPropertyConstraint( int[] propertyKeyIds )
    {
        this.propertyKeyIds = propertyKeyIds;
    }

    public int[] getPropertyKeyIds()
    {
        return propertyKeyIds;
    }
}
