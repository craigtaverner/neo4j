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
package org.neo4j.kernel.impl.store.record;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.NodePropertyExistenceConstraint;
import org.neo4j.kernel.api.index.IndexDescriptor;

public class NodePropertyExistenceConstraintRule extends NodePropertyConstraintRule
{

    public static NodePropertyExistenceConstraintRule nodePropertyExistenceConstraintRule( long id, int labelId,
            int[] propertyKeyIds )
    {
        return new NodePropertyExistenceConstraintRule( id, labelId, propertyKeyIds );
    }

    public static NodePropertyExistenceConstraintRule readNodePropertyExistenceConstraintRule( long id, int labelId,
            ByteBuffer buffer )
    {
        return new NodePropertyExistenceConstraintRule( id, labelId, new int[]{buffer.getInt()} );
    }

    private NodePropertyExistenceConstraintRule( long id, int labelId, int[] propertyKeyIds )
    {
        super( id, labelId, propertyKeyIds, Kind.NODE_PROPERTY_EXISTENCE_CONSTRAINT );
    }

    @Override
    public String toString()
    {
        return "NodePropertyExistenceConstraintRule[id=" + id + ", label=" + label + ", kind=" + kind +
               ", propertyKeyIds=" + getPropertyKeys() + "]";
    }

    @Override
    public int length()
    {
        return 4 /* label id */ +
               1 /* kind id */ +
               4; /* property key id */
    }

    @Override
    public void serialize( ByteBuffer target )
    {
        target.putInt( label );
        target.put( kind.id() );
        target.putInt( propertyKeyIds[0] );
    }

    @Override
    public NodePropertyConstraint toConstraint()
    {
        return new NodePropertyExistenceConstraint( getLabel(), getPropertyKeys() );
    }
}
