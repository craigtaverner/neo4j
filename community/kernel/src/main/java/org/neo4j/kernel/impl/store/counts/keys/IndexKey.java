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
package org.neo4j.kernel.impl.store.counts.keys;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.neo4j.kernel.impl.util.IdPrettyPrinter.label;
import static org.neo4j.kernel.impl.util.IdPrettyPrinter.propertyKey;

abstract class IndexKey implements CountsKey
{
    private final int labelId;
    private final int[] propertyKeyIds;
    private final CountsKeyType type;

    IndexKey( int labelId, int[] propertyKeyIds, CountsKeyType type )
    {
        this.labelId = labelId;
        this.propertyKeyIds = propertyKeyIds;
        this.type = type;
    }

    public int labelId()
    {
        return labelId;
    }

    public int[] propertyKeyIds()
    {
        return propertyKeyIds;
    }

    @Override
    public String toString()
    {
        String propertyText = Arrays.stream( propertyKeyIds ).mapToObj( id -> propertyKey( id ) )
                .collect( Collectors.joining( "," ) );
        return String.format( "IndexKey[%s (%s {%s})]", type.name(), label( labelId ), propertyText );
    }

    @Override
    public CountsKeyType recordType()
    {
        return type;
    }

    @Override
    public int hashCode()
    {
        int result = labelId;
        for ( int propertyKeyId : propertyKeyIds )
        {
            result = 31 * result + propertyKeyId;
        }
        result = 31 * result + type.hashCode();
        return result;
    }

    @Override
    public boolean equals( Object other )
    {
        if ( this == other )
        {
            return true;
        }
        if ( other == null || getClass() != other.getClass() )
        {
            return false;
        }

        IndexKey indexKey = (IndexKey) other;
        return labelId == indexKey.labelId && Arrays.equals( propertyKeyIds, indexKey.propertyKeyIds ) &&
               type == indexKey.type;
    }

    @Override
    public int compareTo( CountsKey other )
    {
        if ( other instanceof IndexKey )
        {
            IndexKey that = (IndexKey) other;
            int cmp = this.labelId() - that.labelId();
            if ( cmp == 0 && !Arrays.equals( this.propertyKeyIds(), that.propertyKeyIds() ) )
            {
                cmp = this.propertyKeyIds().length - that.propertyKeyIds().length;
                if ( cmp == 0 )
                {
                    for ( int i = 0; i < this.propertyKeyIds().length; i++ )
                    {
                        cmp = this.propertyKeyIds()[i] - that.propertyKeyIds()[i];
                        if ( cmp != 0 )
                        {
                            return cmp;
                        }
                    }
                }
            }
            return cmp;
        }
        return recordType().ordinal() - other.recordType().ordinal();
    }
}
