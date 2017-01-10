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

import org.neo4j.kernel.api.index.IndexDescriptor;

import static org.neo4j.kernel.impl.util.IdPrettyPrinter.label;
import static org.neo4j.kernel.impl.util.IdPrettyPrinter.propertyKey;

abstract class IndexKey implements CountsKey
{
    private final CountsKeyType type;
    private final IndexDescriptor descriptor;

    IndexKey( IndexDescriptor descriptor, CountsKeyType type )
    {
        this.type = type;
        this.descriptor = descriptor;
    }

    public IndexDescriptor descriptor()
    {
        return descriptor;
    }

    @Override
    public String toString()
    {
        String propertyText = descriptor.descriptor().propertyIdText();
        return String.format( "IndexKey[%s (%s {%s})]", type.name(), label( descriptor.getLabelId() ), propertyText );
    }

    @Override
    public CountsKeyType recordType()
    {
        return type;
    }

    @Override
    public int hashCode()
    {
        return descriptor.hashCode();
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
        return indexKey.equals( descriptor );
    }

    @Override
    public int compareTo( CountsKey other )
    {
        if ( other instanceof IndexKey )
        {
            IndexKey that = (IndexKey) other;
            return this.descriptor.descriptor().compareTo( that.descriptor.descriptor() );
        }
        return recordType().ordinal() - other.recordType().ordinal();
    }
}
