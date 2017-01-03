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
package org.neo4j.kernel.api.index;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.storageengine.api.schema.SchemaRule;

import static java.lang.String.format;

/**
 * Description of a single index.
 *
 * @see SchemaRule
 */
public class CompositeIndexDescriptor extends IndexDescriptor
{
    private final int[] propertyKeyIds;

    public CompositeIndexDescriptor( int labelId, int[] propertyKeyIds )
    {
        super( labelId, -1 );
        this.propertyKeyIds = propertyKeyIds;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
        {
            return true;
        }
        if ( obj != null && getClass() == obj.getClass() )
        {
            CompositeIndexDescriptor that = (CompositeIndexDescriptor) obj;
            return this.labelId == that.labelId &&
                   Arrays.equals( this.propertyKeyIds, that.propertyKeyIds );
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return hashcode( labelId, propertyKeyIds );
    }

    /**
     * @return label token id this index is for.
     */
    public int getLabelId()
    {
        return labelId;
    }

    /**
     * @return property key token id this index is for.
     */
    public int getPropertyKeyId()
    {
        throw new UnsupportedOperationException( "Cannot get single property Id of composite index" );
    }

    /**
     * @return property key token ids this index is for.
     */
    public int[] getPropertyKeyIds()
    {
        return propertyKeyIds;
    }

    @Override
    public String toString()
    {
        return format( ":label[%d](property[%s])", labelId, propertyIdText( propertyKeyIds ) );
    }

    /**
     * @param tokenNameLookup used for looking up names for token ids.
     * @return a user friendly description of what this index indexes.
     */
    public String userDescription( TokenNameLookup tokenNameLookup )
    {
        return format( ":%s(%s)", tokenNameLookup.labelGetName( labelId ),
                propertyNameText( tokenNameLookup, propertyKeyIds ) );
    }

    public static String propertyNameText( TokenNameLookup tokenNameLookup, int[] propertyKeyIds )
    {
        return Arrays.stream( propertyKeyIds ).mapToObj( id ->
                tokenNameLookup.propertyKeyGetName( id ) ).collect( Collectors.joining( "," ) );
    }

    public static String propertyIdText( int[] propertyKeyIds )
    {
        return Arrays.stream( propertyKeyIds ).mapToObj( id ->
                Integer.toString( id ) ).collect( Collectors.joining( "," ) );
    }

    public static int hashcode( int labelId, int[] propertyKeyIds )
    {
        int result = labelId;
        for ( int element : propertyKeyIds )
        {
            result = 31 * result + element;
        }
        return result;
    }
}
