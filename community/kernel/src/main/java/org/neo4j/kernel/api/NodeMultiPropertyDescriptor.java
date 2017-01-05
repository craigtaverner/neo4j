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
package org.neo4j.kernel.api;

import java.util.Arrays;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Description of a single label combined with multiple properties.
 */
public class NodeMultiPropertyDescriptor extends NodePropertyDescriptor
{
    private final int[] propertyKeyIds;

    public NodeMultiPropertyDescriptor( int labelId, int[] propertyKeyIds )
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
            NodeMultiPropertyDescriptor that = (NodeMultiPropertyDescriptor) obj;
            return this.getLabelId() == that.getLabelId() &&
                   Arrays.equals( this.propertyKeyIds, that.propertyKeyIds );
        }
        return false;
    }

    @Override
    public boolean isComposite()
    {
        return true;
    }

    @Override
    public int hashCode()
    {
        return hashcode( getLabelId(), propertyKeyIds );
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
    public String propertyIdText()
    {
        return Arrays.stream( propertyKeyIds ).mapToObj( id -> Integer.toString( id ) )
                .collect( Collectors.joining( "," ) );
    }

    public String propertyNameText( TokenNameLookup tokenNameLookup )
    {
        return Arrays.stream( propertyKeyIds ).mapToObj( id -> tokenNameLookup.propertyKeyGetName( id ) )
                .collect( Collectors.joining( "," ) );
    }


    /**
     * @param tokenNameLookup used for looking up names for token ids.
     * @return a user friendly description of what this index indexes.
     */
    public String userDescription( TokenNameLookup tokenNameLookup )
    {
        return format( ":%s(%s)", tokenNameLookup.labelGetName( getLabelId() ),
                propertyNameText( tokenNameLookup ) );
    }

    //TODO: remove and inline above
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
