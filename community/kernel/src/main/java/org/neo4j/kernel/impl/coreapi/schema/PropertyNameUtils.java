/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.coreapi.schema;

import java.util.ArrayList;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.schema.DefinitionWithProperties;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.DescriptorWithProperties;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.operations.KeyReadOperations;

import static java.lang.String.format;
import static java.util.Arrays.asList;

public class PropertyNameUtils
{
    public static String getPropertyKeyNameAt( DefinitionWithProperties index, int propertyKeyIndex )
    {
        for ( String propertyKey : index.getPropertyKeys() )
        {
            if ( propertyKeyIndex == 0 )
            {
                return propertyKey;
            }
            propertyKeyIndex--;
        }
        return null;
    }

    public static IndexDescriptor getIndexDescriptor(ReadOperations readOperations, IndexDefinition index)
            throws SchemaRuleNotFoundException
    {
        int labelId = readOperations.labelGetForName( index.getLabel().name() );
        int[] propertyKeyIds = getPropertyKeyIds( readOperations, index );
        checkValidLabelAndProperties( index.getLabel(), labelId, propertyKeyIds, index );
        return readOperations.indexGetForLabelAndPropertyKey( labelId, propertyKeyIds );
    }

    private static void checkValidLabelAndProperties(Label label, int labelId, int[] propertyKeyIds, DefinitionWithProperties index)
    {
        if ( labelId == KeyReadOperations.NO_SUCH_LABEL )
        {
            throw new NotFoundException( format( "Label %s not found", label.name() ) );
        }

        for ( int i = 0; i < propertyKeyIds.length; i++ )
        {
            if ( propertyKeyIds[i] == KeyReadOperations.NO_SUCH_PROPERTY_KEY )
            {
                throw new NotFoundException(
                        format( "Property key %s not found", getPropertyKeyNameAt( index, i ) ) );
            }
        }
    }

    public static IndexDescriptor getIndexDescriptor(ReadOperations readOperations, Label label, String[] propertyKeys)
            throws SchemaRuleNotFoundException
    {
        int labelId = readOperations.labelGetForName( label.name() );
        int[] propertyKeyIds = getPropertyKeyIds( readOperations, propertyKeys );
        checkValidLabelAndProperties( label, labelId, propertyKeyIds, () -> asList( propertyKeys ) );
        return readOperations.indexGetForLabelAndPropertyKey( labelId, propertyKeyIds );
    }

    public static String[] getPropertyKeys( ReadOperations readOperations, DescriptorWithProperties index )
            throws PropertyKeyIdNotFoundKernelException
    {
        int[] propertyKeyIds = index.getPropertyKeyIds();
        String[] propertyKeys = new String[propertyKeyIds.length];
        for ( int i = 0; i < propertyKeyIds.length; i++ )
        {
            propertyKeys[i] = readOperations.propertyKeyGetName( propertyKeyIds[i] );
        }
        return propertyKeys;
    }

    public static String[] getPropertyKeys( TokenNameLookup tokenNameLookup, DescriptorWithProperties index )
    {
        int[] propertyKeyIds = index.getPropertyKeyIds();
        String[] propertyKeys = new String[propertyKeyIds.length];
        for ( int i = 0; i < propertyKeyIds.length; i++ )
        {
            propertyKeys[i] = tokenNameLookup.propertyKeyGetName( propertyKeyIds[i] );
        }
        return propertyKeys;
    }

    public static int[] getPropertyKeyIds( ReadOperations statement, DefinitionWithProperties index )
    {
        ArrayList<Integer> propertyKeyIds = new ArrayList<>();
        for ( String propertyKey : index.getPropertyKeys() )
        {
            propertyKeyIds.add( statement.propertyKeyGetForName( propertyKey ) );
        }
        return propertyKeyIds.stream().mapToInt( i -> i ).toArray();
    }

    public static int[] getPropertyKeyIds( ReadOperations statement, String[] propertyKeys )
    {
        int[] propertyKeyIds = new int[propertyKeys.length];
        for ( int i = 0; i < propertyKeys.length; i++ )
        {
            propertyKeyIds[i] = statement.propertyKeyGetForName( propertyKeys[i] );
        }
        return propertyKeyIds;
    }

    public static int[] getOrCreatePropertyKeyIds( SchemaWriteOperations statement, String[] propertyKeys )
            throws IllegalTokenNameException
    {
        int[] propertyKeyIds = new int[propertyKeys.length];
        for ( int i = 0; i < propertyKeys.length; i++ )
        {
            propertyKeyIds[i] = statement.propertyKeyGetOrCreateForName( propertyKeys[i] );
        }
        return propertyKeyIds;
    }
}
