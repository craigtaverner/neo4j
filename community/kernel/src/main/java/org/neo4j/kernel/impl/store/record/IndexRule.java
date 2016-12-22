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
import java.util.stream.Collectors;

import org.neo4j.graphdb.Label;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.storageengine.api.schema.IndexSchemaRule;
import org.neo4j.string.UTF8;

import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.safeCastLongToInt;
import static org.neo4j.string.UTF8.getDecodedStringFrom;

/**
 * A {@link Label} can have zero or more index rules which will have data specified in the rules indexed.
 */
public class IndexRule extends AbstractSchemaRule implements IndexSchemaRule
{
    private static final long NO_OWNING_CONSTRAINT = -1;
    private final SchemaIndexProvider.Descriptor providerDescriptor;
    private final int label;
    private final int[] propertyKeys;
    /**
     * Non-null for constraint indexes, equal to {@link #NO_OWNING_CONSTRAINT} for
     * constraint indexes with no owning constraint record.
     */
    private final Long owningConstraint;

    static IndexRule readIndexRule( long id, boolean constraintIndex, int label, ByteBuffer serialized )
    {
        SchemaIndexProvider.Descriptor providerDescriptor = readProviderDescriptor( serialized );
        int[] propertyKeyIds = readPropertyKeys( serialized );
        if ( constraintIndex )
        {
            long owningConstraint = readOwningConstraint( serialized );
            return constraintIndexRule( id, label, propertyKeyIds, providerDescriptor, owningConstraint );
        }
        else
        {
            return indexRule( id, label, propertyKeyIds, providerDescriptor );
        }
    }

    public static IndexRule indexRule( long id, int label, int[] propertyKeyIds,
                                       SchemaIndexProvider.Descriptor providerDescriptor )
    {
        return new IndexRule( id, label, propertyKeyIds, providerDescriptor, null );
    }

    public static IndexRule constraintIndexRule( long id, int label, int[] propertyKeyIds,
                                                 SchemaIndexProvider.Descriptor providerDescriptor,
                                                 Long owningConstraint )
    {
        return new IndexRule( id, label, propertyKeyIds, providerDescriptor,
                              owningConstraint == null ? NO_OWNING_CONSTRAINT : owningConstraint );
    }

    public IndexRule( long id, int label, int[] propertyKeys, SchemaIndexProvider.Descriptor providerDescriptor,
                       Long owningConstraint )
    {
        super( id, indexKind( owningConstraint ) );
        this.owningConstraint = owningConstraint;

        if ( providerDescriptor == null )
        {
            throw new IllegalArgumentException( "null provider descriptor prohibited" );
        }

        this.providerDescriptor = providerDescriptor;
        this.label = label;
        this.propertyKeys = propertyKeys;
    }

    private static Kind indexKind( Long owningConstraint )
    {
        return owningConstraint == null ? Kind.INDEX_RULE : Kind.CONSTRAINT_INDEX_RULE;
    }

    private static SchemaIndexProvider.Descriptor readProviderDescriptor( ByteBuffer serialized )
    {
        String providerKey = getDecodedStringFrom( serialized );
        String providerVersion = getDecodedStringFrom( serialized );
        return new SchemaIndexProvider.Descriptor( providerKey, providerVersion );
    }

    private static int[] readPropertyKeys( ByteBuffer serialized )
    {
        // Currently only one key is supported although the data format supports multiple
        int count = serialized.getShort();
        assert count >= 1;

        // Changed from being a long to an int 2013-09-10, but keeps reading a long to not change the store format.
        int[] props = new int[count];
        for ( int i = 0; i < count; i++ )
        {
            props[i] = safeCastLongToInt( serialized.getLong() );
        }
        return props;
    }

    private static long readOwningConstraint( ByteBuffer serialized )
    {
        return serialized.getLong();
    }

    public SchemaIndexProvider.Descriptor getProviderDescriptor()
    {
        return providerDescriptor;
    }

    @Override
    public int[] getPropertyKeys()
    {
        return propertyKeys;
    }

    @Override
    public boolean isConstraintIndex()
    {
        return owningConstraint != null;
    }

    @Override
    public Long getOwningConstraint()
    {
        if ( !isConstraintIndex() )
        {
            throw new IllegalStateException( "Can only get owner from constraint indexes." );
        }
        long owningConstraint = this.owningConstraint;
        if ( owningConstraint == NO_OWNING_CONSTRAINT )
        {
            return null;
        }
        return owningConstraint;
    }

    @Override
    public int getLabel()
    {
        return label;
    }

    @Override
    public int getRelationshipType()
    {
        throw new IllegalStateException( "Index rule is associated with nodes" );
    }

    @Override
    public int length()
    {
        return 4 /* label id */
               + 1 /* kind id */
               + UTF8.computeRequiredByteBufferSize( providerDescriptor.getKey() )
               + UTF8.computeRequiredByteBufferSize( providerDescriptor.getVersion() )
               + 2                                  /* number of property keys (short) */
               + 8 * propertyKeys.length            /* the property keys, each 8 bytes (long) */
               + (isConstraintIndex() ? 8 : 0)      /* constraint indexes have an owner field */;
    }

    @Override
    public void serialize( ByteBuffer target )
    {
        target.putInt( label );
        // 0 is reserved, so use ordinal + 1
        target.put( (byte) (kind.ordinal()+1) );
        UTF8.putEncodedStringInto( providerDescriptor.getKey(), target );
        UTF8.putEncodedStringInto( providerDescriptor.getVersion(), target );
        target.putShort( (short) propertyKeys.length );
        for ( int i = 0; i < propertyKeys.length; i++ )
        {
            target.putLong( propertyKeys[i] );
        }
        if ( isConstraintIndex() )
        {
            target.putLong( owningConstraint );
        }
    }

    @Override
    public int hashCode()
    {
        // TODO: Think if this needs to be extended with providerDescriptor
        int result = 31 * (31 * super.hashCode() + label);
        for (int element : propertyKeys)
        {
            result = 31 * result + element;
        }
        return result;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        if ( !super.equals( o ) )
        {
            return false;
        }
        IndexRule indexRule = (IndexRule) o;
        return label == indexRule.label && Arrays.equals( propertyKeys, indexRule.propertyKeys );
    }

    @Override
    public String toString()
    {
        String ownerString = "";
        if ( owningConstraint != null )
        {
            ownerString = ", owner=" + (owningConstraint == -1 ? "<not set>" : owningConstraint);
        }
        String propertyKeyString = Arrays.stream(propertyKeys).mapToObj(id -> Integer.toString(id)).collect(
                Collectors.joining(","));

        return "IndexRule[id=" + id + ", label=" + label + ", kind=" + kind +
               ", provider=" + providerDescriptor + ", properties=" + propertyKeyString + ownerString + "]";
    }

    public IndexRule withOwningConstraint( long constraintId )
    {
        if ( !isConstraintIndex() )
        {
            throw new IllegalStateException( this + " is not a constraint index" );
        }
        return constraintIndexRule( getId(), getLabel(), getPropertyKeys(), getProviderDescriptor(), constraintId );
    }
}
