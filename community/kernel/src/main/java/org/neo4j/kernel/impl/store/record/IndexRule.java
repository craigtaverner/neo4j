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
import org.neo4j.kernel.api.NodePropertyDescriptor;
import org.neo4j.kernel.api.index.CompositeIndexDescriptor;
import org.neo4j.kernel.api.index.IndexDescriptor;
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
    private final NodePropertyDescriptor descriptor;
    /**
     * Non-null for constraint indexes, equal to {@link #NO_OWNING_CONSTRAINT} for
     * constraint indexes with no owning constraint record.
     */
    private final Long owningConstraint;

    static IndexRule readIndexRule( long id, boolean constraintIndex, int label, ByteBuffer serialized )
    {
        SchemaIndexProvider.Descriptor providerDescriptor = readProviderDescriptor( serialized );
        NodePropertyDescriptor descriptor = new CompositeIndexDescriptor( label, readPropertyKeys( serialized ) );
        if ( constraintIndex )
        {
            long owningConstraint = readOwningConstraint( serialized );
            return constraintIndexRule( id, descriptor, providerDescriptor, owningConstraint );
        }
        else
        {
            return indexRule( id, descriptor, providerDescriptor );
        }
    }

    public static IndexRule indexRule( long id, NodePropertyDescriptor descriptor,
            SchemaIndexProvider.Descriptor providerDescriptor )
    {
        return new IndexRule( id, descriptor, providerDescriptor, null );
    }

    public static IndexRule constraintIndexRule( long id, NodePropertyDescriptor descriptor,
                                                 SchemaIndexProvider.Descriptor providerDescriptor,
                                                 Long owningConstraint )
    {
        return new IndexRule( id, descriptor, providerDescriptor,
                              owningConstraint == null ? NO_OWNING_CONSTRAINT : owningConstraint );
    }

    public IndexRule( long id, NodePropertyDescriptor descriptor, SchemaIndexProvider.Descriptor providerDescriptor,
                       Long owningConstraint )
    {
        super( id, indexKind( owningConstraint, descriptor.isComposite() ) );
        this.owningConstraint = owningConstraint;

        if ( providerDescriptor == null )
        {
            throw new IllegalArgumentException( "null provider descriptor prohibited" );
        }

        this.providerDescriptor = providerDescriptor;
        this.descriptor = descriptor;
    }

    private static Kind indexKind( Long owningConstraint, boolean isComposite )
    {
        return owningConstraint == null ? (isComposite ? Kind.COMPOSITE_INDEX_RULE : Kind.INDEX_RULE)
                                        : Kind.CONSTRAINT_INDEX_RULE;
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
        return descriptor.getPropertyKeyIds();
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
        return descriptor.getLabelId();
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
               + 8 * descriptor.getPropertyKeyIds().length            /* the property keys, each 8 bytes (long) */
               + (isConstraintIndex() ? 8 : 0)      /* constraint indexes have an owner field */;
    }

    @Override
    public void serialize( ByteBuffer target )
    {
        int[] propertyKeys = descriptor.getPropertyKeyIds();
        target.putInt( descriptor.getLabelId() );
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
        return 31 * super.hashCode() + descriptor.hashCode();
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
        return descriptor.equals( ((IndexRule) o).descriptor );
    }

    @Override
    public String toString()
    {
        String ownerString = "";
        if ( owningConstraint != null )
        {
            ownerString = ", owner=" + (owningConstraint == -1 ? "<not set>" : owningConstraint);
        }

        return "IndexRule[id=" + id + ", label=" + descriptor.getLabelId() + ", kind=" + kind +
               ", provider=" + providerDescriptor + ", properties=" + descriptor.propertyIdText() + ownerString + "]";
    }

    public IndexRule withOwningConstraint( long constraintId )
    {
        if ( !isConstraintIndex() )
        {
            throw new IllegalStateException( this + " is not a constraint index" );
        }
        return constraintIndexRule( getId(), descriptor, getProviderDescriptor(), constraintId );
    }
}
