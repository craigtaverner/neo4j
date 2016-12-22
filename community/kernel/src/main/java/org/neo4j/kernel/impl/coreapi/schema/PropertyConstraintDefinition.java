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
package org.neo4j.kernel.impl.coreapi.schema;

import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

abstract class PropertyConstraintDefinition implements ConstraintDefinition
{
    protected final InternalSchemaActions actions;
    protected final String[] propertyKeys;

    protected PropertyConstraintDefinition( InternalSchemaActions actions, String[] propertyKeys )
    {
        this.actions = requireNonNull( actions );
        this.propertyKeys = requireNonEmpty( propertyKeys );
    }

    private static String[] requireNonEmpty(String[] array)
    {
        requireNonNull( array );
        if ( array.length < 1 )
        {
            throw new IllegalArgumentException( "Property constraint must have at least one property" );
        }
        for ( String field : array )
        {
            if ( field == null )
            { throw new NullPointerException(); }
        }
        return array;
    }

    @Override
    public Iterable<String> getPropertyKeys()
    {
        assertInUnterminatedTransaction();
        return asList( propertyKeys );
    }

    @Override
    public boolean isConstraintType( ConstraintType type )
    {
        assertInUnterminatedTransaction();
        return getConstraintType().equals( type );
    }

    @Override
    public abstract boolean equals( Object o );

    @Override
    public abstract int hashCode();

    /**
     * Returned string is used in shell's constraint listing.
     */
    @Override
    public abstract String toString();

    protected void assertInUnterminatedTransaction()
    {
        actions.assertInOpenTransaction();
    }
}
