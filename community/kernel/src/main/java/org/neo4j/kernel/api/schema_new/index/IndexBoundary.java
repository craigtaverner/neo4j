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
package org.neo4j.kernel.api.schema_new.index;

import org.neo4j.kernel.api.schema.IndexDescriptor;
import org.neo4j.kernel.api.schema_new.LabelSchemaDescriptor;

/**
 * This class represents the boundary of where new index descriptors are converted to old index descriptors
 */
public class IndexBoundary
{
    public static IndexDescriptor map( NewIndexDescriptor descriptor )
    {
        LabelSchemaDescriptor labelSchema = descriptor.schema();
        return org.neo4j.kernel.api.schema.IndexDescriptorFactory.of(
                labelSchema.getLabelId(), labelSchema.getPropertyId() );
    }

    public static NewIndexDescriptor map( IndexDescriptor descriptor )
    {
        if ( descriptor == null )
        {
            return null;
        }
        return NewIndexDescriptorFactory.forLabel( descriptor.getLabelId(), descriptor.getPropertyKeyId() );
    }
}
