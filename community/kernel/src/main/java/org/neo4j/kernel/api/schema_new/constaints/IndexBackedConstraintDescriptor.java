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
package org.neo4j.kernel.api.schema_new.constaints;

import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.schema_new.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.SchemaUtil;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptorFactory;

public abstract class IndexBackedConstraintDescriptor extends ConstraintDescriptor implements LabelSchemaDescriptor.Supplier
{
    private final LabelSchemaDescriptor schema;

    IndexBackedConstraintDescriptor( Type type, LabelSchemaDescriptor schema )
    {
        super( type );
        this.schema = schema;
    }

    @Override
    public LabelSchemaDescriptor schema()
    {
        return schema;
    }

    public NewIndexDescriptor ownedIndexDescriptor()
    {
        return NewIndexDescriptorFactory.uniqueForSchema( schema );
    }

    @Override
    public String prettyPrint( TokenNameLookup tokenNameLookup )
    {
        String labelName = escapeLabelOrRelTyp( tokenNameLookup.labelGetName( schema.getLabelId() ) );
        String nodeName = labelName.toLowerCase();
        String properties = SchemaUtil.niceProperties( tokenNameLookup, schema.getPropertyIds(), nodeName + "." );
        if ( schema().getPropertyIds().length > 1 )
        {
            properties = "(" + properties + ")";
        }
        return String.format( "CONSTRAINT ON ( %s:%s ) ASSERT %s IS %s", nodeName, labelName, properties,
                constraintTypeText() );
    }

    protected abstract String constraintTypeText();
}
