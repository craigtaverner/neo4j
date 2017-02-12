/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.security.enterprise.auth.plugin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.server.security.enterprise.auth.plugin.spi.PluginTokenRules;

public class PluginTokenRulesBuilder
{
    private final ArrayList<String> allowedLabels = new ArrayList<>();
    private final ArrayList<String> deniedLabels = new ArrayList<>();
    private final ArrayList<String> allowedTypes = new ArrayList<>();
    private final ArrayList<String> deniedTypes = new ArrayList<>();
    private final ArrayList<String> allowedProperties = new ArrayList<>();
    private final ArrayList<String> deniedProperties = new ArrayList<>();

    public PluginTokenRulesBuilder denyLabels( String... labels )
    {
        Arrays.stream( labels ).forEach( deniedLabels::add );
        return this;
    }

    public PluginTokenRulesBuilder allowLabels( String... labels )
    {
        Arrays.stream( labels ).forEach( allowedLabels::add );
        return this;
    }

    public PluginTokenRulesBuilder denyRelationshipTypes( String... labels )
    {
        Arrays.stream( labels ).forEach( deniedTypes::add );
        return this;
    }

    public PluginTokenRulesBuilder allowRelationshipTypes( String... labels )
    {
        Arrays.stream( labels ).forEach( allowedTypes::add );
        return this;
    }

    public PluginTokenRulesBuilder denyProperties( String... labels )
    {
        Arrays.stream( labels ).forEach( deniedProperties::add );
        return this;
    }

    public PluginTokenRulesBuilder allowProperties( String... labels )
    {
        Arrays.stream( labels ).forEach( allowedProperties::add );
        return this;
    }

    public PluginTokenRules build()
    {
        return new TokenRulesImpl( this );
    }

    private static class TokenRulesImpl implements PluginTokenRules
    {
        private final PluginTokenRulesBuilder builder;

        private TokenRulesImpl( PluginTokenRulesBuilder builder )
        {
            this.builder = builder;
        }

        private static boolean notDeniedOrExplicitlyAllowed( String name, List<String> denied, List<String> allowed )
        {
            return !denied.contains( name ) && (allowed.size() == 0 || allowed.contains( name ));
        }

        @Override
        public boolean allowsLabelReads( String name )
        {
            return notDeniedOrExplicitlyAllowed( name, builder.deniedLabels, builder.allowedLabels );
        }

        @Override
        public boolean allowsRelationshipTypeReads( String name )
        {
            return notDeniedOrExplicitlyAllowed( name, builder.deniedTypes, builder.allowedTypes );
        }

        @Override
        public boolean allowsPropertyReads( String name )
        {
            return notDeniedOrExplicitlyAllowed( name, builder.deniedProperties, builder.allowedProperties );
        }
    }
}
