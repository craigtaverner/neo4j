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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import org.neo4j.server.security.enterprise.auth.plugin.api.AuthToken;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationInfo;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationPlugin;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthorizationInfo;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthorizationPlugin;
import org.neo4j.server.security.enterprise.auth.plugin.spi.PluginTokenRules;
import org.neo4j.server.security.enterprise.auth.plugin.spi.PluginTokenRulesProvider;

public class TestCombinedAuthWithTokenRulesPlugin extends AuthenticationPlugin.Adapter implements AuthorizationPlugin
{
    public static final String allowedLabel = "User";
    public static final String deniedRelationshipType = "KNOWS";
    public static final String deniedProperty = "ssn";
    private final PluginTokenRules tokenRules;

    public TestCombinedAuthWithTokenRulesPlugin()
    {
        this.tokenRules = new PluginTokenRulesBuilder().allowLabels( allowedLabel )
                .denyRelationshipTypes( deniedRelationshipType ).denyProperties( deniedProperty ).build();
    }

    @Override
    public String name()
    {
        return getClass().getSimpleName();
    }

    @Override
    public AuthenticationInfo authenticate( AuthToken authToken )
    {
        String principal = authToken.principal();
        char[] credentials = authToken.credentials();

        if ( principal.equals( "neo4j" ) && Arrays.equals( credentials, "neo4j".toCharArray() ) )
        {
            return AuthenticationInfo.of( "neo4j" );
        }
        else if ( principal.equals( "restricted" ) && Arrays.equals( credentials, "neo4j".toCharArray() ) )
        {
            return (AuthenticationInfo) () -> "restricted";
        }
        return null;
    }

    @Override
    public AuthorizationInfo authorize( Collection<PrincipalAndProvider> principals )
    {
        if ( principals.stream().anyMatch( p -> "neo4j".equals( p.principal() ) ) )
        {
            return (AuthorizationInfo) () -> Collections.singleton( PredefinedRoles.PUBLISHER );
        }
        else if ( principals.stream().anyMatch( p -> "restricted".equals( p.principal() ) ) )
        {
            return (AuthorizationInfo) () -> Collections.singleton( PredefinedRoles.READER );
        }
        return null;
    }

    @Override
    public PluginTokenRulesProvider getTokenRulesProvider()
    {
        return roles -> roles.contains( PredefinedRoles.READER ) ? Optional.of( tokenRules ) : Optional.empty();
    }
}
