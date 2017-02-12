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

import org.apache.shiro.authz.SimpleAuthorizationInfo;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthorizationInfo;
import org.neo4j.server.security.enterprise.auth.plugin.spi.PluginTokenRules;
import org.neo4j.server.security.enterprise.auth.plugin.spi.PluginTokenRulesProvider;

public class PluginAuthorizationInfo extends SimpleAuthorizationInfo implements PluginTokenRulesProvider
{
    private final PluginTokenRulesProvider tokenRulesProvider;

    private PluginAuthorizationInfo( Set<String> roles, PluginTokenRulesProvider tokenRulesProvider )
    {
        super( roles );
        this.tokenRulesProvider = tokenRulesProvider;
    }

    public static PluginAuthorizationInfo create( AuthorizationInfo authorizationInfo, PluginTokenRulesProvider tokenRulesProvider )
    {
        return new PluginAuthorizationInfo( new LinkedHashSet<>( authorizationInfo.roles() ), tokenRulesProvider );
    }

    @Override
    public Optional<PluginTokenRules> getTokenRules( Collection<String> roles)
    {
        return tokenRulesProvider.getTokenRules( roles );
    }
}
