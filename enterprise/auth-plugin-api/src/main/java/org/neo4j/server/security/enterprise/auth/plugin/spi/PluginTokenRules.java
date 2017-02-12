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
package org.neo4j.server.security.enterprise.auth.plugin.spi;

/**
 * Plugins can specify which tokens (labels and relationship types) are allowed to be visible to the authorized user.
 * The user first needs read  access to the database as defined by their roles, and then this will allow them to be
 * allowed or denied read access to specific parts of the graph. Denying access (by returning false in one of the
 * methods below) will not cause an authorization error, but act as though that part of the graph was simply not there.
 */
public interface PluginTokenRules
{
    /**
     * @param name of label
     * @return true if this label should be visible to the user
     */
    boolean allowsLabelReads(String name);

    /**
     * @param name of relationship type
     * @return true if this relationship type should be visible to the user
     */
    boolean allowsRelationshipTypeReads(String name);

    /**
     * @param name of property
     * @return true if properties with this name should be visible to the user
     */
    boolean allowsPropertyReads(String name);
}
