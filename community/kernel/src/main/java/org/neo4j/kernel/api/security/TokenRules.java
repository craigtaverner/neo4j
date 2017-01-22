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
package org.neo4j.kernel.api.security;

public interface TokenRules
{
    enum Static implements TokenRules
    {
        /** No reading or writing allowed. */
        NONE
                {
                    @Override
                    public boolean allowsLabelReads(String name)
                    {
                        return false;
                    }

                    @Override
                    public boolean allowsLabelWrites(String name)
                    {
                        return false;
                    }
                },

        /** Allows reading tokens, but not writing them. */
        READ_ONLY
                {
                    @Override
                    public boolean allowsLabelReads(String name)
                    {
                        return true;
                    }

                    @Override
                    public boolean allowsLabelWrites(String name)
                    {
                        return false;
                    }
                },

        /** Allows writing tokens, but not reading them */
        WRITE_ONLY
                {
                    @Override
                    public boolean allowsLabelReads(String name)
                    {
                        return false;
                    }

                    @Override
                    public boolean allowsLabelWrites(String name)
                    {
                        return true;
                    }
                },

        /** Allows reading and writing tokens */
        READ_WRITE
                {
                    @Override
                    public boolean allowsLabelReads(String name)
                    {
                        return true;
                    }

                    @Override
                    public boolean allowsLabelWrites(String name)
                    {
                        return true;
                    }
                };
    }

    boolean allowsLabelReads(String name);

    boolean allowsLabelWrites(String name);
}