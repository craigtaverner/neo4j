/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.bolt.protocol.common.transaction;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.neo4j.bolt.dbapi.BoltQueryExecutor;
import org.neo4j.bolt.dbapi.BoltTransaction;
import org.neo4j.bolt.protocol.common.bookmark.Bookmark;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.common.message.result.BoltResultHandle;
import org.neo4j.bolt.protocol.v41.message.request.RoutingContext;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.values.virtual.MapValue;

public interface TransactionStateMachineSPI {
    Bookmark newestBookmark(BoltTransaction tx);

    BoltTransaction beginTransaction(
            KernelTransaction.Type transactionType,
            LoginContext loginContext,
            List<Bookmark> bookmarks,
            Duration txTimeout,
            AccessMode accessMode,
            Map<String, Object> txMetaData,
            RoutingContext routingContext);

    BoltResultHandle executeQuery(BoltQueryExecutor boltQueryExecutor, String statement, MapValue params);

    boolean supportsNestedStatementsInTransaction();

    void transactionClosed();
}
