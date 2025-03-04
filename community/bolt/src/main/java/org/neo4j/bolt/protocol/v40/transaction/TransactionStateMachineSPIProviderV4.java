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
package org.neo4j.bolt.protocol.v40.transaction;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.protocol.common.connector.tx.TransactionOwner;
import org.neo4j.bolt.protocol.common.transaction.TransactionStateMachineSPI;
import org.neo4j.bolt.protocol.common.transaction.statement.AbstractTransactionStatementSPIProvider;
import org.neo4j.bolt.protocol.common.transaction.statement.StatementProcessorReleaseManager;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.time.SystemNanoClock;

public class TransactionStateMachineSPIProviderV4 extends AbstractTransactionStatementSPIProvider {
    public static final long SHALLOW_SIZE =
            HeapEstimator.shallowSizeOfInstance(TransactionStateMachineSPIProviderV4.class);

    public TransactionStateMachineSPIProviderV4(
            BoltGraphDatabaseManagementServiceSPI boltGraphDatabaseManagementServiceSPI,
            TransactionOwner owner,
            SystemNanoClock clock) {
        super(boltGraphDatabaseManagementServiceSPI, owner, clock);
    }

    @Override
    protected TransactionStateMachineSPI newTransactionStateMachineSPI(
            BoltGraphDatabaseServiceSPI activeBoltGraphDatabaseServiceSPI,
            StatementProcessorReleaseManager resourceReleaseManager,
            String transactionId) {
        memoryTracker.allocateHeap(TransactionStateMachineV4SPI.SHALLOW_SIZE);
        return new TransactionStateMachineV4SPI(
                activeBoltGraphDatabaseServiceSPI, owner, clock, resourceReleaseManager, transactionId);
    }
}
