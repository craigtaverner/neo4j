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
package org.neo4j.consistency.checking.full;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.NODE_CURSOR;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.test.mockito.mock.Property.property;
import static org.neo4j.test.mockito.mock.Property.set;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.checking.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.GraphStoreFixture;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.impl.schema.TextIndexProvider;
import org.neo4j.kernel.api.impl.schema.trigram.TrigramIndexProvider;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.storageengine.api.EntityUpdates;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class SpecialisedIndexFullCheckTest {
    @TestDirectoryExtension
    abstract class TestBase {

        private static final String PROP1 = "key1";
        private static final String PROP2 = "key2";

        @Inject
        private TestDirectory testDirectory;

        protected GraphStoreFixture fixture;
        private final ByteArrayOutputStream logStream = new ByteArrayOutputStream();
        private final Log4jLogProvider logProvider = new Log4jLogProvider(logStream);

        protected final List<Long> indexedNodes = new ArrayList<>();
        private final List<Long> indexedRelationships = new ArrayList<>();
        private final Map<Setting<?>, Object> settings = new HashMap<>();

        abstract IndexType type();

        abstract Object indexedValue();

        abstract Object anotherIndexedValue();

        abstract Object notIndexedValue();

        void createNodeIndex(Transaction tx, String propertyKey) {
            tx.schema()
                    .indexFor(label("Label1"))
                    .on(propertyKey)
                    .withIndexType(type())
                    .create();
        }

        void createRelIndex(Transaction tx, String propertyKey) {
            tx.schema()
                    .indexFor(withName("Type1"))
                    .on(propertyKey)
                    .withIndexType(type())
                    .create();
        }

        @BeforeEach
        protected void setUp() {
            settings.put(GraphDatabaseInternalSettings.trigram_index, true);
            fixture = createFixture();
        }

        @AfterEach
        void tearDown() {
            fixture.close();
        }

        @Test
        void shouldCheckConsistencyOfAConsistentStore() throws Exception {
            ConsistencySummaryStatistics result = check();

            assertTrue(result.isConsistent(), result.toString());
        }

        @ParameterizedTest
        @EnumSource(IndexSize.class)
        void shouldReportIndexInconsistencies(IndexSize indexSize) throws Exception {
            indexSize.createAdditionalData(fixture);

            NodeStore nodeStore = fixture.directStoreAccess().nativeStores().getNodeStore();
            StoreCursors storeCursors = fixture.getStoreCursors();
            try (var cursor = storeCursors.writeCursor(NODE_CURSOR)) {
                for (Long id : indexedNodes) {
                    NodeRecord nodeRecord = new NodeRecord(id);
                    nodeRecord.clear();
                    nodeStore.updateRecord(nodeRecord, cursor, NULL_CONTEXT, storeCursors);
                }
            }

            ConsistencySummaryStatistics stats = check();

            assertFalse(stats.isConsistent());
            assertThat(logStream.toString()).contains("This index entry refers to a node record that is not in use");
            assertThat(stats.getInconsistencyCountForRecordType(RecordType.INDEX.name()))
                    .isEqualTo(3);
        }

        @ParameterizedTest
        @EnumSource(IndexSize.class)
        void shouldReportNodesThatAreNotIndexed(IndexSize indexSize) throws Exception {
            indexSize.createAdditionalData(fixture);

            Iterator<IndexDescriptor> indexDescriptorIterator = getValueIndexDescriptors();
            while (indexDescriptorIterator.hasNext()) {
                IndexDescriptor indexDescriptor = indexDescriptorIterator.next();
                if (indexDescriptor.schema().entityType() == EntityType.NODE) {
                    IndexAccessor accessor = fixture.indexAccessorLookup().apply(indexDescriptor);
                    try (IndexUpdater updater = accessor.newUpdater(IndexUpdateMode.ONLINE, NULL_CONTEXT, false)) {
                        for (long nodeId : indexedNodes) {
                            EntityUpdates updates = fixture.nodeAsUpdates(nodeId);
                            for (IndexEntryUpdate<?> update :
                                    updates.valueUpdatesForIndexKeys(singletonList(indexDescriptor))) {
                                updater.process(IndexEntryUpdate.remove(
                                        nodeId, indexDescriptor, ((ValueIndexEntryUpdate<?>) update).values()));
                            }
                        }
                    }
                }
            }

            ConsistencySummaryStatistics stats = check();

            assertFalse(stats.isConsistent());
            assertThat(logStream.toString()).contains("This node was not found in the expected index");
            assertThat(stats.getInconsistencyCountForRecordType(RecordType.NODE.name()))
                    .isEqualTo(3);
        }

        // All the index types doesn't stores values and will not actually be tested by different checkers depending on
        // the size,
        // but doesn't hurt to run it for all anyway.
        @ParameterizedTest
        @EnumSource(IndexSize.class)
        void shouldReportRelationshipsThatAreNotIndexed(IndexSize indexSize) throws Exception {
            indexSize.createAdditionalData(fixture);

            Iterator<IndexDescriptor> indexDescriptorIterator = getValueIndexDescriptors();
            while (indexDescriptorIterator.hasNext()) {
                IndexDescriptor indexDescriptor = indexDescriptorIterator.next();
                if (indexDescriptor.schema().entityType() == EntityType.RELATIONSHIP) {
                    IndexAccessor accessor = fixture.indexAccessorLookup().apply(indexDescriptor);
                    try (IndexUpdater updater = accessor.newUpdater(IndexUpdateMode.ONLINE, NULL_CONTEXT, false)) {
                        for (long relId : indexedRelationships) {
                            EntityUpdates updates = fixture.relationshipAsUpdates(relId);
                            for (IndexEntryUpdate<?> update :
                                    updates.valueUpdatesForIndexKeys(singletonList(indexDescriptor))) {
                                updater.process(IndexEntryUpdate.remove(
                                        relId, indexDescriptor, ((ValueIndexEntryUpdate<?>) update).values()));
                            }
                        }
                    }
                }
            }

            ConsistencySummaryStatistics stats = check();

            assertFalse(stats.isConsistent());
            assertThat(logStream.toString()).contains("This relationship was not found in the expected index");
            assertThat(stats.getInconsistencyCountForRecordType(RecordType.RELATIONSHIP.name()))
                    .isEqualTo(3);
        }

        @ParameterizedTest
        @EnumSource(IndexSize.class)
        void shouldReportNodesThatAreIndexedWhenTheyShouldNotBe(IndexSize indexSize) throws Exception {
            indexSize.createAdditionalData(fixture);

            // given
            long newNode = createOneNode();

            Iterator<IndexDescriptor> indexDescriptorIterator = getValueIndexDescriptors();
            while (indexDescriptorIterator.hasNext()) {
                IndexDescriptor indexDescriptor = indexDescriptorIterator.next();
                if (indexDescriptor.schema().entityType() == EntityType.NODE && !indexDescriptor.isUnique()) {
                    IndexAccessor accessor = fixture.indexAccessorLookup().apply(indexDescriptor);
                    try (IndexUpdater updater = accessor.newUpdater(IndexUpdateMode.ONLINE, NULL_CONTEXT, false)) {
                        updater.process(IndexEntryUpdate.add(newNode, indexDescriptor, values(indexDescriptor)));
                    }
                }
            }

            // when
            ConsistencySummaryStatistics stats = check();

            assertFalse(stats.isConsistent());
            assertThat(stats.getInconsistencyCountForRecordType(RecordType.INDEX.name()))
                    .isEqualTo(2);
        }

        Value[] values(IndexDescriptor indexRule) {
            switch (indexRule.schema().getPropertyIds().length) {
                case 1:
                    return Iterators.array(Values.of(indexedValue()));
                case 2:
                    return Iterators.array(Values.of(indexedValue()), Values.of(anotherIndexedValue()));
                default:
                    throw new UnsupportedOperationException();
            }
        }

        private Iterator<IndexDescriptor> getValueIndexDescriptors() {
            return Iterators.filter(descriptor -> !descriptor.isTokenIndex(), fixture.getIndexDescriptors());
        }

        private ConsistencySummaryStatistics check() throws ConsistencyCheckIncompleteException {
            // the database must not be running during the check because of Lucene-based indexes
            // Lucene files are locked when the DB is running
            fixture.close();

            var config = Config.newBuilder()
                    .set(GraphDatabaseSettings.neo4j_home, testDirectory.homePath())
                    .set(settings)
                    .build();
            return new ConsistencyCheckService(Neo4jLayout.of(config).databaseLayout("neo4j"))
                    .with(config)
                    .with(logProvider)
                    .runFullConsistencyCheck()
                    .summary();
        }

        private GraphStoreFixture createFixture() {
            return new GraphStoreFixture(testDirectory) {
                @Override
                protected void generateInitialData(GraphDatabaseService db) {
                    try (var tx = db.beginTx()) {
                        createNodeIndex(tx, PROP1);
                        createNodeIndex(tx, PROP2);

                        createRelIndex(tx, PROP1);
                        createRelIndex(tx, PROP2);
                        tx.commit();
                    }
                    try (var tx = db.beginTx()) {
                        tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
                    }

                    // Create initial data
                    try (org.neo4j.graphdb.Transaction tx = db.beginTx()) {
                        Node node1 = set(tx.createNode(label("Label1")), property(PROP1, indexedValue()));
                        Node node2 = set(
                                tx.createNode(label("Label1")),
                                property(PROP1, indexedValue()),
                                property(PROP2, anotherIndexedValue()));
                        Node node3 = set(tx.createNode(label("Label1")), property(PROP1, notIndexedValue()));
                        set(tx.createNode(label("AnotherLabel")), property(PROP1, indexedValue()));
                        set(tx.createNode(label("Label1")), property("anotherProperty", indexedValue()));
                        Node node6 = tx.createNode();

                        indexedNodes.add(node1.getId());
                        indexedNodes.add(node2.getId());

                        // Add another node that is indexed so our tests removing an indexed entry actually run for both
                        // IndexSizes
                        set(
                                tx.createNode(label("Label1")),
                                property(PROP1, indexedValue()),
                                property(PROP2, anotherIndexedValue()));

                        indexedRelationships.add(set(
                                        node1.createRelationshipTo(node6, withName("Type1")),
                                        property(PROP1, indexedValue()))
                                .getId());
                        indexedRelationships.add(set(
                                        node2.createRelationshipTo(node6, withName("Type1")),
                                        property(PROP1, indexedValue()),
                                        property(PROP2, anotherIndexedValue()))
                                .getId());
                        set(node3.createRelationshipTo(node6, withName("Type1")), property(PROP1, notIndexedValue()))
                                .getId();

                        // Add another relationship that is indexed so our tests removing an indexed entry actually run
                        // for both IndexSizes
                        set(
                                node1.createRelationshipTo(node3, withName("Type1")),
                                property(PROP1, anotherIndexedValue()),
                                property(PROP2, indexedValue()));
                        tx.commit();
                    }
                }

                @Override
                protected Map<Setting<?>, Object> getConfig() {
                    return settings;
                }
            };
        }

        protected long createOneNode() {
            final AtomicLong id = new AtomicLong();
            fixture.apply(tx -> {
                id.set(tx.createNode().getId());
            });
            return id.get();
        }
    }

    @Nested
    class PointIndex extends TestBase {

        @Override
        IndexType type() {
            return IndexType.POINT;
        }

        @Override
        Object indexedValue() {
            return Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 1, 2);
        }

        @Override
        Object anotherIndexedValue() {
            return Values.pointValue(CoordinateReferenceSystem.WGS_84_3D, 1, 2, 3);
        }

        @Override
        Object notIndexedValue() {
            return "some string";
        }
    }

    @Nested
    class TextIndex extends TestBase {

        @Override
        IndexType type() {
            return IndexType.TEXT;
        }

        @Override
        void createNodeIndex(Transaction tx, String propertyKey) {
            createNodeTextIndex((TransactionImpl) tx, propertyKey, TextIndexProvider.DESCRIPTOR);
        }

        @Override
        void createRelIndex(Transaction tx, String propertyKey) {
            createRelTextIndex((TransactionImpl) tx, propertyKey, TextIndexProvider.DESCRIPTOR);
        }

        @Override
        Object indexedValue() {
            return "some text";
        }

        @Override
        Object anotherIndexedValue() {
            return "another piece of text";
        }

        @Override
        Object notIndexedValue() {
            return 123;
        }
    }

    @Nested
    class TrigramTextIndex extends TestBase {

        @Override
        IndexType type() {
            return IndexType.TEXT;
        }

        @Override
        void createNodeIndex(Transaction tx, String propertyKey) {
            createNodeTextIndex((TransactionImpl) tx, propertyKey, TrigramIndexProvider.DESCRIPTOR);
        }

        @Override
        void createRelIndex(Transaction tx, String propertyKey) {
            createRelTextIndex((TransactionImpl) tx, propertyKey, TrigramIndexProvider.DESCRIPTOR);
        }

        @Override
        Object indexedValue() {
            return "some text";
        }

        @Override
        Object anotherIndexedValue() {
            return "another piece of text";
        }

        @Override
        Object notIndexedValue() {
            return 123;
        }
    }

    private void createRelTextIndex(TransactionImpl tx, String propertyKey, IndexProviderDescriptor descriptor) {
        try {
            KernelTransaction kernelTransaction = tx.kernelTransaction();
            TokenWrite tokenWrite = kernelTransaction.tokenWrite();
            int type = tokenWrite.relationshipTypeGetOrCreateForName("Type1");
            int prop = tokenWrite.propertyKeyGetOrCreateForName(propertyKey);

            IndexPrototype prototype = IndexPrototype.forSchema(SchemaDescriptors.forRelType(type, prop))
                    .withIndexType(org.neo4j.internal.schema.IndexType.TEXT)
                    .withIndexProvider(descriptor);
            kernelTransaction.schemaWrite().indexCreate(prototype);
        } catch (KernelException e) {
            throw new RuntimeException(e);
        }
    }

    private void createNodeTextIndex(TransactionImpl tx, String propertyKey, IndexProviderDescriptor descriptor) {
        try {
            KernelTransaction kernelTransaction = tx.kernelTransaction();
            TokenWrite tokenWrite = kernelTransaction.tokenWrite();
            int label = tokenWrite.labelGetOrCreateForName("Label1");
            int prop = tokenWrite.propertyKeyGetOrCreateForName(propertyKey);

            IndexPrototype prototype = IndexPrototype.forSchema(SchemaDescriptors.forLabel(label, prop))
                    .withIndexType(org.neo4j.internal.schema.IndexType.TEXT)
                    .withIndexProvider(descriptor);
            kernelTransaction.schemaWrite().indexCreate(prototype);
        } catch (KernelException e) {
            throw new RuntimeException(e);
        }
    }

    @Nested
    class FullTextIndex extends TestBase {

        @Override
        IndexType type() {
            return IndexType.FULLTEXT;
        }

        @Override
        Object indexedValue() {
            return "some text";
        }

        @Override
        Object anotherIndexedValue() {
            return "another piece of text";
        }

        @Override
        Object notIndexedValue() {
            return 123;
        }
    }

    /**
     * Indexes are consistency checked in different ways depending on their size.
     * This can be used to make the indexes created in the setup appear large or small.
     */
    private enum IndexSize {
        SMALL_INDEX {
            @Override
            public void createAdditionalData(GraphStoreFixture fixture) {
                fixture.apply(tx -> {
                    // Create more nodes/relationships so our indexes will be considered to be small indexes
                    // (less than 5% of nodes/relationships in index).
                    for (int i = 0; i < 80; i++) {
                        Node node = tx.createNode();
                        node.createRelationshipTo(node, withName("OtherType"));
                    }
                });
            }
        },
        LARGE_INDEX {
            @Override
            public void createAdditionalData(GraphStoreFixture fixture) {}
        };

        public abstract void createAdditionalData(GraphStoreFixture fixture);
    }
}
