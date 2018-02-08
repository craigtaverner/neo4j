/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.index.schema;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.neo4j.concurrent.WorkSync;
import org.neo4j.gis.spatial.index.Envelope;
import org.neo4j.gis.spatial.index.curves.HilbertSpaceFillingCurve2D;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurve;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.DefaultNonUniqueIndexSampler;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.sampling.UniqueIndexSampler;
import org.neo4j.kernel.impl.index.schema.NativeSchemaIndexPopulator.IndexUpdateApply;
import org.neo4j.kernel.impl.index.schema.NativeSchemaIndexPopulator.IndexUpdateWork;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Value;

import static org.neo4j.helpers.collection.Iterators.asResourceIterator;
import static org.neo4j.helpers.collection.Iterators.iterator;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;
import static org.neo4j.kernel.impl.index.schema.NativeSchemaIndexPopulator.BYTE_FAILED;
import static org.neo4j.kernel.impl.index.schema.NativeSchemaIndexPopulator.BYTE_ONLINE;
import static org.neo4j.kernel.impl.index.schema.NativeSchemaIndexPopulator.BYTE_POPULATING;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;

/**
 * An instance of this class represents a dynamically created sub-index specific to a particular coordinate reference system.
 * This allows the fusion index design to be extended to an unknown number of sub-indexes, one for each CRS.
 */
public class SpatialKnownIndex
{
    private UniqueIndexSampler uniqueSampler;
    private final File indexFile;
    private final PageCache pageCache;
    private final CoordinateReferenceSystem crs;
    private final long indexId;
    private final FileSystemAbstraction fs;
    private final SchemaIndexProvider.Monitor monitor;
    private final RecoveryCleanupWorkCollector recoveryCleanupWorkCollector;
    private final SpaceFillingCurve curve;

    private State state;
    private boolean dropped;
    private byte[] failureBytes;
    private SpatialSchemaKey treeKey;
    private NativeSchemaValue treeValue;
    private SpatialLayout layout;
    private NativeSchemaIndexUpdater<SpatialSchemaKey,NativeSchemaValue> singleUpdater;
    private Writer<SpatialSchemaKey,NativeSchemaValue> singleTreeWriter;
    private NativeSchemaIndex<SpatialSchemaKey,NativeSchemaValue> schemaIndex;
    private WorkSync<IndexUpdateApply<SpatialSchemaKey,NativeSchemaValue>,IndexUpdateWork<SpatialSchemaKey,NativeSchemaValue>> workSync;
    private DefaultNonUniqueIndexSampler generalSampler;

    /**
     * Create a representation of a spatial index for a specific coordinate reference system.
     * This constructor should be used for first time creation.
     */
    public SpatialKnownIndex( IndexDirectoryStructure directoryStructure, CoordinateReferenceSystem crs, long indexId, PageCache pageCache,
            FileSystemAbstraction fs, SchemaIndexProvider.Monitor monitor, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector )
    {
        this.crs = crs;
        this.indexId = indexId;
        this.pageCache = pageCache;
        this.fs = fs;
        this.monitor = monitor;
        this.recoveryCleanupWorkCollector = recoveryCleanupWorkCollector;

        // Depends on crs
        SchemaIndexProvider.Descriptor crsDescriptor =
                new SchemaIndexProvider.Descriptor( Integer.toString( crs.getTable().getTableId() ), Integer.toString( crs.getCode() ) );
        IndexDirectoryStructure indexDir =
                IndexDirectoryStructure.directoriesBySubProvider( directoryStructure ).forProvider( crsDescriptor );
        indexFile = new File( indexDir.directoryForIndex( indexId ), indexFileName( indexId ) );
        curve = new HilbertSpaceFillingCurve2D( envelopeFromCRS( crs ), 8 );
        state = State.NONE;
    }

    public void initialize( IndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
    {
        assert state == State.NONE;
        layout = layout( descriptor );
        treeKey = layout.newKey();
        treeValue = layout.newValue();
        schemaIndex = new NativeSchemaIndex<>( pageCache, fs, indexFile, layout, monitor, descriptor, indexId );
        if ( uniqueSampler( descriptor ) )
        {
            uniqueSampler = new UniqueIndexSampler();
        }
        else
        {
            generalSampler = new DefaultNonUniqueIndexSampler( samplingConfig.sampleSizeLimit() );
        }
        state = State.INIT;
    }

    public void online() throws IOException
    {
        assert state == State.POPULATED || state == State.INIT;
        singleUpdater = new NativeSchemaIndexUpdater<>( treeKey, treeValue );
        schemaIndex.instantiateTree( recoveryCleanupWorkCollector, NO_HEADER_WRITER );
        state = State.ONLINE;
    }

    public State getState()
    {
        return state;
    }

    public synchronized void drop() throws IOException
    {
        try
        {
            closeWriter();
            schemaIndex.closeTree();
            schemaIndex.gbpTreeFileUtil.deleteFileIfPresent( indexFile );
        }
        finally
        {
            dropped = true;
            state = State.NONE; // ???
        }
    }

    public synchronized void create() throws IOException
    {
        assert state == State.INIT;
        // extra check here???
        schemaIndex.gbpTreeFileUtil.deleteFileIfPresent( indexFile ); /// TODO <- warning
        schemaIndex.instantiateTree( RecoveryCleanupWorkCollector.IMMEDIATE, new NativeSchemaIndexHeaderWriter( BYTE_POPULATING ) );
        instantiateWriter();
        workSync = new WorkSync<>( new IndexUpdateApply<>( treeKey, treeValue, singleTreeWriter, new ConflictDetectingValueMerger<>() ) );
        state = State.POPULATING;
    }

    public void close() throws IOException
    {
        schemaIndex.closeTree();
    }

    public synchronized void close( boolean populationCompletedSuccessfully ) throws IOException
    {
        assert state == State.POPULATING;
        closeWriter();
        if ( populationCompletedSuccessfully && failureBytes != null )
        {
            throw new IllegalStateException( "Can't mark index as online after it has been marked as failure" );
        }

        try
        {
            if ( populationCompletedSuccessfully )
            {
                assertPopulatorOpen();
                markTreeAsOnline();
                state = State.POPULATED;
            }
            else
            {
                assertNotDropped();
                ensureTreeInstantiated();
                markTreeAsFailed();
                state = State.FAILED;
            }
        }
        finally
        {
            schemaIndex.closeTree();
        }
    }

    public void add( Collection<IndexEntryUpdate<?>> updates ) throws IndexEntryConflictException, IOException
    {
        applyWithWorkSync( updates );
    }

    public void markAsFailed( String failure )
    {
        failureBytes = failure.getBytes( StandardCharsets.UTF_8 );
    }

    public void includeSample( IndexEntryUpdate<?> update )
    {
        if ( uniqueSampler != null )
        {
            uniqueSampler.increment( 1 );
        }
        else if ( generalSampler != null )
        {
            generalSampler.include( SamplingUtil.encodedStringValuesForSampling( (Object[]) update.values() ) );
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }

    public IndexSample sampleResult()
    {
        if ( uniqueSampler != null )
        {
            return uniqueSampler.result();
        }
        else if ( generalSampler != null )
        {
            // Close the writer before scanning
            try
            {
                closeWriter();
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }

            try
            {
                return generalSampler.result();
            }
            finally
            {
                try
                {
                    instantiateWriter();
                }
                catch ( IOException e )
                {
                    System.out.println( "GAAAAAH" );
                    throw new UncheckedIOException( e );
                }
            }
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }

    public IndexUpdater newPopulatingUpdater() throws IOException
    {
        return new IndexUpdater()
        {
            private boolean closed;
            private final Collection<IndexEntryUpdate<?>> updates = new ArrayList<>();

            @Override
            public void process( IndexEntryUpdate<?> update ) throws IOException, IndexEntryConflictException
            {
                assertOpen();
                updates.add( update );
            }

            @Override
            public void close() throws IOException, IndexEntryConflictException
            {
                applyWithWorkSync( updates );
                closed = true;
            }

            private void assertOpen()
            {
                if ( closed )
                {
                    throw new IllegalStateException( "Updater has been closed" );
                }
            }
        };
    }

    public IndexUpdater newUpdater()
    {
        schemaIndex.assertOpen();
        try
        {
            return singleUpdater.initialize( schemaIndex.tree.writer(), true );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    public ResourceIterator<File> snapshotFiles() throws IOException
    {
        return asResourceIterator( iterator( indexFile ) );
    }

    public void force() throws IOException
    {
        // TODO add IOLimiter arg
        schemaIndex.tree.checkpoint( IOLimiter.unlimited() );
    }

    public BoundedIterable<Long> newAllEntriesReader()
    {
        return new NativeAllEntriesReader<>( schemaIndex.tree, layout );
    }

    public IndexReader newReader( IndexSamplingConfig samplingConfig, IndexDescriptor descriptor )
    {
        schemaIndex.assertOpen();
        return new SpatialSchemaIndexReader<>( schemaIndex.tree, layout, samplingConfig, descriptor );
    }

    private void instantiateWriter() throws IOException
    {
        assert singleTreeWriter == null;
        singleTreeWriter = schemaIndex.tree.writer();
    }

    private void applyWithWorkSync( Collection<? extends IndexEntryUpdate<?>> updates ) throws IOException
    {
        try
        {
            workSync.apply( new IndexUpdateWork<>( updates ) );
        }
        catch ( ExecutionException e )
        {
            throw new IOException( e );
        }
    }

    private void assertPopulatorOpen()
    {
        if ( schemaIndex.tree == null )
        {
            throw new IllegalStateException( "Populator has already been closed." );
        }
    }

    private void closeWriter() throws IOException
    {
        singleTreeWriter = schemaIndex.closeIfPresent( singleTreeWriter );
    }

    private void markTreeAsOnline() throws IOException
    {
        schemaIndex.tree.checkpoint( IOLimiter.unlimited(), pc -> pc.putByte( BYTE_ONLINE ) );
    }

    private void markTreeAsFailed() throws IOException
    {
        if ( failureBytes == null )
        {
            failureBytes = new byte[0];
        }
        schemaIndex.tree.checkpoint( IOLimiter.unlimited(), new FailureHeaderWriter( failureBytes ) );
    }

    private void assertNotDropped()
    {
        if ( dropped )
        {
            throw new IllegalStateException( "Populator has already been dropped." );
        }
    }

    private void ensureTreeInstantiated() throws IOException
    {
        if ( schemaIndex.tree == null )
        {
            schemaIndex.instantiateTree( RecoveryCleanupWorkCollector.IGNORE, NO_HEADER_WRITER );
        }
    }

    public interface Factory
    {
        SpatialKnownIndex selectAndCreate( Map<CoordinateReferenceSystem,SpatialKnownIndex> indexMap, long indexId, Value value );

        SpatialKnownIndex selectAndCreate( Map<CoordinateReferenceSystem,SpatialKnownIndex> indexMap, long indexId,
                CoordinateReferenceSystem crs );
    }

    static Envelope envelopeFromCRS( CoordinateReferenceSystem crs )
    {
        Envelope curveEnvelope;
        //TODO: Support more CRS
        if ( crs.equals( WGS84 ) )
        {
            curveEnvelope = new Envelope( -180, 180, -90, 90 );
        }
        else
        {
            // TODO: support user configurable default envelope ranges (neo4j.conf or other)
            curveEnvelope = new Envelope( -1000000, 1000000, -1000000, 1000000 );
        }
        //TODO: support 3D
        return curveEnvelope;
    }

    private static String indexFileName( long indexId )
    {
        return "index-" + indexId;
    }

    public boolean indexExists()
    {
        return fs.fileExists( indexFile );
    }

    public String readPopulationFailure( IndexDescriptor descriptor ) throws IOException
    {
        NativeSchemaIndexHeaderReader headerReader = new NativeSchemaIndexHeaderReader();
        GBPTree.readHeader( pageCache, indexFile, layout( descriptor ), headerReader );
        return headerReader.failureMessage;
    }

    public InternalIndexState readState( IndexDescriptor descriptor ) throws IOException
    {
        NativeSchemaIndexHeaderReader headerReader = new NativeSchemaIndexHeaderReader();
        GBPTree.readHeader( pageCache, indexFile, layout( descriptor ), headerReader );
        switch ( headerReader.state )
        {
        case BYTE_FAILED:
            return InternalIndexState.FAILED;
        case BYTE_ONLINE:
            return InternalIndexState.ONLINE;
        case BYTE_POPULATING:
            return InternalIndexState.POPULATING;
        default:
            throw new IllegalStateException( "Unexpected initial state byte value " + headerReader.state );
        }
    }

    private SpatialLayout layout( IndexDescriptor descriptor )
    {
        SpatialLayout layout;
        if ( uniqueSampler( descriptor ) )
        {
            layout = new SpatialLayoutUnique( crs, curve );
        }
        else
        {
            layout = new SpatialLayoutNonUnique( crs, curve );
        }
        return layout;
    }

    private boolean uniqueSampler( IndexDescriptor descriptor )
    {
        switch ( descriptor.type() )
        {
        case GENERAL:
            return false;
        case UNIQUE:
            return true;
        default:
            throw new UnsupportedOperationException( "Unexpected index type " + descriptor.type() );
        }
    }

    public enum State
    {
        NONE,
        INIT,
        POPULATING,
        POPULATED,
        ONLINE,
        FAILED
    }
}
