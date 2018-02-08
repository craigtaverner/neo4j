package org.neo4j.consistency;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;

public class RepairStore
{
    @Test
    public void checkStore()
    {
        File dbDir = new File( "/Users/craig/dev/neo4j/ronja-benchmarks/musicbrainz-2.3/graph.db" );
        ConsistencyCheckService consistencyCheckService = new ConsistencyCheckService();
        String[] args = {dbDir.getPath()};
        try
        {
            new ConsistencyCheckTool( consistencyCheckService, new GraphDatabaseFactory(),
                    new DefaultFileSystemAbstraction(), System.err,
                    org.neo4j.legacy.consistency.ConsistencyCheckTool.ExitHandle.SYSTEM_EXIT )
                    .run( args );
        }
        catch ( ConsistencyCheckTool.ToolFailureException e )
        {
            System.err.println( "Failed to run consistency check tool: " + e.getMessage() );
            e.printStackTrace();
        }
        catch ( IOException e )
        {
            System.err.println( "Failed to run consistency check: " + e.getMessage() );
            e.printStackTrace();
        }
    }

    @Test
    public void fixStore()
    {
        int blockSize = 10;
        File dbDir = new File( "/Users/craig/dev/neo4j/ronja-benchmarks/musicbrainz-2.3/graph.db" );
        GraphDatabaseService graph = new GraphDatabaseFactory().newEmbeddedDatabase( dbDir );
        LinkedHashMap<Long,int[]> badNodes = readBadNodesFromDatabase( graph, blockSize );
        if ( badNodes.size() > 0 )
        {
            clearLabelsOfBadNodesInDatabase( graph, badNodes );
            clearRemainingLabelsOfBadNodesInDatabase( graph, badNodes.keySet() );
            addLabelsOfBadNodesInDatabase( graph, badNodes );
        }
        else
        {
            System.out.println( "No bad nodes found" );
        }
    }

    @Test
    public void fixStoreWithNodeIds() throws IOException
    {
        File dbDir = new File( "/Users/craig/dev/neo4j/ronja-benchmarks/musicbrainz-2.3/graph.db" );
        LinkedHashMap<Long,int[]> badNodes = readBadNodesFromFile( new File( dbDir.getParentFile(), "bad_nods.txt" ) );
        if ( badNodes.size() > 0 )
        {
            GraphDatabaseService graph = new GraphDatabaseFactory().newEmbeddedDatabase( dbDir );
            clearLabelsOfBadNodesInDatabase( graph, badNodes );
            clearRemainingLabelsOfBadNodesInDatabase( graph, badNodes.keySet() );
            addLabelsOfBadNodesInDatabase( graph, badNodes );
        }
    }

    private LinkedHashMap<Long,int[]> readBadNodesFromFile( File nodesFile )
    {
        LinkedHashMap<Long,int[]> badNodes = new LinkedHashMap<>();
        try
        {
            BufferedReader reader = new BufferedReader(
                    new FileReader( "/Users/craig/dev/neo4j/ronja-benchmarks/musicbrainz-2.3/bad_nodes.txt" ) );
            String line;
            while ( (line = reader.readLine()) != null )
            {
                String[] fields = line.split( "[\\s\\,]+" );
                long nodeId = Long.parseLong( fields[0] );
                int[] labels = new int[fields.length - 1];
                for ( int i = 0; i < labels.length; i++ )
                {
                    labels[i] = Integer.parseInt( fields[i + 1] );
                }
                badNodes.put( nodeId, labels );
            }
            reader.close();
        }
        catch ( IOException e )
        {
            System.err.println( "Failed to read bad nodes: " + e.getMessage() );
        }
        return badNodes;
    }

    private void clearLabelsOfBadNodesInDatabase( GraphDatabaseService graph, LinkedHashMap<Long,int[]> badNodes )
    {
        ArrayList<Integer> labels = new ArrayList<>( 10 );
        System.out.println( "Deleting labels from " + badNodes.size() + " bad nodes" );
        try ( Transaction tx = graph.beginTx() )
        {
            Statement statement = ((GraphDatabaseAPI) graph).getDependencyResolver()
                    .resolveDependency( ThreadToStatementContextBridge.class ).get();
            for ( Map.Entry<Long,int[]> entry : badNodes.entrySet() )
            {
                long id = entry.getKey();
                int[] badLabels = entry.getValue();
                try
                {
                    for ( int i = badLabels.length - 1; i >= 0; i-- )
                    {
                        int label = badLabels[i];
                        System.out.println( "\tRemoving label[" + label + "] from node[" + id + "]" );
                        statement.dataWriteOperations().nodeRemoveLabel( id, label );
                    }
                }
                catch ( Exception e )
                {
                    System.err.println( "Failed to remove labels from node[" + id + "]: " + e.getMessage() );
                }
            }
            tx.success();
        }
    }

    private void clearRemainingLabelsOfBadNodesInDatabase( GraphDatabaseService graph, Set<Long> badNodes )
    {
        ArrayList<Integer> labels = new ArrayList<>( 10 );
        System.out.println( "Deleting remaining labels from " + badNodes.size() + " bad nodes" );
        try ( Transaction tx = graph.beginTx() )
        {
            Statement statement = ((GraphDatabaseAPI) graph).getDependencyResolver()
                    .resolveDependency( ThreadToStatementContextBridge.class ).get();
            for ( long id : badNodes )
            {
                try
                {
                    labels.clear();
                    PrimitiveIntIterator labelIterator = statement.readOperations().nodeGetLabels( id );
                    while ( labelIterator.hasNext() )
                    {
                        int current = labelIterator.next();
                        labels.add( current );
                    }
                    for ( int label : labels )
                    {
                        System.out.println( "\tRemoving label[" + label + "] from node[" + id + "]" );
                        statement.dataWriteOperations().nodeRemoveLabel( id, label );
                    }
                }
                catch ( Exception e )
                {
                    System.err.println( "Failed to remove labels from node[" + id + "]: " + e.getMessage() );
                }
            }
            tx.success();
        }
    }

    public void addLabelsOfBadNodesInDatabase( GraphDatabaseService graph, LinkedHashMap<Long,int[]> badNodes )
    {
        ArrayList<Integer> labels = new ArrayList<>( 10 );
        System.out.println( "Adding back labels to " + badNodes.size() + " bad nodes" );
        try ( Transaction tx = graph.beginTx() )
        {
            Statement statement = ((GraphDatabaseAPI) graph).getDependencyResolver()
                    .resolveDependency( ThreadToStatementContextBridge.class ).get();
            for ( Map.Entry<Long,int[]> entry : badNodes.entrySet() )
            {
                long id = entry.getKey();
                int[] badLabels = entry.getValue();
                labels.clear();
                for ( int i = 0; i < badLabels.length; i++ )
                {
                    int label = badLabels[i];
                    if ( !labels.contains( label ) )
                    {
                        labels.add( label );
                    }
                }
                int[] fixedLabels = new int[labels.size()];
                for ( int i = 0; i < labels.size(); i++ )
                {
                    fixedLabels[i] = labels.get( i );
                }
                Arrays.sort( fixedLabels );
                try
                {
                    for ( int label : fixedLabels )
                    {
                        System.out.println( "\tAdding label[" + label + "] to node[" + id + "]" );
                        statement.dataWriteOperations().nodeAddLabel( id, label );
                    }
                }
                catch ( Exception e )
                {
                    System.err.println( "Failed to add labels to node[" + id + "]: " + e.getMessage() );
                }
            }
            tx.success();
        }
    }

    private LinkedHashMap<Long,int[]> readBadNodesFromDatabase( GraphDatabaseService graph, int blockSize )
    {
        LinkedHashMap<Long,int[]> badNodes = new LinkedHashMap<>();
        ArrayList<Integer> labels = new ArrayList<>( 10 );
        try ( Transaction tx = graph.beginTx() )
        {
            Statement statement = ((GraphDatabaseAPI) graph).getDependencyResolver()
                    .resolveDependency( ThreadToStatementContextBridge.class ).get();
            PrimitiveLongIterator nodes = statement.readOperations().nodesGetAll();
            long previousTime = System.currentTimeMillis();
            while ( nodes.hasNext() )
            {
                long id = nodes.next();
                long currentTime = System.currentTimeMillis();
                if ( currentTime - previousTime > 2000 )
                {
                    System.out.println( "Processing node: " + id );
                    previousTime = currentTime;
                }
                if ( id == 833533 || id == 833813 || id == 833997 )
                {
                    // Nodes mis-ordered labels: [17, 9]
                    System.out.println( "Should see misordered labels for node: " + id );
                }
                try
                {
                    int previous = -1;
                    boolean misordered = false;
                    boolean duplicated = false;
                    labels.clear();
                    PrimitiveIntIterator labelIterator = statement.readOperations().nodeGetLabels( id );
                    while ( labelIterator.hasNext() )
                    {
                        int current = labelIterator.next();
                        if ( current < previous )
                        {
                            misordered = true;
                        }
                        if ( labels.contains( current ) )
                        {
                            duplicated = true;
                        }
                        labels.add( current );
                    }
                    if ( duplicated || misordered )
                    {
                        System.out.println( "Found bad labels on node[" + id + "]: " + labels );
                        int[] badLabels = new int[labels.size()];
                        for ( int i = 0; i < labels.size(); i++ )
                        {
                            badLabels[i] = labels.get( i );
                        }
                        badNodes.put( id, badLabels );
                    }
                    if ( id == 47837 || id == 72351 || id == 144406 )
                    {
                        // Nodes with duplicate labels: [11, 12, 12, 122]
                        System.out.println( "Should see duplicate labels for node: " + id );
                        System.out.println( "\t" + labels );
                        for ( int labelId : labels )
                        {
                            String labelName = statement.readOperations().labelGetName( labelId );
                            System.out.println( "\t" + labelId + ":\t" + labelName );
                        }
                    }
                }
                catch ( EntityNotFoundException e )
                {
                    System.out.println( "Error: " + e.getMessage() );
                    e.printStackTrace();
                }
                catch ( LabelNotFoundKernelException e )
                {
                    System.out.println( "Error: " + e.getMessage() );
                    e.printStackTrace();
                }
                if ( badNodes.size() > blockSize )
                {
                    System.out.println( "Found blocksize[" + blockSize + "] bad nodes - moving on" );
                    break;
                }
            }
            tx.success();
        }
        return badNodes;
    }

}
