/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.bolt.v1.transport.integration;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import org.neo4j.bolt.BoltKernelExtension;
import org.neo4j.bolt.v1.transport.socket.client.Connection;
import org.neo4j.bolt.v1.transport.socket.client.SecureSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SecureWebSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.WebSocketConnection;
import org.neo4j.function.Factory;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.api.exceptions.Status;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.BoltKernelExtension.EncryptionLevel.REQUIRED;
import static org.neo4j.bolt.BoltKernelExtension.Settings.connector;
import static org.neo4j.bolt.v1.messaging.message.Messages.init;
import static org.neo4j.bolt.v1.messaging.message.Messages.pullAll;
import static org.neo4j.bolt.v1.messaging.message.Messages.reset;
import static org.neo4j.bolt.v1.messaging.message.Messages.run;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgFailure;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgRecord;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.v1.runtime.spi.StreamMatchers.eqRecord;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyRecieves;
import static org.neo4j.helpers.collection.MapUtil.map;

@RunWith(Parameterized.class)
public class LoginSessionIT
{
    @Rule
    public Neo4jWithSocket server = new Neo4jWithSocket(
            settings -> {
                settings.put( connector(0, BoltKernelExtension.Settings.enabled), "true");
                settings.put( connector(0, BoltKernelExtension.Settings.socket_address), "localhost:7878");
                settings.put( connector(0, BoltKernelExtension.Settings.tls_level), REQUIRED.name() );
            } );

    @Parameterized.Parameter(0)
    public Factory<Connection> cf;

    @Parameterized.Parameter(1)
    public HostnamePort address;

    private Connection client;

    @Parameterized.Parameters
    public static Collection<Object[]> transports()
    {
        return asList(
//                new Object[]{
//                        (Factory<Connection>) SocketConnection::new,
//                        new HostnamePort( "localhost:7687" )
//                },
//                new Object[]{
//                        (Factory<Connection>) WebSocketConnection::new,
//                        new HostnamePort( "localhost:7687" )
//                },
                new Object[]{
                        (Factory<Connection>) SecureSocketConnection::new,
                        new HostnamePort( "localhost:7878" )
                },
                new Object[]{
                        (Factory<Connection>) SecureWebSocketConnection::new,
                        new HostnamePort( "localhost:7878" )
                } );
    }

    @Test
    public void shouldRunSimpleStatementWithLogin() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1", "Login/neo4j:neo4j" ),
                        run( "UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared" ),
                        pullAll() ) );

        // Then
        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyRecieves(
                msgSuccess(),
                msgSuccess( map( "fields", asList( "a", "a_squared" ) ) ),
                msgRecord( eqRecord( equalTo( 1l ), equalTo( 1l ) ) ),
                msgRecord( eqRecord( equalTo( 2l ), equalTo( 4l ) ) ),
                msgRecord( eqRecord( equalTo( 3l ), equalTo( 9l ) ) ),
                msgSuccess() ) );
    }

    @Test
    public void shouldFailToRunSimpleStatementWithIncorrectLogin() throws Throwable
    {
        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1", "login/neo4j:bad" ),
                        run( "UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared" ),
                        pullAll() ) );

        // Then
        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyRecieves(
                msgFailure( Status.Request.Invalid, "Failed to authenticate: The credentials provided for account " +
                                                    "[org.apache.shiro.authc.UsernamePasswordToken - neo4j, " +
                                                    "rememberMe=false] did not match the expected credentials." ),
                msgFailure( Status.Request.Invalid, "Not authenticated" ) ) );
    }

    @Before
    public void setup()
    {
        this.client = cf.newInstance();
    }

    @After
    public void teardown() throws Exception
    {
        if ( client != null )
        {
            client.disconnect();
        }
    }
}
