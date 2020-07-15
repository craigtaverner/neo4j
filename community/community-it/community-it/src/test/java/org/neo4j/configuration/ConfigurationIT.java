/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.configuration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.IOException;
import java.util.Map;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.server.helpers.CommunityWebContainerBuilder;
import org.neo4j.server.helpers.TestWebContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.STRING;

public class ConfigurationIT
{
    @Test
    @DisabledOnOs( OS.WINDOWS )
    void shouldBeAbleToEvaluateSettingFromWebServer() throws IOException
    {
        TestWebContainer testWebContainer = CommunityWebContainerBuilder.serverOnRandomPorts().build();

        try
        {
            //Given
            Config config = Config.newBuilder().allowCommandExpansion().addSettingsClass( TestSettings.class )
                    .setRaw( Map.of( TestSettings.stringSetting.name(), "$(curl -I '" + testWebContainer.getBaseUri() + "')" ) ).build();
            //Then
            assertThat( config.get( TestSettings.stringSetting ) ).contains( "200 OK" );
        }
        finally
        {
            testWebContainer.shutdown();
        }
    }

    private static final class TestSettings implements SettingsDeclaration
    {
        static final Setting<String> stringSetting = newBuilder( "test.setting.string", STRING, "" ).build();
    }
}
