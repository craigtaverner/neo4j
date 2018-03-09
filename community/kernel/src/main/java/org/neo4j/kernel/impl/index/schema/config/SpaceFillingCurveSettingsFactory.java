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
package org.neo4j.kernel.impl.index.schema.config;

import java.util.HashMap;

import org.neo4j.gis.spatial.index.Envelope;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.values.storable.CoordinateReferenceSystem;

/**
 * These settings affect the creation of the 2D (or 3D) to 1D mapper.
 * Changing these will change the values of the 1D mapping, and require re-indexing, so
 * once data has been indexed, do not change these without recreating the index.
 */
public class SpaceFillingCurveSettingsFactory
{
    private int maxBits;
    private HashMap<CoordinateReferenceSystem,SpaceFillingCurveSettings> settings = new HashMap<>();
    private static final double DEFAULT_MIN_EXTENT = -1000000;
    private static final double DEFAULT_MAX_EXTENT = 1000000;
    private static final double DEFAULT_MIN_LATITUDE = -90;
    private static final double DEFAULT_MAX_LATITUDE = 90;
    private static final double DEFAULT_MIN_LONGITUDE = -180;
    private static final double DEFAULT_MAX_LONGITUDE = 180;

    public SpaceFillingCurveSettingsFactory( Config config )
    {
        this.maxBits = config.get( SpatialIndexSettings.space_filling_curve_max_bits );
    }

    /**
     * The space filling curve is configured up front to cover a specific region of 2D (or 3D) space,
     * and the mapping tree is configured up front to have a specific maximum depth. These settings
     * are stored in an instance of SpaceFillingCurveSettings and are determined by the Coordinate
     * Reference System, and any neo4j.conf settings to override the CRS defaults.
     *
     * @return The settings for the specified coordinate reference system
     */
    public SpaceFillingCurveSettings settingsFor( CoordinateReferenceSystem crs )
    {
        return new SpaceFillingCurveSettings( crs.getDimension(), maxBits, envelopeFromCRS( crs.getDimension(), crs.isGeographic() ) );
    }

    private static Envelope envelopeFromCRS( int dimension, boolean geographic )
    {
        assert dimension >= 2;
        double[] min = new double[dimension];
        double[] max = new double[dimension];
        int cartesianStartIndex = 0;
        if ( geographic )
        {
            // Geographic CRS default to extent of the earth in degrees
            min[0] = DEFAULT_MIN_LONGITUDE;
            max[0] = DEFAULT_MAX_LONGITUDE;
            min[1] = DEFAULT_MIN_LATITUDE;
            max[1] = DEFAULT_MAX_LATITUDE;
            cartesianStartIndex = 2;    // if geographic index has higher than 2D, then other dimensions are cartesian
        }
        for ( int i = cartesianStartIndex; i < dimension; i++ )
        {
            min[i] = DEFAULT_MIN_EXTENT;
            max[i] = DEFAULT_MAX_EXTENT;
        }
        return new Envelope( min, max );
    }
}
