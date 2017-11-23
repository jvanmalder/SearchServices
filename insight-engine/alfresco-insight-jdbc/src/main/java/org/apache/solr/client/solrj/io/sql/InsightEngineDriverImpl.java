/*
 * Copyright (C) 2005-2017 Alfresco Software Limited.
 *
 * This file is part of Alfresco
 *
 * Alfresco is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Alfresco is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with Alfresco. If not, see <http://www.gnu.org/licenses/>.
 */
package org.apache.solr.client.solrj.io.sql;

import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * A Jdbc driver class for the Insight Engine
 */
public class InsightEngineDriverImpl extends DriverImpl
{
    static {
        try {
            DriverManager.registerDriver(new InsightEngineDriverImpl());
        } catch (SQLException e) {
            throw new RuntimeException("Can't register insight driver!", e);
        }
    }

    @Override
    public boolean acceptsURL(String url)
    {
        return url != null && url.startsWith("jdbc:alfresco");
    }
}
