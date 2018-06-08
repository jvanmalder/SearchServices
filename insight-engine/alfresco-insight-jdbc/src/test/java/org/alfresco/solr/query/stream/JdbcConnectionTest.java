/*
 * Copyright (C) 2005-2018 Alfresco Software Limited.
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
package org.alfresco.solr.query.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.Test;

/**
 * Test to ensure we capture bad connection error and not display user credentials.
 * @author Michael Suzuki
 */
public class JdbcConnectionTest 
{
    @AfterClass
    public static void teardownJDBC()
    {
        System.clearProperty("org.alfresco.search.jdbc.direct");
    }

    @Test
    public void testSearch() throws Exception
    {
        String sql = "select DBID, LID from alfresco where cm_content = 'world' order by DBID limit 10 ";
        String alfrescoJson = "{ \"authorities\": [ \"jim\", \"joel\" ], \"tenants\": [ \"\" ] }";

        Properties props = getConnectionProperties(alfrescoJson);
        String connectionString ="jdbc:alfresco://fakeurl?collection=alfresco";
        Connection con = null;
        Statement stmt = null;
        try 
        {
            con = DriverManager.getConnection(connectionString, props);
            stmt = con.createStatement();
            stmt.executeQuery(sql);
        }
        catch (Exception e)
        {
            assertNotNull(e);
            assertEquals("Unable to execute the query, please check your settings", e.getMessage());
        }
        finally
        {
            stmt.close();
            con.close();
        }
    }

    private Properties getConnectionProperties(String json) 
    {
        Properties props = new Properties();
        props.put("json", json);
        //Add the basicauth username and passwords required by test framework
        props.put("user", "test");
        props.put("password", "pass");
        return props;
    }

}

