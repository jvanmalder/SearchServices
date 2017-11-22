/*
 * Copyright (C) 2005-2014 Alfresco Software Limited.
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

import java.sql.DriverManager;
import java.util.List;
import java.util.Properties;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Joel
 */
@SolrTestCaseJ4.SuppressSSL
@LuceneTestCase.SuppressCodecs({"Appending","Lucene3x","Lucene40","Lucene41","Lucene42","Lucene43", "Lucene44", "Lucene45","Lucene46","Lucene47","Lucene48","Lucene49"})
public class DistributedJdbcTest extends AbstractStreamTest
{
    @Rule
    public JettyServerRule jetty = new JettyServerRule(1, this);

    @Test
    public void testSearch() throws Exception
    {
        String sql = "select DBID, LID from alfresco where cm_content = 'world' order by DBID limit 10 ";
        String alfrescoJson = "{ \"authorities\": [ \"jim\", \"joel\" ], \"tenants\": [ \"\" ] }";

        Properties props = getConnectionProperties(alfrescoJson);
        String connectionString = getConnectionString();
        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            con = DriverManager.getConnection(connectionString, props);
            stmt = con.createStatement();
            rs = stmt.executeQuery(sql);
            int i=0;
            while (rs.next()) {
                ++i;
                assertNotNull(rs.getString("DBID"));
            }
            assertEquals(i, 4);
        } finally {
            rs.close();
            stmt.close();
            con.close();
        }
    }

    private String getConnectionString() {
        List<SolrClient> clusterClients = getClusterClients();
        String baseUrl = ((HttpSolrClient) clusterClients.get(0)).getBaseURL();
        String[] parts = baseUrl.split("://");
        String uri = parts[1];
        String[] path = uri.split("/");
        return "jdbc:alfresco://"+path[0]+"?collection="+path[2];
    }

    private Properties getConnectionProperties(String json) {
        List<SolrClient> clusterClients = getClusterClients();
        String shards = getShardsString(clusterClients);
        Properties props = new Properties();
        props.put("json", json);
        props.put("alfresco.shards", shards);
        return props;
    }

}

