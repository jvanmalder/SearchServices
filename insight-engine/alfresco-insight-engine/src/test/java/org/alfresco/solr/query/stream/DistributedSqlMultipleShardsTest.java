/*
 * Copyright (C) 2005-2019 Alfresco Software Limited.
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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.io.Tuple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Elia
 */
@SolrTestCaseJ4.SuppressSSL
@LuceneTestCase.SuppressCodecs({"Appending","Lucene3x","Lucene40","Lucene41","Lucene42","Lucene43", "Lucene44", "Lucene45","Lucene46","Lucene47","Lucene48","Lucene49"})
public class DistributedSqlMultipleShardsTest extends AbstractStreamTest
{

    private String alfrescoJson = "{ \"authorities\": [ \"jim\", \"joel\" ], \"tenants\": [ \"\" ] }";

    /**
     * The following tests should verify the correctness of query executed on a shard where the requested fields are not actually indexed.
     * For this reason, the environment is started with 4 shards, in order to possibly have one node indexed on each shard.
     *
     * @throws Throwable
     */
    @BeforeClass
    public static void initData() throws Throwable
    {
        initSolrServers(4, getClassName(), null);
        JettySolrRunner localJetty = jettyContainers.values().iterator().next();
        System.setProperty("solr.solr.home", localJetty.getSolrHome());
    }

    @AfterClass
    public static void destroyData()
    {
        dismissSolrServers();
    }

    /**
     * SEARCH-903
     * It search on multiple shards for custom properties. The search is performed on one shard where
     * or at least one field between finance:Title and finance:Location is not set.
     * @throws Exception
     */
    @Test
    public void distributedSearch_customModelField_multipleShard() throws Exception
    {
        Set<String> expectedColumns = new HashSet<>(Arrays.asList("finance_Title","finance_location"));
        String sql = "select finance_Title, finance_location from alfresco";
        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);
        assertEquals(indexedNodesCount, tuples.size());
        for (Tuple t : tuples)
        {
            assertEquals("Mismatched columns", expectedColumns, t.fields.keySet());
        }
    }

    /**
     * Check that the query does not fail if select statement contains fields specified in a loaded model
     * but not indexed in any shard.
     * @throws Exception
     */
    @Test
    public void distributedSearch_customModelField_notIndexed() throws Exception
    {
        Set<String> expectedColumns = new HashSet<>(Arrays.asList("finance_No"));
        String sql = "select finance_No from alfresco";
        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);
        assertEquals(indexedNodesCount, tuples.size());
        for (Tuple t : tuples)
        {
            assertEquals("Mismatched columns", expectedColumns, t.fields.keySet());
        }
    }

}

