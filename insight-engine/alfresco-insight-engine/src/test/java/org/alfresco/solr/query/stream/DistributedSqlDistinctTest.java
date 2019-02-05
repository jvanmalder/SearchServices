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

import java.util.List;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.io.Tuple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Michael Suzuki
 */
@SolrTestCaseJ4.SuppressSSL
@LuceneTestCase.SuppressCodecs({"Appending","Lucene3x","Lucene40","Lucene41","Lucene42","Lucene43", "Lucene44", "Lucene45","Lucene46","Lucene47","Lucene48","Lucene49"})
public class DistributedSqlDistinctTest extends AbstractStreamTest
{
    private String alfrescoJson = "{ \"authorities\": [ \"jim\", \"joel\" ], \"tenants\": [ \"\" ] }";

    @BeforeClass
    private static void initData() throws Throwable
    {
        initSolrServers(1, getClassName(), null);
    }

    @AfterClass
    private static void destroyData()
    {
        dismissSolrServers();
    }

    @Test
    public void testSearch() throws Exception
    {
        List<Tuple> tuples = sqlQuery("select distinct ACLID from alfresco where `cm:content` = 'world' limit 10", alfrescoJson);
        assertEquals(2, tuples.size());
        assertFalse(tuples.get(0).get("ACLID").toString().equals(tuples.get(1).get("ACLID").toString()));

        tuples = sqlQuery("select distinct ACLID from alfresco", alfrescoJson);
        assertEquals(2, tuples.size());
        
        tuples = sqlQuery("select distinct ACLID,DBID from alfresco where `cm:content` = 'world' limit 10 ", alfrescoJson);
        assertEquals(4, tuples.size());
        
        tuples = sqlQuery("select distinct cm_name from alfresco where cm_content = 'world'", alfrescoJson);
        assertEquals(3, tuples.size());

        tuples = sqlQuery("select distinct cm_name from alfresco where cm_content = 'hello world'", alfrescoJson);
        assertEquals(3, tuples.size());

        tuples = sqlQuery("select distinct cm_name from alfresco where cm_content = 'world hello'", alfrescoJson);
        assertEquals(0, tuples.size());

        tuples = sqlQuery("select distinct `cm:name` from alfresco where `cm:content` = 'world' limit 1", alfrescoJson);
        assertEquals(1, tuples.size());
        assertEquals("name1", tuples.get(0).getString(("cm:name")));

        tuples = sqlQuery("select distinct cm_name from alfresco where cm_content = 'world'", alfrescoJson);
        assertEquals(3, tuples.size());

        tuples = sqlQuery("select distinct cm_title from alfresco", alfrescoJson);
        assertEquals(2, tuples.size());
        
        tuples = sqlQuery("select distinct cm_creator from alfresco where `cm:content` = 'world'", alfrescoJson);
        assertEquals(3, tuples.size());
        
        tuples = sqlQuery("select distinct owner from alfresco where cm_content = 'world'", alfrescoJson);
        assertEquals(1, tuples.size());
    }
}