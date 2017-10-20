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

import static org.alfresco.solr.AlfrescoSolrUtils.getNode;
import static org.alfresco.solr.AlfrescoSolrUtils.getNodeMetaData;
import static org.alfresco.solr.AlfrescoSolrUtils.getTransaction;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import org.alfresco.model.ContentModel;
import org.alfresco.solr.client.Node;
import org.alfresco.solr.client.NodeMetaData;
import org.alfresco.solr.client.StringPropertyValue;
import org.alfresco.solr.client.Transaction;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.io.Tuple;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Tuna Aksoy
 */
@SolrTestCaseJ4.SuppressSSL
@LuceneTestCase.SuppressCodecs({"Appending","Lucene3x","Lucene40","Lucene41","Lucene42","Lucene43", "Lucene44", "Lucene45","Lucene46","Lucene47","Lucene48","Lucene49"})
public class DistributedSqlTimeSeriesTest2 extends AbstractStreamTest
{
    @Rule
    public JettyServerRule jetty = new JettyServerRule(1, this);

    //private int numDocs = 12;
    private int numDocs = 720;
    private Transaction txn = getTransaction(0, numDocs);
    private List<Node> nodes = new ArrayList<>();
    private List<NodeMetaData> nodeMetaDatas = new ArrayList<>();

    @Test
    public void testSearch() throws Exception
    {
        String sql = "select cm_created_day, count(*) from alfresco where cm_created >= '2017-01-01T00:00:00Z' and cm_created <= '2017-01-05T23:59:59Z' group by cm_created_day";
        String alfrescoJson = "{ \"authorities\": [ \"jim\", \"joel\" ], \"tenants\": [ \"\" ] }";
        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);
        int size = tuples.size();
    }

    @Before
    private void createData() throws Exception
    {
        TimeZone.setDefault(TimeZone.getTimeZone(DateTimeZone.UTC.getID()));

        int currentYear = LocalDateTime.now().getYear();
        /*
        for (int i = 0; i < numDocs; i++)
        {
            //updateProperties(LocalDateTime.of(currentYear, 10, 20, i, 0, 0));
            updateProperties(LocalDateTime.of(currentYear, 1, i, 0, 0, 0));
            updateProperties(LocalDateTime.of(currentYear + 1, i, 1, 0, 0, 0));
            updateProperties(LocalDateTime.of(currentYear - i, 1, 1, 0, 0, 0));
        }
        */

        for (int i = 1; i <= 30; i++)
        {
            for (int j = 0; j < 24; j++)
            {
                updateProperties(LocalDateTime.of(currentYear, 10, i, j, 0, 0));
            }
        }

        indexTransaction(txn, nodes, nodeMetaDatas);
        waitForDocCount(new TermQuery(new Term("content@s___t@{http://www.alfresco.org/model/content/1.0}content", "world")), numDocs + 4, 80000);
    }

    private void updateProperties(LocalDateTime localDateTime)
    {
        Node node = getNode(txn, acl, Node.SolrApiNodeStatus.UPDATED);
        nodes.add(node);
        NodeMetaData nodeMetaData = getNodeMetaData(node, txn, acl, "mike", null, false);

        nodeMetaData.getProperties().put(ContentModel.PROP_CREATED, new StringPropertyValue(localDateTime.toInstant(ZoneOffset.UTC).toString()));
        nodeMetaData.getProperties().put(ContentModel.PROP_NAME, new StringPropertyValue("name1"));
        nodeMetaData.getProperties().put(ContentModel.PROP_TITLE, new StringPropertyValue("title1"));
        nodeMetaData.getProperties().put(ContentModel.PROP_CREATOR, new StringPropertyValue("creator1"));
        nodeMetaData.getProperties().put(ContentModel.PROP_OWNER, new StringPropertyValue("jim"));

        nodeMetaDatas.add(nodeMetaData);
    }
}
