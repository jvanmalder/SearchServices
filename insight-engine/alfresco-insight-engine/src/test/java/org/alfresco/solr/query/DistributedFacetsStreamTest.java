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
package org.alfresco.solr.query;

import static java.util.Collections.singletonList;
import static org.alfresco.solr.AlfrescoSolrUtils.getAclChangeSet;
import static org.alfresco.solr.AlfrescoSolrUtils.getAcl;
import static org.alfresco.solr.AlfrescoSolrUtils.getTransaction;
import static org.alfresco.solr.AlfrescoSolrUtils.indexAclChangeSet;
import static org.alfresco.solr.AlfrescoSolrUtils.getAclReaders;
import static org.alfresco.solr.AlfrescoSolrUtils.getNode;
import static org.alfresco.solr.AlfrescoSolrUtils.getNodeMetaData;

import org.alfresco.model.ContentModel;
import org.alfresco.repo.search.adaptor.lucene.QueryConstants;
import org.alfresco.service.namespace.NamespaceService;
import org.alfresco.service.namespace.QName;
import org.alfresco.solr.AbstractAlfrescoDistributedTest;
import org.alfresco.solr.client.*;
import org.alfresco.solr.stream.AlfrescoSolrStream;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.LegacyNumericRangeQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.common.params.SolrParams;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

@SolrTestCaseJ4.SuppressSSL
@LuceneTestCase.SuppressCodecs({"Appending","Lucene3x","Lucene40","Lucene41","Lucene42","Lucene43", "Lucene44", "Lucene45","Lucene46","Lucene47","Lucene48","Lucene49"})
public class DistributedFacetsStreamTest extends AbstractAlfrescoDistributedTest
{
    private static final QName PROP_RATING = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "fiveStarRatingSchemeTotal");
    private static final QName PROP_TRACK  = QName.createQName(NamespaceService.AUDIO_MODEL_1_0_URI, "trackNumber");

    @BeforeClass
    private static void initData() throws Throwable
    {
        initSolrServers(2, getClassName(), null);
    }

    @AfterClass
    private static void destroyData()
    {
        dismissSolrServers();
    }

    @After
    public void clearData() throws Exception
    {
        deleteByQueryAllClients("*:*");
    }

    @Test
    public void testFacets() throws Exception
    {

        AclChangeSet aclChangeSet = getAclChangeSet(1);

        Acl acl = getAcl(aclChangeSet);

        AclReaders aclReaders = getAclReaders(aclChangeSet, acl, singletonList("joel"), singletonList("phil"), null);

        indexAclChangeSet(aclChangeSet,
                singletonList(acl),
                singletonList(aclReaders));

        //Check for the ACL state stamp.
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new BooleanClause(new TermQuery(new Term(QueryConstants.FIELD_SOLR4_ID, "TRACKER!STATE!ACLTX")), BooleanClause.Occur.MUST));
        builder.add(new BooleanClause(LegacyNumericRangeQuery.newLongRange(QueryConstants.FIELD_S_ACLTXID, aclChangeSet.getId(), aclChangeSet.getId() + 1, true, false), BooleanClause.Occur.MUST));
        BooleanQuery waitForQuery = builder.build();
        waitForDocCountAllCores(waitForQuery, 1, 80000);

        /*
        * Create and index a Transaction
        */

        //First create a transaction.
        int numNodes = 10;
        List<Node> nodes = new ArrayList<>();
        List<NodeMetaData> nodeMetaDatas = new ArrayList<>();

        Transaction bigTxn = getTransaction(0, numNodes);

        for (int i = 0; i < numNodes; i++)
        {
            Node node = getNode(bigTxn, acl, Node.SolrApiNodeStatus.UPDATED);
            nodes.add(node);
            NodeMetaData nodeMetaData = getNodeMetaData(node, bigTxn, acl, "mike", null, false);
            nodeMetaData.getProperties().put(ContentModel.PROP_TITLE, new StringPropertyValue("statsworld"));
            nodeMetaData.getProperties().put(PROP_RATING, new StringPropertyValue("" + (i + 1)));
            int trackNum = 2 + (i % 3) * 2;
            nodeMetaData.getProperties().put(PROP_TRACK, new StringPropertyValue("" + trackNum));
            nodeMetaDatas.add(nodeMetaData);
        }

        indexTransaction(bigTxn, nodes, nodeMetaDatas);
        waitForDocCount(new TermQuery(new Term("content@s___t@{http://www.alfresco.org/model/content/1.0}content", "world")), numNodes, 100000);

        putHandleDefaults();

        List<SolrClient> clusterClients = getShardedClients();
        String shards = getShardsString();

        String alfrescoJson = "{ \"authorities\": [ \"mike\"], \"tenants\": [ \"\" ] }";
        String expr = "alfrescoFacets(facet("
                + "myCollection, "
                + "q=\"*.*\", "
                + "buckets=\"cm:title\", "
                + "bucketSorts=\"cm:title desc\", "
                + "bucketSizeLimit=100, "
                + "count(*)"
                + "))";
        SolrParams params = params("expr", expr, "qt", "/stream", "myCollection.shards", shards);

        AlfrescoSolrStream tupleStream = new AlfrescoSolrStream(((HttpSolrClient) clusterClients.get(0)).getBaseURL(), params);
        tupleStream.setJson(alfrescoJson);
        List<Tuple> tuples = getTuples(tupleStream);

        assert (tuples.size() == 1);
        Tuple tuple = tuples.get(0);

        Double count = tuple.getDouble("count(*)");
        assertEquals(10, count, 0.0);

        expr = "alfrescoFacets(facet("
                + "myCollection, "
                + "q=\"*.*\", "
                + "buckets=\"audio:trackNumber\", "
                + "bucketSorts=\"audio:trackNumber desc\", "
                + "bucketSizeLimit=100, "
                + "min(cm:fiveStarRatingSchemeTotal), max(cm:fiveStarRatingSchemeTotal),"
                +" avg(cm:fiveStarRatingSchemeTotal), sum(cm:fiveStarRatingSchemeTotal)"
                +", count(*)"
                + "))";
        params = params("expr", expr, "qt", "/stream", "myCollection.shards", shards);

        tupleStream = new AlfrescoSolrStream(((HttpSolrClient) clusterClients.get(0)).getBaseURL(), params);
        tupleStream.setJson(alfrescoJson);
        tuples = getTuples(tupleStream);

        assert (tuples.size() == 3);
        for (Tuple aTuple : tuples)
        {
            switch (aTuple.getDouble("audio:trackNumber").intValue())
            {
                case 2:
                    assertEquals(4D, aTuple.getDouble("count(*)"), 0.0);
                    assertEquals(1D, aTuple.getDouble("min(cm:fiveStarRatingSchemeTotal)"), 0.0);
                    assertEquals(10D, aTuple.getDouble("max(cm:fiveStarRatingSchemeTotal)"), 0.0);
                    assertEquals(22D, aTuple.getDouble("sum(cm:fiveStarRatingSchemeTotal)"), 0.0);
                    assertEquals(5.5D, aTuple.getDouble("avg(cm:fiveStarRatingSchemeTotal)"), 0.0);
                    break;
                case 4:
                    assertEquals(3D, aTuple.getDouble("count(*)"), 0.0);
                    assertEquals(2D, aTuple.getDouble("min(cm:fiveStarRatingSchemeTotal)"), 0.0);
                    assertEquals(8D, aTuple.getDouble("max(cm:fiveStarRatingSchemeTotal)"), 0.0);
                    assertEquals(15D, aTuple.getDouble("sum(cm:fiveStarRatingSchemeTotal)"), 0.0);
                    assertEquals(5D, aTuple.getDouble("avg(cm:fiveStarRatingSchemeTotal)"), 0.0);
                    break;
                case 6:
                    assertEquals(3D, aTuple.getDouble("count(*)"), 0.0);
                    assertEquals(3D, aTuple.getDouble("min(cm:fiveStarRatingSchemeTotal)"), 0.0);
                    assertEquals(9D, aTuple.getDouble("max(cm:fiveStarRatingSchemeTotal)"), 0.0);
                    assertEquals(18D, aTuple.getDouble("sum(cm:fiveStarRatingSchemeTotal)"), 0.0);
                    assertEquals(6D, aTuple.getDouble("avg(cm:fiveStarRatingSchemeTotal)"), 0.0);
                    break;
                default:
                    fail("Incorrect bucket sizes");
            }
        }

        expr = "alfrescoFacets(facet("
                + "myCollection, "
                + "q=\"*.*\", "
                + "buckets=\"cm:fiveStarRatingSchemeTotal\", "
                + "bucketSorts=\"fiveStarRatingSchemeTotal desc\", "
                + "bucketSizeLimit=5, "
                + "min(audio:trackNumber)"
                + "))";

        params = params("expr", expr, "qt", "/stream", "myCollection.shards", shards);

        tupleStream = new AlfrescoSolrStream(((HttpSolrClient) clusterClients.get(0)).getBaseURL(), params);
        tupleStream.setJson(alfrescoJson);
        tuples = getTuples(tupleStream);

        assert (tuples.size() == 5);
    }
}