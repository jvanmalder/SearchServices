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

import static java.util.Collections.singletonList;
import static org.alfresco.solr.AlfrescoSolrUtils.getAcl;
import static org.alfresco.solr.AlfrescoSolrUtils.getAclChangeSet;
import static org.alfresco.solr.AlfrescoSolrUtils.getAclReaders;
import static org.alfresco.solr.AlfrescoSolrUtils.getNode;
import static org.alfresco.solr.AlfrescoSolrUtils.getNodeMetaData;
import static org.alfresco.solr.AlfrescoSolrUtils.getTransaction;
import static org.alfresco.solr.AlfrescoSolrUtils.indexAclChangeSet;

import org.alfresco.model.ContentModel;
import org.alfresco.repo.search.adaptor.lucene.QueryConstants;
import org.alfresco.service.cmr.repository.datatype.DefaultTypeConverter;
import org.alfresco.solr.SolrInformationServer;
import org.alfresco.solr.client.Acl;
import org.alfresco.solr.client.AclChangeSet;
import org.alfresco.solr.client.AclReaders;
import org.alfresco.solr.client.Node;
import org.alfresco.solr.client.NodeMetaData;
import org.alfresco.solr.client.StringPropertyValue;
import org.alfresco.solr.client.Transaction;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.LegacyNumericRangeQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.io.Tuple;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

/**
 * Test that validates the time zone is taken into account when a query is executed.
 * The documents will be indexed with different time zone and the queries should
 * show correct number of documents pending the time zone specified.
 * 
 * Test data is of 2 nodes created on the same day but in 2 different time zones.
 * Due to time difference, the node created in America will appear as 1999, as Los Angeles is
 * 16 hours behind Japan which should show as 2000. 
 * 
 * To run this test we will need to mark
 * @author Michael Suzuki
 */
@SolrTestCaseJ4.SuppressSSL
@LuceneTestCase.SuppressCodecs({"Appending","Lucene3x","Lucene40","Lucene41","Lucene42","Lucene43", "Lucene44", "Lucene45","Lucene46","Lucene47","Lucene48","Lucene49"})
public class DistributedTimeZoneTest extends AbstractStreamTest
{
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

    @Before
    public void loadTimeZoneData() throws Exception
    {
        AclChangeSet aclChangeSet = getAclChangeSet(1);
        Acl tzAcl = getAcl(aclChangeSet);
        AclReaders tzAclReaders =
                getAclReaders(
                        aclChangeSet,
                        tzAcl,
                        singletonList("mcfly"),
                        singletonList("delorean"), null);

        indexAclChangeSet(aclChangeSet,
                singletonList(tzAcl),
                singletonList(tzAclReaders));

        //Check for the ACL state stamp.
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new BooleanClause(new TermQuery(new Term(QueryConstants.FIELD_SOLR4_ID, "TRACKER!STATE!ACLTX")), BooleanClause.Occur.MUST));
        builder.add(new BooleanClause(LegacyNumericRangeQuery.newLongRange(QueryConstants.FIELD_S_ACLTXID, aclChangeSet.getId(), aclChangeSet.getId() + 1, true, false), BooleanClause.Occur.MUST));
        BooleanQuery waitForQuery = builder.build();
        waitForDocCountAllCores(waitForQuery, 1, 80000);

        //Check that both ACL's are in the index
        BooleanQuery.Builder builder1 = new BooleanQuery.Builder();
        builder1.add(new BooleanClause(new TermQuery(new Term(QueryConstants.FIELD_DOC_TYPE, SolrInformationServer.DOC_TYPE_ACL)), BooleanClause.Occur.MUST));
        BooleanQuery waitForQuery1 = builder1.build();
        waitForDocCountAllCores(waitForQuery1, 3, 80000);
        /*
         * Create and index a Transaction
         */
        LocalDateTime presentTime= LocalDateTime.now(ZoneId.of("UTC"));
        LocalDateTime testPresentTime = LocalDateTime.of(presentTime.getYear(),
                presentTime.getMonth(),
                presentTime.getDayOfMonth(),
                0, 0);
 

         //First create a transaction.
         
         for(int i = 0; i <= 23; i++)
         {
             int hour = i;
//             int hour = i == 4 ? (i * 6) - 1: i * 6;
             int minute = 23 == hour ? 59 : 0;
             LocalDateTime time= LocalDateTime.of(testPresentTime.getYear(),
                                                  testPresentTime.getMonth(),
                                                  testPresentTime.getDayOfMonth(),
                                                  hour, minute);
             Instant instant = time.toInstant(ZoneOffset.UTC);
            
             Transaction txn = getTransaction(0, 1);
             Node node = getNode(txn, tzAcl, Node.SolrApiNodeStatus.UPDATED);
             NodeMetaData nodeMeta = getNodeMetaData(node, txn, tzAcl, "marty", null, false);

             nodeMeta.getProperties().put(ContentModel.PROP_CREATED,
                 new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, instant.toString())));
             nodeMeta.getProperties().put(ContentModel.PROP_TITLE, new StringPropertyValue("NowTime" + i));
             nodeMeta.getProperties().put(ContentModel.PROP_CREATOR, new StringPropertyValue("Dr who" + i));
             
             indexTransaction(txn,
                     singletonList(node),
                     singletonList(nodeMeta));

             //Check for the TXN state stamp.
             builder = new BooleanQuery.Builder();
             builder.add(new BooleanClause(new TermQuery(new Term(QueryConstants.FIELD_SOLR4_ID, "TRACKER!STATE!TX")), BooleanClause.Occur.MUST));
             builder.add(new BooleanClause(LegacyNumericRangeQuery.newLongRange(QueryConstants.FIELD_S_TXID, txn.getId(), txn.getId() + 1, true, false), BooleanClause.Occur.MUST));
             waitForQuery = builder.build();
             
             waitForDocCountAllCores(waitForQuery, 1, 80000);
         }
    }

    private long now()
    {
        TimeZone timeZone = TimeZone.getTimeZone("UTC");
        Calendar cal = Calendar.getInstance(timeZone);
        cal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), cal.get(Calendar.DAY_OF_MONTH), 10, 0, 0);
        return cal.getTime().getTime();
    }

    @Test
    public void testSearch() throws Exception
    {
        String timeJson = "{ \"authorities\": [ \"mcfly\" ], \"tenants\": [ \"\" ] }";
        String sql = "select cm_created_day, count(*) as total from alfresco where cm_created >= 'NOW/DAY' group by cm_created_day";
        long now = now();
        List<Tuple> tuples = sqlQuery(sql, timeJson);
        System.out.println("UTC " + tuples.get(0).get("total") + ":" + tuples.size());
        assertEquals(24L, tuples.get(0).get("total"));

        tuples = sqlQuery(sql, timeJson, "America/Los_Angeles", now);
        System.out.println("LA " + tuples.get(0).get("total"));
        assertEquals(16L, tuples.get(0).get("total"));

        tuples = sqlQuery(sql, timeJson, "Japan", now);
        System.out.println("Japan " + tuples.get(0).get("total"));
        assertEquals(16L, tuples.get(0).get("total"));
    }
}