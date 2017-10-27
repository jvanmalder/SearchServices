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

import static org.alfresco.solr.AlfrescoSolrUtils.getAcl;
import static org.alfresco.solr.AlfrescoSolrUtils.getAclChangeSet;
import static org.alfresco.solr.AlfrescoSolrUtils.getAclReaders;
import static org.alfresco.solr.AlfrescoSolrUtils.getNode;
import static org.alfresco.solr.AlfrescoSolrUtils.getNodeMetaData;
import static org.alfresco.solr.AlfrescoSolrUtils.getTransaction;
import static org.alfresco.solr.AlfrescoSolrUtils.indexAclChangeSet;
import static org.alfresco.solr.AlfrescoSolrUtils.list;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

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
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

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
    private Node nodeUSA, nodeJapan, nodeHere;
    @Rule
    public JettyServerRule jetty = new JettyServerRule(1, this);
    
    private LocalDateTime presentTime= LocalDateTime.of(2017, 1, 1, 12, 0);
    @Before
    public void loadTimeZoneData() throws Exception
    {
        AclChangeSet aclChangeSet = getAclChangeSet(1);
        Acl tzAcl = getAcl(aclChangeSet);
        AclReaders tzAclReaders = getAclReaders(aclChangeSet, tzAcl, list("mcfly"), list("delorean"), null);
        indexAclChangeSet(aclChangeSet,
                list(tzAcl),
                list(tzAclReaders));
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

         //First create a transaction.
         Transaction txn = getTransaction(0, 2);
         Instant present = presentTime.toInstant(ZoneOffset.UTC);
         Instant dateLA = LocalDateTime.ofInstant(presentTime.toInstant(ZoneOffset.UTC), ZoneId.of("America/Los_Angeles")).toInstant(ZoneOffset.UTC);
         Instant dateTokyo = LocalDateTime.ofInstant(presentTime.toInstant(ZoneOffset.UTC), ZoneId.of("Asia/Tokyo")).toInstant(ZoneOffset.UTC);;
         System.out.println(present.toString());
         System.out.println(dateLA.toString());
         System.out.println(dateTokyo.toString());
         
         nodeUSA = getNode(txn, tzAcl, Node.SolrApiNodeStatus.UPDATED);
         nodeJapan = getNode(txn, tzAcl, Node.SolrApiNodeStatus.UPDATED);
         nodeHere = getNode(txn, tzAcl, Node.SolrApiNodeStatus.UPDATED);
         
         NodeMetaData nodeMetaDataHere = getNodeMetaData(nodeHere, txn, tzAcl, "marty", null, false);

         nodeMetaDataHere.getProperties().put(ContentModel.PROP_CREATED,
                 new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, present.toString())));
         nodeMetaDataHere.getProperties().put(ContentModel.PROP_TITLE, new StringPropertyValue("NowTime"));
         nodeMetaDataHere.getProperties().put(ContentModel.PROP_CREATOR, new StringPropertyValue("Dr who"));
         
         NodeMetaData nodeMetaDataUSA = getNodeMetaData(nodeUSA, txn, tzAcl, "marty", null, false);

         nodeMetaDataUSA.getProperties().put(ContentModel.PROP_CREATED,
                 new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, dateLA.toString())));
         nodeMetaDataUSA.getProperties().put(ContentModel.PROP_TITLE, new StringPropertyValue("AmericanTime"));
         nodeMetaDataUSA.getProperties().put(ContentModel.PROP_CREATOR, new StringPropertyValue("Brown"));
         
         NodeMetaData nodeMetaDataJapan = getNodeMetaData(nodeJapan, txn, tzAcl, "marty", null, false);
 
         nodeMetaDataJapan.getProperties().put(ContentModel.PROP_CREATED,
                 new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, dateTokyo.toString())));
         nodeMetaDataJapan.getProperties().put(ContentModel.PROP_TITLE, new StringPropertyValue("NipponTime"));
         nodeMetaDataJapan.getProperties().put(ContentModel.PROP_CREATOR, new StringPropertyValue("suzuki"));
         
         indexTransaction(txn,
                 list(nodeUSA, nodeJapan, nodeHere),
                 list(nodeMetaDataUSA, nodeMetaDataJapan, nodeMetaDataHere));
         
         //Check for the TXN state stamp.
         builder = new BooleanQuery.Builder();
         builder.add(new BooleanClause(new TermQuery(new Term(QueryConstants.FIELD_SOLR4_ID, "TRACKER!STATE!TX")), BooleanClause.Occur.MUST));
         builder.add(new BooleanClause(LegacyNumericRangeQuery.newLongRange(QueryConstants.FIELD_S_TXID, txn.getId(), txn.getId() + 1, true, false), BooleanClause.Occur.MUST));
         waitForQuery = builder.build();

         waitForDocCountAllCores(waitForQuery, 1, 80000);
    }
    @Test
    public void testSearch() throws Exception
    {
        String alfrescoJson = "{ \"authorities\": [ \"jim\", \"joel\" ], \"tenants\": [ \"\" ] }";
        String timeJson = "{ \"authorities\": [ \"mcfly\" ], \"tenants\": [ \"\" ] }";
//        String sql = "select DBID, cm_created, cm_creator from alfresco where cm_created = 'NOW/DAY-1DAYS'";
        String sql = "select cm_created_day, count(*) as total from alfresco where cm_created >='2017-01-01T01:00:01Z' and cm_created <= '2017-01-01T23:59:59Z' group by cm_created_day";

        Long expected = new Long(3);

        List<Tuple> tuples = sqlQuery(sql, timeJson);
        assertTrue(tuples.size() == 1);
        System.out.println("UTC " + tuples.get(0).get("total"));
        assertEquals(expected, tuples.get(0).get("total"));
        
        tuples = sqlQuery(sql, timeJson, "America/Los_Angeles");
        System.out.println("LA " + tuples.get(0).get("total"));
        assertTrue(tuples.size() == 1);
        assertEquals(expected, tuples.get(0).get("total"));

        tuples = sqlQuery(sql, timeJson, "Japan");
        System.out.println("Tokyo " + tuples.get(0).get("total"));
        assertTrue(tuples.size() == 1);
        assertEquals(expected, tuples.get(0).get("total"));
        
        
    }

}

