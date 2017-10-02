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

import org.alfresco.model.ContentModel;
import org.alfresco.service.cmr.repository.datatype.DefaultTypeConverter;
import org.alfresco.solr.client.Node;
import org.alfresco.solr.client.NodeMetaData;
import org.alfresco.solr.client.StringPropertyValue;
import org.alfresco.solr.client.Transaction;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.io.Tuple;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.alfresco.solr.AlfrescoSolrUtils.getNode;
import static org.alfresco.solr.AlfrescoSolrUtils.getNodeMetaData;
import static org.alfresco.solr.AlfrescoSolrUtils.getTransaction;

/**
 * @author Joel
 */
@SolrTestCaseJ4.SuppressSSL
@LuceneTestCase.SuppressCodecs({"Appending","Lucene3x","Lucene40","Lucene41","Lucene42","Lucene43", "Lucene44", "Lucene45","Lucene46","Lucene47","Lucene48","Lucene49"})
public class DistributedSqlTimeSeriesTest extends AbstractStreamTest
{
    private String sql = "select DBID, LID from alfresco where cm_content = 'world' order by DBID limit 10 ";
    private Map<Integer, Integer> dayCount = new HashMap();
    private Map<String, Integer> dayCount2 = new HashMap();
    private Map<String, Integer> yearCount = new HashMap();



    @Rule
    public JettyServerRule jetty = new JettyServerRule(2, this);

    @Test
    public void testSearch() throws Exception
    {
        loadTimeSeriesData();

        String alfrescoJson = "{ \"authorities\": [ \"jim\", \"joel\" ], \"tenants\": [ \"\" ] }";
        sql = "select cm_created_day, count(*), sum(cm_fiveStarRatingSchemeTotal), avg(cm_fiveStarRatingSchemeTotal), min(cm_fiveStarRatingSchemeTotal), max(cm_fiveStarRatingSchemeTotal) from alfresco where cm_created >= '2010-02-01T01:01:01Z' and cm_created <= '2010-02-14T23:59:59Z' group by cm_created_day";
        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);

        assertTrue(tuples.size() == 14);

        for(Tuple tuple : tuples) {
            String dayString = tuple.getString("cm_created_day");
            int day = Integer.parseInt(dayString.split("-")[2]);
            int indexedCount = dayCount.get(day);
            long count = tuple.getLong("EXPR$1");
            double sum = tuple.getDouble("EXPR$2");
            double avg = tuple.getDouble("EXPR$3");
            double min = tuple.getDouble("EXPR$4");
            double max = tuple.getDouble("EXPR$5");
            assertEquals(indexedCount, count);
            assertEquals(indexedCount*10, sum, 0);
            assertEquals(avg, 10, 0);
            assertEquals(min, 10, 0);
            assertEquals(max, 10, 0);
        }

        sql = "select cm_created_day, count(*) as ct, sum(cm_fiveStarRatingSchemeTotal) as sm, avg(cm_fiveStarRatingSchemeTotal) as av, min(cm_fiveStarRatingSchemeTotal) as mn, max(cm_fiveStarRatingSchemeTotal) as mx from alfresco where cm_created >= '2010-02-01T01:01:01Z' and cm_created <= '2010-02-14T23:59:59Z' group by cm_created_day";
        tuples = sqlQuery(sql, alfrescoJson);
        assertTrue(tuples.size() == 14);
        for(Tuple tuple : tuples) {
            String dayString = tuple.getString("cm_created_day");
            int day = Integer.parseInt(dayString.split("-")[2]);
            int indexedCount = dayCount.get(day);
            long count = tuple.getLong("ct");
            double sum = tuple.getDouble("sm");
            double avg = tuple.getDouble("av");
            double min = tuple.getDouble("mn");
            double max = tuple.getDouble("mx");
            assertEquals(indexedCount, count);
            assertEquals(indexedCount*10, sum, 0);
            assertEquals(avg, 10, 0);
            assertEquals(min, 10, 0);
            assertEquals(max, 10, 0);
        }

        //Test the date math
        sql = "select cm_created_day, count(*) as ct from alfresco where cm_created >= 'NOW-12DAYS' and cm_created <= 'NOW' group by cm_created_day";
        tuples = sqlQuery(sql, alfrescoJson);
        assertTrue(tuples.size() == 12);


        for(Tuple tuple : tuples) {
            String dayString = tuple.getString("cm_created_day");
            int indexedCount = dayCount2.get(dayString);
            long count = tuple.getLong("ct");
            assertEquals(indexedCount, count);
        }

        //Test year time grain
        sql = "select cm_created_year, count(*) as ct from alfresco where cm_created >= 'NOW-32YEARS' and cm_created <= 'NOW-20YEARS' group by cm_created_year";
        tuples = sqlQuery(sql, alfrescoJson);

        assertTrue(tuples.size() == 12);

        for(Tuple tuple : tuples) {
            String dayString = tuple.getString("cm_created_year");
            int indexedCount = yearCount.get(dayString);
            long count = tuple.getLong("ct");
            assertEquals(indexedCount, count);
        }
    }

    private void loadTimeSeriesData() throws Exception {

        int numDocs = 250;

        Transaction txn = getTransaction(0, numDocs);

        List<Node> nodes = new ArrayList();
        List<NodeMetaData> nodeMetaDatas = new ArrayList();

        for(int i=0; i<numDocs; i++) {
            Node node = getNode(txn, acl, Node.SolrApiNodeStatus.UPDATED);
            nodes.add(node);
            NodeMetaData nodeMetaData1 = getNodeMetaData(node, txn, acl, "mike", null, false);
            int day = (i%14)+1;

            if(dayCount.containsKey(day)) {
                Integer count = dayCount.get(day);
                dayCount.put(day, count.intValue()+1);
            } else {
                dayCount.put(day, 1);
            }

            Date date1 = getDate(2010, 1, day);
            nodeMetaData1.getProperties().put(ContentModel.PROP_CREATED, new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, date1)));
            nodeMetaData1.getProperties().put(PROP_RATING, new StringPropertyValue("10"));
            nodeMetaData1.getProperties().put(PROP_TRACK, new StringPropertyValue("12"));
            nodeMetaData1.getProperties().put(PROP_MANUFACTURER, new StringPropertyValue("Nikon"));
            nodeMetaData1.getProperties().put(ContentModel.PROP_NAME, new StringPropertyValue("name1"));
            nodeMetaData1.getProperties().put(ContentModel.PROP_TITLE, new StringPropertyValue("title1"));
            nodeMetaData1.getProperties().put(ContentModel.PROP_CREATOR, new StringPropertyValue("creator1"));
            nodeMetaData1.getProperties().put(ContentModel.PROP_OWNER, new StringPropertyValue("michael"));
            nodeMetaDatas.add(nodeMetaData1);
        }

        indexTransaction(txn, nodes, nodeMetaDatas);
        loadTimeSeriesData2(numDocs);
        loadTimeSeriesData3(numDocs);
        waitForDocCount(new TermQuery(new Term("content@s___t@{http://www.alfresco.org/model/content/1.0}content", "world")), (numDocs * 3) + 4, 80000);
    }

    private void loadTimeSeriesData2(int numDocs) throws Exception {

        Transaction txn = getTransaction(0, numDocs);

        List<Node> nodes = new ArrayList();
        List<NodeMetaData> nodeMetaDatas = new ArrayList();

        for(int i=0; i<numDocs; i++) {
            Node node = getNode(txn, acl, Node.SolrApiNodeStatus.UPDATED);
            nodes.add(node);
            NodeMetaData nodeMetaData1 = getNodeMetaData(node, txn, acl, "mike", null, false);
            int day = (i%14);

            GregorianCalendar calendar = new GregorianCalendar();
            calendar.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DAY_OF_MONTH), 23, 59, 59);
            calendar.add(calendar.DAY_OF_YEAR, -day);
            String key=calendar.get(calendar.YEAR)+"-"+pad((calendar.get(Calendar.MONTH) + 1))+"-"+pad(calendar.get(Calendar.DAY_OF_MONTH));

            if(dayCount2.containsKey(key)) {
                Integer count = dayCount2.get(key);
                dayCount2.put(key, count.intValue() + 1);
            } else {
                dayCount2.put(key, 1);
            }

            Date date = calendar.getTime();

            nodeMetaData1.getProperties().put(ContentModel.PROP_CREATED, new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, date)));
            nodeMetaData1.getProperties().put(PROP_RATING, new StringPropertyValue("10"));
            nodeMetaData1.getProperties().put(PROP_TRACK, new StringPropertyValue("12"));
            nodeMetaData1.getProperties().put(PROP_MANUFACTURER, new StringPropertyValue("Nikon"));
            nodeMetaData1.getProperties().put(ContentModel.PROP_NAME, new StringPropertyValue("name1"));
            nodeMetaData1.getProperties().put(ContentModel.PROP_TITLE, new StringPropertyValue("title1"));
            nodeMetaData1.getProperties().put(ContentModel.PROP_CREATOR, new StringPropertyValue("creator1"));
            nodeMetaData1.getProperties().put(ContentModel.PROP_OWNER, new StringPropertyValue("michael"));
            nodeMetaDatas.add(nodeMetaData1);
        }

        indexTransaction(txn, nodes, nodeMetaDatas);

    }


    private void loadTimeSeriesData3(int numDocs) throws Exception {

        Transaction txn = getTransaction(0, numDocs);

        List<Node> nodes = new ArrayList();
        List<NodeMetaData> nodeMetaDatas = new ArrayList();

        for(int i=0; i<numDocs; i++) {
            Node node = getNode(txn, acl, Node.SolrApiNodeStatus.UPDATED);
            nodes.add(node);
            NodeMetaData nodeMetaData1 = getNodeMetaData(node, txn, acl, "mike", null, false);
            int year = (i%14);

            GregorianCalendar calendar = new GregorianCalendar();
            calendar.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DAY_OF_MONTH), 23, 59, 59);
            calendar.add(calendar.YEAR, -(year+20));
            String key=Integer.toString(calendar.get(calendar.YEAR));

            if(yearCount.containsKey(key)) {
                Integer count = yearCount.get(key);
                yearCount.put(key, count.intValue() + 1);
            } else {
                yearCount.put(key, 1);
            }

            Date date = calendar.getTime();

            nodeMetaData1.getProperties().put(ContentModel.PROP_CREATED, new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, date)));
            nodeMetaData1.getProperties().put(PROP_RATING, new StringPropertyValue("10"));
            nodeMetaData1.getProperties().put(PROP_TRACK, new StringPropertyValue("12"));
            nodeMetaData1.getProperties().put(PROP_MANUFACTURER, new StringPropertyValue("Nikon"));
            nodeMetaData1.getProperties().put(ContentModel.PROP_NAME, new StringPropertyValue("name1"));
            nodeMetaData1.getProperties().put(ContentModel.PROP_TITLE, new StringPropertyValue("title1"));
            nodeMetaData1.getProperties().put(ContentModel.PROP_CREATOR, new StringPropertyValue("creator1"));
            nodeMetaData1.getProperties().put(ContentModel.PROP_OWNER, new StringPropertyValue("michael"));
            nodeMetaDatas.add(nodeMetaData1);
        }

        indexTransaction(txn, nodes, nodeMetaDatas);
    }

    private String pad(int i) {
        String s = Integer.toString(i);
        if(s.length() == 1) {
            s="0"+s;
        }

        return s;
    }
}

