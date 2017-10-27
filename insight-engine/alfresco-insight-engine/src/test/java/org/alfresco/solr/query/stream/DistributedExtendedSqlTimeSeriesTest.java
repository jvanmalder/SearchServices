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
package org.alfresco.solr.query.stream;

import static org.alfresco.solr.AlfrescoSolrUtils.getNode;
import static org.alfresco.solr.AlfrescoSolrUtils.getNodeMetaData;
import static org.alfresco.solr.AlfrescoSolrUtils.getTransaction;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.TimeZone;

import org.alfresco.model.ContentModel;
import org.alfresco.solr.client.Node;
import org.alfresco.solr.client.NodeMetaData;
import org.alfresco.solr.client.StringPropertyValue;
import org.alfresco.solr.client.Transaction;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.util.DateMathParser;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Tuna Aksoy
 */
public class DistributedExtendedSqlTimeSeriesTest extends AbstractStreamTest
{
    @Rule
    public JettyServerRule jetty = new JettyServerRule(1, this);

    private int hours = 24;
    private int days = 31;
    private int months = 12;
    private int years = 3;
    private int totalNumberOfDocuments = hours * days * months * years;
    private ZoneId zoneId = ZoneId.of(DateTimeZone.UTC.getID());
    private LocalDateTime now = LocalDateTime.now(zoneId);
    private Transaction txn = getTransaction(0, totalNumberOfDocuments);
    private List<Node> nodes = new ArrayList<>();
    private List<NodeMetaData> nodeMetaDatas = new ArrayList<>();
    private DateMathParser dateMathParser = new DateMathParser();
    private Map<String, Integer> createdDay = new HashMap<>();
    private Map<String, Integer> createdMonth = new HashMap<>();
    private Map<String, Integer> createdYear = new HashMap<>();

    @Test
    public void testSearch() throws Exception
    {
        // Start date inclusive, end date exclusive
        LocalDateTime startDate = LocalDateTime.of(2017, 1, 5, 0, 0, 0);
        LocalDateTime endDate = startDate.plus(7, ChronoUnit.MONTHS).plus(3, ChronoUnit.DAYS);
        Instant start = startDate.toInstant(ZoneOffset.UTC);
        Instant end = endDate.toInstant(ZoneOffset.UTC);
        int numberOfBuckets = calculateNumberOfBuckets(start, end);

        String sql = "select cm_created_day, count(*) from alfresco where cm_created >= '" + start.toString() + "' and cm_created < '" + end.toString() + "' group by cm_created_day";
        List<Tuple> buckets = executeQuery(sql);
        int bucketSize = buckets.size();

        assertEquals(numberOfBuckets, bucketSize);
        assertExpectedBucketContent(buckets, true, false);

        // Start date inclusive, end date exclusive
        String solrStartDate = "/YEAR+5MONTHS/DAY";
        String solrEndDate = "/DAY+1MONTH-2DAYS";
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created >= 'NOW" + solrStartDate + "' and cm_created < 'NOW" + solrEndDate + "' group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertEquals(numberOfBuckets, bucketSize);
        assertExpectedBucketContent(buckets, true, false);

        // Start date inclusive, end date inclusive
        startDate = LocalDateTime.of(2017, 6, 1, 0, 0, 0);
        endDate = startDate.plus(1, ChronoUnit.MONTHS).plus(8, ChronoUnit.DAYS);
        start = startDate.toInstant(ZoneOffset.UTC);
        end = endDate.toInstant(ZoneOffset.UTC);
        numberOfBuckets = calculateNumberOfBuckets(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created >= '" + start.toString() + "' and cm_created <= '" + end.toString() + "' group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertEquals(numberOfBuckets, bucketSize);
        assertExpectedBucketContent(buckets, true, true);

        // Start date inclusive, end date inclusive
        solrStartDate = "/DAY-2MONTHS";
        solrEndDate = "/MONTH+20DAYS";
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created >= 'NOW" + solrStartDate + "' and cm_created <= 'NOW" + solrEndDate + "' group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertEquals(numberOfBuckets, bucketSize);
        assertExpectedBucketContent(buckets, true, true);

        // Start date exclusive, end date inclusive
        startDate = LocalDateTime.of(2017, 4, 1, 0, 0, 0);
        endDate = startDate.plus(1, ChronoUnit.MONTHS).plus(10, ChronoUnit.DAYS);
        start = startDate.toInstant(ZoneOffset.UTC);
        end = endDate.toInstant(ZoneOffset.UTC);
        numberOfBuckets = calculateNumberOfBuckets(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created > '" + start.toString() + "' and cm_created <= '" + end.toString() + "' group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertEquals(numberOfBuckets, bucketSize);
        assertExpectedBucketContent(buckets, false, true);

        // Start date exclusive, end date inclusive
        solrStartDate = "-60DAYS/MONTH";
        solrEndDate = "+1MONTH/DAY";
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created > 'NOW" + solrStartDate + "' and cm_created <= 'NOW" + solrEndDate + "' group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertEquals(numberOfBuckets, bucketSize);
        assertExpectedBucketContent(buckets, false, true);

        // Start date exclusive, end date exclusive
        startDate = LocalDateTime.of(2017, 9, 7, 0, 0, 0);
        endDate = startDate.plus(2, ChronoUnit.MONTHS).plus(5, ChronoUnit.DAYS);
        start = startDate.toInstant(ZoneOffset.UTC);
        end = endDate.toInstant(ZoneOffset.UTC);
        numberOfBuckets = calculateNumberOfBuckets(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created > '" + start.toString() + "' and cm_created < '" + end.toString() + "' group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertEquals(numberOfBuckets, bucketSize);
        assertExpectedBucketContent(buckets, false, false);

        // Start date exclusive, end date exclusive
        solrStartDate = "/MONTH+2DAYS";
        solrEndDate = "+5DAYS/DAY";
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created > 'NOW" + solrStartDate + "' and cm_created < 'NOW" + solrEndDate + "' group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertEquals(numberOfBuckets, bucketSize);
        assertExpectedBucketContent(buckets, false, false);

        // No start date specified, end date exclusive
        startDate = getFallbackStartDate();
        endDate = startDate.plus(2, ChronoUnit.MONTHS).plus(15, ChronoUnit.DAYS);
        start = startDate.toInstant(ZoneOffset.UTC);
        end = endDate.toInstant(ZoneOffset.UTC);
        numberOfBuckets = calculateNumberOfBuckets(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created < '" + end.toString() + "' group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertEquals(numberOfBuckets, bucketSize);
        assertExpectedBucketContent(buckets, true, false);

        // No start date specified, end date exclusive
        solrStartDate = "-30DAYS/DAY";
        solrEndDate = "/DAY+1MONTH";
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created < 'NOW" + solrEndDate + "' group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertEquals(numberOfBuckets, bucketSize);
        assertExpectedBucketContent(buckets, true, false);

        // No start date specified, end date inclusive
        startDate = getFallbackStartDate();
        endDate = startDate.plus(0, ChronoUnit.MONTHS).plus(9, ChronoUnit.DAYS);
        start = startDate.toInstant(ZoneOffset.UTC);
        end = endDate.toInstant(ZoneOffset.UTC);
        numberOfBuckets = calculateNumberOfBuckets(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created <= '" + end.toString() + "' group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertEquals(numberOfBuckets, bucketSize);
        assertExpectedBucketContent(buckets, true, true);

        // No start date specified, end date inclusive
        solrStartDate = "/DAY-1MONTH";
        solrEndDate = "/DAY+15DAYS";
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created <= 'NOW" + solrEndDate + "' group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertEquals(numberOfBuckets, bucketSize);
        assertExpectedBucketContent(buckets, true, true);

        // Start date exclusive, no end date specified
        endDate = getFallbackEndDate();
        end = endDate.toInstant(ZoneOffset.UTC);
        startDate = endDate.toLocalDate().atStartOfDay().minus(1, ChronoUnit.MONTHS).minus(5, ChronoUnit.DAYS);
        start = startDate.toInstant(ZoneOffset.UTC);
        numberOfBuckets = calculateNumberOfBuckets(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created > '" + start.toString() + "' group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertEquals(numberOfBuckets, bucketSize);
        assertExpectedBucketContent(buckets, false, true);

        // Start date exclusive, no end date specified
        solrStartDate = "/DAY-5DAYS";
        solrEndDate = "+1DAY/DAY-1SECOND";
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created > 'NOW" + solrStartDate + "' group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertEquals(numberOfBuckets, bucketSize);
        assertExpectedBucketContent(buckets, false, true);

        // Start date inclusive, no end date specified
        endDate = getFallbackEndDate();
        end = endDate.toInstant(ZoneOffset.UTC);
        startDate = endDate.toLocalDate().atStartOfDay().minus(3, ChronoUnit.MONTHS).minus(18, ChronoUnit.DAYS);
        start = startDate.toInstant(ZoneOffset.UTC);
        numberOfBuckets = calculateNumberOfBuckets(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created >= '" + start.toString() + "' group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertEquals(numberOfBuckets, bucketSize);
        assertExpectedBucketContent(buckets, true, true);

        // Start date inclusive, no end date specified
        solrStartDate = "-1MONTH/DAY+24HOURS";
        solrEndDate = "+1DAY/DAY-1SECOND";
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created >= 'NOW" + solrStartDate + "' group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertEquals(numberOfBuckets, bucketSize);
        assertExpectedBucketContent(buckets, true, true);

        // No start date specified, no end date specified
        start = getFallbackStartDate().toInstant(ZoneOffset.UTC);
        end = getFallbackEndDate().toInstant(ZoneOffset.UTC);
        numberOfBuckets = calculateNumberOfBuckets(start, end);

        sql = "select cm_created_day, count(*) from alfresco group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertEquals(numberOfBuckets, bucketSize);
        assertExpectedBucketContent(buckets, true, true);
    }

    @Before
    private void createData() throws Exception
    {
        TimeZone.setDefault(TimeZone.getTimeZone(zoneId));

        int year = now.getYear() - 1;
        int failedDateCount = 0;

        for (int i = 0; i < years; i++)
        {
            for (int j = 1; j <= months; j++)
            {
                for (int k = 1; k <= days; k++)
                {
                    for (int l = 0; l < hours; l++)
                    {
                        try
                        {
                            LocalDateTime localDateTime = LocalDateTime.of(year + i, j, k, l, 0, 0);
                            setProperties(localDateTime.toInstant(ZoneOffset.UTC).toString());

                            String localDate = localDateTime.toLocalDate().toString();
                            String dayKey = localDate;
                            if (createdDay.containsKey(dayKey))
                            {
                                createdDay.put(dayKey, createdDay.get(dayKey) + 1);
                            }
                            else
                            {
                                createdDay.put(dayKey, 1);
                            }

                            String monthKey = localDate.substring(0, 7);
                            if (dayKey.startsWith(monthKey))
                            {
                                if (createdMonth.containsKey(monthKey))
                                {
                                    createdMonth.put(monthKey, createdMonth.get(monthKey) + 1);
                                }
                                else
                                {
                                    createdMonth.put(monthKey, 1);
                                }
                            }

                            String yearKey = localDate.substring(0, 4);
                            if (monthKey.startsWith(yearKey))
                            {
                                if (createdYear.containsKey(yearKey))
                                {
                                    createdYear.put(yearKey, createdYear.get(yearKey) + 1);
                                }
                                else
                                {
                                    createdYear.put(yearKey, 1);
                                }
                            }
                        }
                        catch (DateTimeException dte)
                        {
                            failedDateCount += 1;
                        }
                    }
                }
            }
        }

        indexTransaction(txn, nodes, nodeMetaDatas);
        waitForDocCount(new TermQuery(new Term("content@s___t@{http://www.alfresco.org/model/content/1.0}content", "world")), totalNumberOfDocuments - failedDateCount + 4, 200000);
    }

    private void setProperties(String createdDate)
    {
        Node node = getNode(txn, acl, Node.SolrApiNodeStatus.UPDATED);
        nodes.add(node);
        NodeMetaData nodeMetaData = getNodeMetaData(node, txn, acl, "mike", null, false);

        nodeMetaData.getProperties().put(ContentModel.PROP_CREATED, new StringPropertyValue(createdDate));
        nodeMetaData.getProperties().put(ContentModel.PROP_NAME, new StringPropertyValue("name1"));
        nodeMetaData.getProperties().put(ContentModel.PROP_TITLE, new StringPropertyValue("title1"));
        nodeMetaData.getProperties().put(ContentModel.PROP_CREATOR, new StringPropertyValue("creator1"));
        nodeMetaData.getProperties().put(ContentModel.PROP_OWNER, new StringPropertyValue("jim"));

        nodeMetaDatas.add(nodeMetaData);
    }

    private List<Tuple> executeQuery(String sql) throws IOException
    {
        String alfrescoJson = "{ \"authorities\": [ \"jim\", \"joel\" ], \"tenants\": [ \"\" ] }";
        return sqlQuery(sql, alfrescoJson);
    }

    private int calculateNumberOfBuckets(Instant startDate, Instant endDate)
    {
        double days = (double) startDate.until(endDate, ChronoUnit.HOURS) / hours;
        int difference = (int) Math.ceil(days);
        return difference;
    }

    private LocalDateTime getFallbackStartDate()
    {
        return now.toLocalDate().atStartOfDay().minusDays(30);
    }

    private LocalDateTime getFallbackEndDate()
    {
        return now.with(LocalTime.MAX).withNano(0);
    }

    private void assertExpectedBucketContent(List<Tuple> buckets, boolean startInclusive, boolean endInclusive)
    {
        ListIterator<Tuple> iterator = buckets.listIterator();
        while (iterator.hasNext())
        {
            boolean hasPrevious = iterator.hasPrevious();
            Tuple tuple = iterator.next();
            boolean hasNext = iterator.hasNext();
            String createdDate = tuple.getString("cm_created_day");
            long count = tuple.getLong("EXPR$1").longValue();
            int numberOfCreatedDocuments = createdDay.get(createdDate).intValue();

            if (!hasPrevious)
            {
                int range = startInclusive ? 0 : -1;
                assertEquals(numberOfCreatedDocuments + range, count);
            }
            else if (!hasNext)
            {
                int range = endInclusive ? 1 : 0;
                assertEquals(numberOfCreatedDocuments + range, count);
            }
            else
            {
                assertEquals(numberOfCreatedDocuments, count);
            }
        }
    }
}
