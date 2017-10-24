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

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.TimeZone;

import org.alfresco.model.ContentModel;
import org.alfresco.solr.client.Node;
import org.alfresco.solr.client.NodeMetaData;
import org.alfresco.solr.client.StringPropertyValue;
import org.alfresco.solr.client.Transaction;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.client.solrj.io.Tuple;
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

    private ZoneId zoneId = ZoneId.of(DateTimeZone.UTC.getID());
    private LocalDateTime now = LocalDateTime.now(zoneId);
    private int currentYear = now.getYear();
    private int months = 12;
    private int days = 31;
    private int hours = 24;
    private int numDocs = days * hours * months;
    private Transaction txn = getTransaction(0, numDocs);
    private List<Node> nodes = new ArrayList<>();
    private List<NodeMetaData> nodeMetaDatas = new ArrayList<>();
    private String alfrescoJson = "{ \"authorities\": [ \"jim\", \"joel\" ], \"tenants\": [ \"\" ] }";

    @Test
    public void testSearch() throws Exception
    {
        // Start date inclusive, end date exclusive
        LocalDateTime startDate = LocalDateTime.of(currentYear, 1, 5, 0, 0, 0);
        LocalDateTime endDate = startDate.plus(7, ChronoUnit.MONTHS).plus(3, ChronoUnit.DAYS);
        Instant start = startDate.toInstant(ZoneOffset.UTC);
        Instant end = endDate.toInstant(ZoneOffset.UTC);
        int days = calculateDifference(start, end);

        String sql = "select cm_created_day, count(*) from alfresco where cm_created >= '" + start.toString() + "' and cm_created < '" + end.toString() +"' group by cm_created_day";
        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);
        int size = tuples.size();

        assertEquals(days, size);

        ListIterator<Tuple> iterator = tuples.listIterator();
        while (iterator.hasNext())
        {
            boolean hasPrevious = iterator.hasPrevious();
            Tuple tuple = iterator.next();
            boolean hasNext = iterator.hasNext();
            long count = tuple.getLong("EXPR$1").longValue();

            if (!hasPrevious)
            {
                assertEquals(hours, count);
            }
            else if (!hasNext)
            {
                assertEquals(hours, count);
            }
            else
            {
                assertEquals(hours, count);
            }
        }

        // Start date inclusive, end date inclusive
        startDate = LocalDateTime.of(currentYear, 6, 1, 0, 0, 0);
        endDate = startDate.plus(1, ChronoUnit.MONTHS).plus(8, ChronoUnit.DAYS);
        start = startDate.toInstant(ZoneOffset.UTC);
        end = endDate.toInstant(ZoneOffset.UTC);
        days = calculateDifference(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created >= '" + start.toString() + "' and cm_created <= '" + end.toString() +"' group by cm_created_day";
        tuples = sqlQuery(sql, alfrescoJson);
        size = tuples.size();

        assertEquals(days, size);

        iterator = tuples.listIterator();
        while (iterator.hasNext())
        {
            boolean hasPrevious = iterator.hasPrevious();
            Tuple tuple = iterator.next();
            boolean hasNext = iterator.hasNext();
            long count = tuple.getLong("EXPR$1").longValue();

            if (!hasPrevious)
            {
                assertEquals(hours, count);
            }
            else if (!hasNext)
            {
                assertEquals(hours + 1, count);
            }
            else
            {
                assertEquals(hours, count);
            }
        }

        // Start date exclusive, end date inclusive
        startDate = LocalDateTime.of(currentYear, 4, 1, 0, 0, 0);
        endDate = startDate.plus(1, ChronoUnit.MONTHS).plus(10, ChronoUnit.DAYS);
        start = startDate.toInstant(ZoneOffset.UTC);
        end = endDate.toInstant(ZoneOffset.UTC);
        days = calculateDifference(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created > '" + start.toString() + "' and cm_created <= '" + end.toString() +"' group by cm_created_day";
        tuples = sqlQuery(sql, alfrescoJson);
        size = tuples.size();

        assertEquals(days, size);

        iterator = tuples.listIterator();
        while (iterator.hasNext())
        {
            boolean hasPrevious = iterator.hasPrevious();
            Tuple tuple = iterator.next();
            boolean hasNext = iterator.hasNext();
            long count = tuple.getLong("EXPR$1").longValue();

            if (!hasPrevious)
            {
                assertEquals(hours - 1, count);
            }
            else if (!hasNext)
            {
                assertEquals(hours + 1, count);
            }
            else
            {
                assertEquals(hours, count);
            }
        }

        // Start date exclusive, end date exclusive
        startDate = LocalDateTime.of(currentYear, 9, 7, 0, 0, 0);
        endDate = startDate.plus(2, ChronoUnit.MONTHS).plus(5, ChronoUnit.DAYS);
        start = startDate.toInstant(ZoneOffset.UTC);
        end = endDate.toInstant(ZoneOffset.UTC);
        days = calculateDifference(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created > '" + start.toString() + "' and cm_created < '" + end.toString() +"' group by cm_created_day";
        tuples = sqlQuery(sql, alfrescoJson);
        size = tuples.size();

        assertEquals(days, size);

        iterator = tuples.listIterator();
        while (iterator.hasNext())
        {
            boolean hasPrevious = iterator.hasPrevious();
            Tuple tuple = iterator.next();
            boolean hasNext = iterator.hasNext();
            long count = tuple.getLong("EXPR$1").longValue();

            if (!hasPrevious)
            {
                assertEquals(hours - 1, count);
            }
            else if (!hasNext)
            {
                assertEquals(hours, count);
            }
            else
            {
                assertEquals(hours, count);
            }
        }

        // No start date specified, end date exclusive
        startDate = now.toLocalDate().atStartOfDay().minusDays(30);
        endDate = startDate.plus(2, ChronoUnit.MONTHS).plus(15, ChronoUnit.DAYS);
        start = startDate.toInstant(ZoneOffset.UTC);
        end = endDate.toInstant(ZoneOffset.UTC);
        days = calculateDifference(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created < '" + end.toString() +"' group by cm_created_day";
        tuples = sqlQuery(sql, alfrescoJson);
        size = tuples.size();

        assertEquals(days, size);

        end = now.withDayOfMonth(1).with(LocalTime.MIN).toInstant(ZoneOffset.UTC);
        int daysToStartCurrentMonth = calculateDifference(start, end);

        iterator = tuples.listIterator();
        while (iterator.hasNext())
        {
            int index = iterator.nextIndex();
            Tuple tuple = iterator.next();
            boolean hasNext = iterator.hasNext();
            long count = tuple.getLong("EXPR$1").longValue();

            if (index < daysToStartCurrentMonth)
            {
                assertEquals(hours, count);
            }
            else if (!hasNext)
            {
                assertEquals(hours, count);
            }
            else
            {
                assertEquals(hours, count);
            }
        }

        // No start date specified, end date inclusive
        startDate = now.toLocalDate().atStartOfDay().minusDays(30);
        endDate = startDate.plus(0, ChronoUnit.MONTHS).plus(9, ChronoUnit.DAYS);
        start = startDate.toInstant(ZoneOffset.UTC);
        end = endDate.toInstant(ZoneOffset.UTC);
        days = calculateDifference(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created <= '" + end.toString() +"' group by cm_created_day";
        tuples = sqlQuery(sql, alfrescoJson);
        size = tuples.size();

        assertEquals(days, size);

        end = now.withDayOfMonth(1).with(LocalTime.MIN).toInstant(ZoneOffset.UTC);
        daysToStartCurrentMonth = calculateDifference(start, end);

        iterator = tuples.listIterator();
        while (iterator.hasNext())
        {
            int index = iterator.nextIndex();
            Tuple tuple = iterator.next();
            boolean hasNext = iterator.hasNext();
            long count = tuple.getLong("EXPR$1").longValue();

            if (index < daysToStartCurrentMonth)
            {
                assertEquals(hours, count);
            }
            else if (!hasNext)
            {
                assertEquals(hours + 1, count);
            }
            else
            {
                assertEquals(hours, count);
            }
        }

        // Start date exclusive, no end date specified
        endDate = now.with(LocalTime.MAX).withNano(0);
        end = endDate.toInstant(ZoneOffset.UTC);
        startDate = endDate.toLocalDate().atStartOfDay().minus(1, ChronoUnit.MONTHS).minus(5, ChronoUnit.DAYS);
        start = startDate.toInstant(ZoneOffset.UTC);
        days = calculateDifference(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created > '" + start.toString() +"' group by cm_created_day";
        tuples = sqlQuery(sql, alfrescoJson);
        size = tuples.size();

        assertEquals(days, size);

        iterator = tuples.listIterator();
        while (iterator.hasNext())
        {
            boolean hasPrevious = iterator.hasPrevious();
            Tuple tuple = iterator.next();
            boolean hasNext = iterator.hasNext();
            long count = tuple.getLong("EXPR$1").longValue();

            if (!hasPrevious)
            {
                assertEquals(hours - 1, count);
            }
            else if (!hasNext)
            {
                assertEquals(hours + 1, count);
            }
            else
            {
                assertEquals(hours, count);
            }
        }

        // Start date inclusive, no end date specified
        endDate = now.with(LocalTime.MAX).withNano(0);
        end = endDate.toInstant(ZoneOffset.UTC);
        startDate = endDate.toLocalDate().atStartOfDay().minus(3, ChronoUnit.MONTHS).minus(18, ChronoUnit.DAYS);
        start = startDate.toInstant(ZoneOffset.UTC);
        days = calculateDifference(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created >= '" + start.toString() +"' group by cm_created_day";
        tuples = sqlQuery(sql, alfrescoJson);
        size = tuples.size();

        assertEquals(days, size);

        iterator = tuples.listIterator();
        while (iterator.hasNext())
        {
            boolean hasPrevious = iterator.hasPrevious();
            Tuple tuple = iterator.next();
            boolean hasNext = iterator.hasNext();
            long count = tuple.getLong("EXPR$1").longValue();

            if (!hasPrevious)
            {
                assertEquals(hours, count);
            }
            else if (!hasNext)
            {
                assertEquals(hours + 1, count);
            }
            else
            {
                assertEquals(hours, count);
            }
        }

        // No start date specified, no end date specified
        start = now.toLocalDate().atStartOfDay().minusDays(30).toInstant(ZoneOffset.UTC);
        end = now.with(LocalTime.MAX).withNano(0).toInstant(ZoneOffset.UTC);
        days = calculateDifference(start, end);

        sql = "select cm_created_day, count(*) from alfresco group by cm_created_day";
        tuples = sqlQuery(sql, alfrescoJson);
        size = tuples.size();

        assertEquals(days, size);

        end = now.withDayOfMonth(1).with(LocalTime.MIN).toInstant(ZoneOffset.UTC);
        daysToStartCurrentMonth = calculateDifference(start, end);

        iterator = tuples.listIterator();
        while (iterator.hasNext())
        {
            int index = iterator.nextIndex();
            Tuple tuple = iterator.next();
            boolean hasNext = iterator.hasNext();
            long count = tuple.getLong("EXPR$1").longValue();

            if (index < daysToStartCurrentMonth)
            {
                assertEquals(hours, count);
            }
            else if (!hasNext)
            {
                assertEquals(hours + 1, count);
            }
            else
            {
                assertEquals(hours, count);
            }
        }
    }

    @Before
    private void createData() throws Exception
    {
        TimeZone.setDefault(TimeZone.getTimeZone(zoneId));

        int failedDateCount = 0;
        for (int i = 1; i <= months; i++)
        {
            for (int j = 1; j <= days; j++)
            {
                for (int k = 0; k < hours; k++)
                {
                    try
                    {
                        setProperties(LocalDateTime.of(currentYear, i, j, k, 0, 0).toInstant(ZoneOffset.UTC).toString());
                    }
                    catch (DateTimeException dte)
                    {
                        failedDateCount += 1;
                    }
                }
            }
        }

        indexTransaction(txn, nodes, nodeMetaDatas);
        waitForDocCount(new TermQuery(new Term("content@s___t@{http://www.alfresco.org/model/content/1.0}content", "world")), numDocs - failedDateCount + 4, 80000);
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

    private int calculateDifference(Instant startDate, Instant endDate)
    {
        double days = (double) startDate.until(endDate, ChronoUnit.HOURS) / hours;
        int difference = (int) Math.ceil(days);
        return difference;
    }
}
