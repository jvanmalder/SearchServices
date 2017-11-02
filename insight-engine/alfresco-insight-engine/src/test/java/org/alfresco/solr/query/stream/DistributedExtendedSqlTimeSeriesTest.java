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

import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.MONTHS;
import static java.time.temporal.ChronoUnit.YEARS;

import static org.alfresco.model.ContentModel.PROP_CREATED;
import static org.alfresco.model.ContentModel.PROP_CREATOR;
import static org.alfresco.model.ContentModel.PROP_NAME;
import static org.alfresco.model.ContentModel.PROP_OWNER;
import static org.alfresco.model.ContentModel.PROP_TITLE;
import static org.alfresco.solr.AlfrescoSolrUtils.getNode;
import static org.alfresco.solr.AlfrescoSolrUtils.getNodeMetaData;
import static org.alfresco.solr.AlfrescoSolrUtils.getTransaction;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.MonthDay;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.TimeZone;

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
    private int currentYear = now.getYear();
    private Transaction txn = getTransaction(0, totalNumberOfDocuments);
    private List<Node> nodes = new ArrayList<>();
    private List<NodeMetaData> nodeMetaDatas = new ArrayList<>();
    private DateMathParser dateMathParser = new DateMathParser();
    private Map<String, Integer> createdDay = new HashMap<>();
    private Map<String, Integer> createdMonth = new HashMap<>();
    private Map<String, Integer> createdYear = new HashMap<>();
    private boolean debugEnabled = false;

    @Test
    public void testSearch() throws Exception
    {
        // Start date inclusive, end date exclusive
        LocalDateTime startDate = LocalDateTime.of(currentYear, 1, 5, 0, 0, 0);
        LocalDateTime endDate = startDate.plus(7, MONTHS).plus(3, DAYS);
        Instant start = startDate.toInstant(UTC);
        Instant end = endDate.toInstant(UTC);
        int numberOfBuckets = calculateNumberOfBuckets_Day(start, end);

        String sql = "select cm_created_day, count(*) from alfresco where cm_created >= '" + start.toString() + "' and cm_created < '" + end.toString() + "' group by cm_created_day";
        List<Tuple> buckets = executeQuery(sql);
        int bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Day(buckets, true, false, start, end);

        // Start date inclusive, end date exclusive
        String solrStartDate = "/YEAR+5MONTHS/DAY";
        String solrEndDate = "/DAY+1MONTH-2DAYS";
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets_Day(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created >= 'NOW" + solrStartDate + "' and cm_created < 'NOW" + solrEndDate + "' group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Day(buckets, true, false, start, end);

        // Start date inclusive, end date inclusive
        startDate = LocalDateTime.of(currentYear, 6, 1, 0, 0, 0);
        endDate = startDate.plus(1, MONTHS).plus(8, DAYS);
        start = startDate.toInstant(UTC);
        end = endDate.toInstant(UTC);
        numberOfBuckets = calculateNumberOfBuckets_Day(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created >= '" + start.toString() + "' and cm_created <= '" + end.toString() + "' group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Day(buckets, true, true, start, end);

        // Start date inclusive, end date inclusive
        solrStartDate = "/DAY-2MONTHS";
        solrEndDate = "/MONTH+20DAYS";
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets_Day(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created >= 'NOW" + solrStartDate + "' and cm_created <= 'NOW" + solrEndDate + "' group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Day(buckets, true, true, start, end);

        // Start date exclusive, end date inclusive
        startDate = LocalDateTime.of(currentYear, 4, 1, 0, 0, 0);
        endDate = startDate.plus(1, MONTHS).plus(10, DAYS);
        start = startDate.toInstant(UTC);
        end = endDate.toInstant(UTC);
        numberOfBuckets = calculateNumberOfBuckets_Day(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created > '" + start.toString() + "' and cm_created <= '" + end.toString() + "' group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Day(buckets, false, true, start, end);

        // Start date exclusive, end date inclusive
        solrStartDate = "-60DAYS/MONTH";
        solrEndDate = "+1MONTH/DAY";
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets_Day(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created > 'NOW" + solrStartDate + "' and cm_created <= 'NOW" + solrEndDate + "' group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Day(buckets, false, true, start, end);

        // Start date exclusive, end date exclusive
        startDate = LocalDateTime.of(currentYear, 9, 7, 0, 0, 0);
        endDate = startDate.plus(2, MONTHS).plus(5, DAYS);
        start = startDate.toInstant(UTC);
        end = endDate.toInstant(UTC);
        numberOfBuckets = calculateNumberOfBuckets_Day(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created > '" + start.toString() + "' and cm_created < '" + end.toString() + "' group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Day(buckets, false, false, start, end);

        // Start date exclusive, end date exclusive
        solrStartDate = "/MONTH+2DAYS";
        solrEndDate = "+5DAYS/DAY";
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets_Day(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created > 'NOW" + solrStartDate + "' and cm_created < 'NOW" + solrEndDate + "' group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Day(buckets, false, false, start, end);

        // No start date specified, end date exclusive
        startDate = getFallbackStartDate_Day();
        endDate = startDate.plus(2, MONTHS).plus(15, DAYS);
        start = startDate.toInstant(UTC);
        end = endDate.toInstant(UTC);
        numberOfBuckets = calculateNumberOfBuckets_Day(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created < '" + end.toString() + "' group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Day(buckets, true, false, start, end);

        // No start date specified, end date exclusive
        solrStartDate = getSolrFallbackStartDate_Day();
        solrEndDate = "/DAY+1MONTH";
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets_Day(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created < 'NOW" + solrEndDate + "' group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Day(buckets, true, false, start, end);

        // No start date specified, end date inclusive
        startDate = getFallbackStartDate_Day();
        endDate = startDate.plus(0, MONTHS).plus(9, DAYS);
        start = startDate.toInstant(UTC);
        end = endDate.toInstant(UTC);
        numberOfBuckets = calculateNumberOfBuckets_Day(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created <= '" + end.toString() + "' group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Day(buckets, true, true, start, end);

        // No start date specified, end date inclusive
        solrStartDate = getSolrFallbackStartDate_Day();
        solrEndDate = "/DAY+15DAYS";
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets_Day(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created <= 'NOW" + solrEndDate + "' group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Day(buckets, true, true, start, end);

        // Start date exclusive, no end date specified
        endDate = getFallbackEndDate_Day();
        end = endDate.toInstant(UTC);
        startDate = endDate.toLocalDate().atStartOfDay().minus(1, MONTHS).minus(5, DAYS);
        start = startDate.toInstant(UTC);
        numberOfBuckets = calculateNumberOfBuckets_Day(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created > '" + start.toString() + "' group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Day(buckets, false, true, start, end);

        // Start date exclusive, no end date specified
        solrStartDate = "/DAY-5DAYS";
        solrEndDate = getSolrFallbackEndDate_Day();
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets_Day(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created > 'NOW" + solrStartDate + "' group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Day(buckets, false, true, start, end);

        // Start date inclusive, no end date specified
        endDate = getFallbackEndDate_Day();
        end = endDate.toInstant(UTC);
        startDate = endDate.toLocalDate().atStartOfDay().minus(3, MONTHS).minus(18, DAYS);
        start = startDate.toInstant(UTC);
        numberOfBuckets = calculateNumberOfBuckets_Day(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created >= '" + start.toString() + "' group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Day(buckets, true, true, start, end);

        // Start date inclusive, no end date specified
        solrStartDate = "-1MONTH/DAY+24HOURS";
        solrEndDate = getSolrFallbackEndDate_Day();
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets_Day(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created >= 'NOW" + solrStartDate + "' group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Day(buckets, true, true, start, end);

        // No start date specified, no end date specified
        start = getFallbackStartDate_Day().toInstant(UTC);
        end = getFallbackEndDate_Day().toInstant(UTC);
        numberOfBuckets = calculateNumberOfBuckets_Day(start, end);

        sql = "select cm_created_day, count(*) from alfresco group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Day(buckets, true, true, start, end);

        // Start date inclusive, end date exclusive
        startDate = LocalDateTime.of(currentYear, 5, 2, 0, 0, 0);
        endDate = startDate.plus(6, MONTHS).plus(10, DAYS);
        start = startDate.toInstant(UTC);
        end = endDate.toInstant(UTC);
        numberOfBuckets = calculateNumberOfBuckets_Month(start, end);

        sql = "select cm_created_month, count(*) from alfresco where cm_created >= '" + start.toString() + "' and cm_created < '" + end.toString() + "' group by cm_created_month";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Month(buckets, true, false, start, end);

        // Start date inclusive, end date exclusive
        solrStartDate = "/YEAR+2MONTHS+18DAYS/DAY";
        solrEndDate = "/DAY+1MONTH+4DAYS";
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets_Month(start, end);

        sql = "select cm_created_month, count(*) from alfresco where cm_created >= 'NOW" + solrStartDate + "' and cm_created < 'NOW" + solrEndDate + "' group by cm_created_month";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Month(buckets, true, false, start, end);

        // Start date inclusive, end date inclusive
        startDate = LocalDateTime.of(currentYear, 10, 8, 0, 0, 0);
        endDate = startDate.plus(2, MONTHS).minus(22, DAYS);
        start = startDate.toInstant(UTC);
        end = endDate.toInstant(UTC);
        numberOfBuckets = calculateNumberOfBuckets_Month(start, end);

        sql = "select cm_created_month, count(*) from alfresco where cm_created >= '" + start.toString() + "' and cm_created <= '" + end.toString() + "' group by cm_created_month";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Month(buckets, true, true, start, end);

        // Start date inclusive, end date inclusive
        solrStartDate = "/DAY-1MONTHS+18DAYS";
        solrEndDate = "/MONTH+20DAYS+1MONTH";
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets_Month(start, end);

        sql = "select cm_created_month, count(*) from alfresco where cm_created >= 'NOW" + solrStartDate + "' and cm_created <= 'NOW" + solrEndDate + "' group by cm_created_month";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Month(buckets, true, true, start, end);

        // Start date exclusive, end date inclusive
        startDate = LocalDateTime.of(currentYear, 5, 19, 0, 0, 0);
        endDate = startDate.plus(2, MONTHS).minus(13, DAYS);
        start = startDate.toInstant(UTC);
        end = endDate.toInstant(UTC);
        numberOfBuckets = calculateNumberOfBuckets_Month(start, end);

        sql = "select cm_created_month, count(*) from alfresco where cm_created > '" + start.toString() + "' and cm_created <= '" + end.toString() + "' group by cm_created_month";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Month(buckets, false, true, start, end);

        // Start date exclusive, end date inclusive
        solrStartDate = "-55DAYS/MONTH+3DAYS";
        solrEndDate = "+1MONTH/DAY-5DAYS";
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets_Month(start, end);

        sql = "select cm_created_month, count(*) from alfresco where cm_created > 'NOW" + solrStartDate + "' and cm_created <= 'NOW" + solrEndDate + "' group by cm_created_month";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Month(buckets, false, true, start, end);

        // Start date exclusive, end date exclusive
        startDate = LocalDateTime.of(currentYear, 10, 4, 0, 0, 0);
        endDate = startDate.plus(1, MONTHS).minus(11, DAYS);
        start = startDate.toInstant(UTC);
        end = endDate.toInstant(UTC);
        numberOfBuckets = calculateNumberOfBuckets_Month(start, end);

        sql = "select cm_created_month, count(*) from alfresco where cm_created > '" + start.toString() + "' and cm_created < '" + end.toString() + "' group by cm_created_month";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Month(buckets, false, false, start, end);

        // Start date exclusive, end date exclusive
        solrStartDate = "/MONTH+2DAYS-3MONTHS";
        solrEndDate = "+5DAYS/DAY+1MONTH";
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets_Month(start, end);

        sql = "select cm_created_month, count(*) from alfresco where cm_created > 'NOW" + solrStartDate + "' and cm_created < 'NOW" + solrEndDate + "' group by cm_created_month";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Month(buckets, false, false, start, end);

        // No start date specified, end date exclusive
        startDate = getFallbackStartDate_Month();
        endDate = startDate.plus(4, MONTHS).minus(20, DAYS);
        start = startDate.toInstant(UTC);
        end = endDate.toInstant(UTC);
        numberOfBuckets = calculateNumberOfBuckets_Month(start, end);

        sql = "select cm_created_month, count(*) from alfresco where cm_created < '" + end.toString() + "' group by cm_created_month";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Month(buckets, true, false, start, end);

        // No start date specified, end date exclusive
        solrStartDate = getSolrFallbackStartDate_Month();
        solrEndDate = "/DAY+2MONTH-3DAYS";
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets_Month(start, end);

        sql = "select cm_created_month, count(*) from alfresco where cm_created < 'NOW" + solrEndDate + "' group by cm_created_month";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Month(buckets, true, false, start, end);

        // No start date specified, end date inclusive
        startDate = getFallbackStartDate_Month();
        endDate = startDate.plus(0, MONTHS).plus(18, DAYS);
        start = startDate.toInstant(UTC);
        end = endDate.toInstant(UTC);
        numberOfBuckets = calculateNumberOfBuckets_Month(start, end);

        sql = "select cm_created_month, count(*) from alfresco where cm_created <= '" + end.toString() + "' group by cm_created_month";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Month(buckets, true, true, start, end);

        // No start date specified, end date inclusive
        solrStartDate = getSolrFallbackStartDate_Month();
        solrEndDate = "/DAY-15DAYS+3MONTHS";
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets_Month(start, end);

        sql = "select cm_created_month, count(*) from alfresco where cm_created <= 'NOW" + solrEndDate + "' group by cm_created_month";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Month(buckets, true, true, start, end);

        // Start date exclusive, no end date specified
        endDate = getFallbackEndDate_Month();
        end = endDate.toInstant(UTC);
        startDate = endDate.toLocalDate().atStartOfDay().minus(3, MONTHS).plus(18, DAYS);
        start = startDate.toInstant(UTC);
        numberOfBuckets = calculateNumberOfBuckets_Month(start, end);

        sql = "select cm_created_month, count(*) from alfresco where cm_created > '" + start.toString() + "' group by cm_created_month";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Month(buckets, false, true, start, end, true);

        // Start date exclusive, no end date specified
        solrStartDate = "/DAY-25DAYS";
        solrEndDate = getSolrFallbackEndDate_Month();
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets_Month(start, end);

        sql = "select cm_created_month, count(*) from alfresco where cm_created > 'NOW" + solrStartDate + "' group by cm_created_month";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Month(buckets, false, true, start, end, true);

        // Start date inclusive, no end date specified
        endDate = getFallbackEndDate_Month();
        end = endDate.toInstant(UTC);
        startDate = endDate.toLocalDate().atStartOfDay().minus(1, MONTHS).minus(5, DAYS);
        start = startDate.toInstant(UTC);
        numberOfBuckets = calculateNumberOfBuckets_Month(start, end);

        sql = "select cm_created_month, count(*) from alfresco where cm_created >= '" + start.toString() + "' group by cm_created_month";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Month(buckets, true, true, start, end, true);

        // Start date inclusive, no end date specified
        solrStartDate = "-2MONTH/DAY+24HOURS";
        solrEndDate = getSolrFallbackEndDate_Month();
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets_Month(start, end);

        sql = "select cm_created_month, count(*) from alfresco where cm_created >= 'NOW" + solrStartDate + "' group by cm_created_month";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Month(buckets, true, true, start, end, true);

        // No start date specified, no end date specified
        start = getFallbackStartDate_Month().toInstant(UTC);
        end = getFallbackEndDate_Month().toInstant(UTC);
        numberOfBuckets = calculateNumberOfBuckets_Month(start, end);

        sql = "select cm_created_month, count(*) from alfresco group by cm_created_month";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Month(buckets, true, true, start, end, true);

        // Start date inclusive, end date exclusive
        startDate = LocalDateTime.of(currentYear - 2, 1, 5, 0, 0, 0);
        endDate = startDate.plus(3, YEARS).plus(7, MONTHS).plus(3, DAYS);
        start = startDate.toInstant(UTC);
        end = endDate.toInstant(UTC);
        numberOfBuckets = calculateNumberOfBuckets_Year(start, end);

        sql = "select cm_created_year, count(*) from alfresco where cm_created >= '" + start.toString() + "' and cm_created < '" + end.toString() + "' group by cm_created_year";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Year(buckets, true, false, start, end);
    }

    @Before
    private void createData() throws Exception
    {
        TimeZone.setDefault(TimeZone.getTimeZone(zoneId));

        int year = currentYear - 1;
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
                            setProperties(localDateTime.toInstant(UTC).toString());

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

        nodeMetaData.getProperties().put(PROP_CREATED, new StringPropertyValue(createdDate));
        nodeMetaData.getProperties().put(PROP_NAME, new StringPropertyValue("name1"));
        nodeMetaData.getProperties().put(PROP_TITLE, new StringPropertyValue("title1"));
        nodeMetaData.getProperties().put(PROP_CREATOR, new StringPropertyValue("creator1"));
        nodeMetaData.getProperties().put(PROP_OWNER, new StringPropertyValue("jim"));

        nodeMetaDatas.add(nodeMetaData);
    }

    private List<Tuple> executeQuery(String sql) throws IOException
    {
        String alfrescoJson = "{ \"authorities\": [ \"jim\", \"joel\" ], \"tenants\": [ \"\" ] }";
        return sqlQuery(sql, alfrescoJson);
    }

    private int calculateNumberOfBuckets_Day(Instant startDate, Instant endDate)
    {
        double days = (double) startDate.until(endDate, HOURS) / hours;
        return (int) Math.ceil(days);
    }

    private int calculateNumberOfBuckets_Month(Instant startDate, Instant endDate)
    {
        LocalDateTime difference = difference(startDate, endDate);
        LocalDateTime dateTime = difference.with(ChronoField.DAY_OF_MONTH, difference.getDayOfMonth() + 1).withHour(0).withMinute(0).withSecond(0).withNano(0);
        return dateTime.getYear() * 12 + dateTime.getMonthValue() + (dateTime.getDayOfMonth() > 0 ? 1 : 0);
    }

    private int calculateNumberOfBuckets_Year(Instant startDate, Instant endDate)
    {
        LocalDateTime difference = difference(startDate, endDate);
        LocalDateTime dateTime = difference.with(ChronoField.MONTH_OF_YEAR, difference.getMonthValue() + 1).withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0).withNano(0);
        return dateTime.getYear() + (dateTime.getMonthValue() > 0 ? 1 : 0);
    }

    private LocalDateTime difference(Instant startDate, Instant endDate)
    {
        LocalDateTime end = LocalDateTime.ofInstant(endDate, zoneId);
        LocalDateTime start = LocalDateTime.ofInstant(startDate, zoneId);
        LocalDateTime difference = end.minusYears(start.getYear());
        difference = difference.minusMonths(start.getMonthValue());
        difference = difference.minusDays(start.getDayOfMonth());
        difference = difference.minusHours(start.getHour());
        difference = difference.minusMinutes(start.getMinute());
        difference = difference.minusSeconds(start.getSecond());
        return difference.minusNanos(start.getNano());
    }

    private LocalDateTime getFallbackStartDate_Day()
    {
        MonthDay monthDay = MonthDay.from(now);
        YearMonth yearMonth = YearMonth.from(now);
        return LocalDateTime.of(yearMonth.getYear(), yearMonth.getMonthValue(), monthDay.getDayOfMonth(), 0, 0, 0).minusMonths(1);
    }

    private LocalDateTime getFallbackStartDate_Month()
    {
        YearMonth yearMonth = YearMonth.from(now);
        return LocalDateTime.of(yearMonth.getYear(), yearMonth.getMonthValue(), 1, 0, 0, 0).minusMonths(24);
    }

    private LocalDateTime getFallbackEndDate_Day()
    {
        MonthDay monthDay = MonthDay.from(now);
        YearMonth yearMonth = YearMonth.from(now);
        return LocalDateTime.of(yearMonth.getYear(), yearMonth.getMonthValue(), monthDay.getDayOfMonth(), 0, 0, 0).plusDays(1).minusSeconds(1);
    }

    private LocalDateTime getFallbackEndDate_Month()
    {
        YearMonth yearMonth = YearMonth.from(now);
        return LocalDateTime.of(yearMonth.getYear(), yearMonth.getMonthValue(), 1, 0, 0, 0).plusMonths(1).minusSeconds(1);
    }

    private String getSolrFallbackStartDate_Day()
    {
        return "/DAY-1MONTH";
    }

    private String getSolrFallbackStartDate_Month()
    {
        return "/MONTH-24MONTHS";
    }

    private String getSolrFallbackEndDate_Day()
    {
        return "/DAY+1DAY-1SECOND";
    }

    private String getSolrFallbackEndDate_Month()
    {
        return "/MONTH+1MONTH-1SECOND";
    }

    private void assertBucketSize(int expectedBucketSize, int actualBucketSize)
    {
        print("Expected bucket size: " + expectedBucketSize);
        print("Actual bucket size: " + actualBucketSize);
        assertEquals(expectedBucketSize, actualBucketSize);
    }

    private void assertBucketContentSize(long expectedBucketContentSize, long actualBucketContentSize)
    {
        print("Expected bucket content size: " + expectedBucketContentSize);
        print("Actual bucket content size: " + actualBucketContentSize);
        assertEquals(expectedBucketContentSize, actualBucketContentSize);
    }

    private void assertExpectedBucketContent_Day(List<Tuple> buckets, boolean startInclusive, boolean endInclusive, Instant start, Instant end)
    {
        LocalDateTime endDate = LocalDateTime.ofInstant(end, zoneId);
        LocalDateTime startDate = LocalDateTime.ofInstant(start, zoneId);

        print("\n"+ "Start date: " + start);
        print("End date: " + end);
        print("Difference between end date and start date: " + difference(start, end));

        ListIterator<Tuple> iterator = buckets.listIterator();
        int dayCounter = 0;
        while (iterator.hasNext())
        {
            boolean hasPrevious = iterator.hasPrevious();
            Tuple tuple = iterator.next();
            boolean hasNext = iterator.hasNext();
            String createdDate = tuple.getString("cm_created_day");
            long count = tuple.getLong("EXPR$1").longValue();

            print("\n" + "Creation date: " + createdDate + ".");

            LocalDateTime startRange;
            LocalDateTime endRange;
            int numberOfCreatedDocuments;
            if (!hasPrevious)
            {
                if (buckets.size() == 1)
                {
                    startRange = startDate;
                    endRange = endDate;
                    numberOfCreatedDocuments = getTotalNumberOfDocumentsForRange(startRange, endRange);
                }
                else
                {
                    startRange = startDate.plusDays(dayCounter++);
                    endRange = startDate.plusDays(dayCounter);
                    numberOfCreatedDocuments = getTotalNumberOfDocumentsForRange(startRange, endRange);
                }
                int range = startInclusive ? 0 : -1;
                assertBucketContentSize(numberOfCreatedDocuments + range, count);
            }
            else if (!hasNext)
            {
                startRange = startDate.plusDays(dayCounter++);
                endRange = endDate;
                int range = endInclusive ? 1 : 0;
                numberOfCreatedDocuments = getTotalNumberOfDocumentsForRange(startRange, endRange);
                assertBucketContentSize(numberOfCreatedDocuments + range, count);
            }
            else
            {
                startRange = startDate.plusDays(dayCounter++);
                endRange = startDate.plusDays(dayCounter);
                numberOfCreatedDocuments = getTotalNumberOfDocumentsForRange(startRange, endRange);
                assertBucketContentSize(numberOfCreatedDocuments, count);
            }
        }

        print("************************************************************************************" + "\n");
    }

    private void assertExpectedBucketContent_Month(List<Tuple> buckets, boolean startInclusive, boolean endInclusive, Instant start, Instant end)
    {
        assertExpectedBucketContent_Month(buckets, startInclusive, endInclusive, start, end, null);
    }

    private void assertExpectedBucketContent_Month(List<Tuple> buckets, boolean startInclusive, boolean endInclusive, Instant start, Instant end, Boolean endDateNotSpecified)
    {
        LocalDateTime endDate = LocalDateTime.ofInstant(end, zoneId);
        LocalDateTime startDate = LocalDateTime.ofInstant(start, zoneId);

        print("\n"+ "Start date: " + start);
        print("End date: " + end);
        print("Difference between end date and start date: " + difference(start, end));

        ListIterator<Tuple> iterator = buckets.listIterator();
        int monthCounter = 0;
        while (iterator.hasNext())
        {
            boolean hasPrevious = iterator.hasPrevious();
            Tuple tuple = iterator.next();
            boolean hasNext = iterator.hasNext();
            String createdDate = tuple.getString("cm_created_month");
            long count = tuple.getLong("EXPR$1").longValue();

            print("\n"+ "Creation date: " + createdDate + ".");

            LocalDateTime startRange;
            LocalDateTime endRange;
            int numberOfCreatedDocuments;
            if (!hasPrevious)
            {
                if (buckets.size() == 1)
                {
                    startRange = startDate;
                    endRange = endDate;
                }
                else
                {
                    startRange = startDate.plusMonths(monthCounter++);
                    endRange = startDate.plusMonths(monthCounter);
                }

                int range = startInclusive ? 0 : -1;
                numberOfCreatedDocuments = getTotalNumberOfDocumentsForRange(startRange, endRange);
                assertBucketContentSize(numberOfCreatedDocuments + range, count);
            }
            else if (!hasNext)
            {
                if (endDateNotSpecified == null)
                {
                    startRange = startDate.plusMonths(monthCounter++);
                    endRange = endDate;
                    int range = endInclusive ? 1 : 0;
                    numberOfCreatedDocuments = getTotalNumberOfDocumentsForRange(startRange, endRange);
                    assertBucketContentSize(numberOfCreatedDocuments + range, count);
                }
                else
                {
                    assertBucketContentSize(createdMonth.get(createdDate) + 1, count);
                }
            }
            else
            {
                startRange = startDate.plusMonths(monthCounter++);
                endRange = startDate.plusMonths(monthCounter);
                numberOfCreatedDocuments = getTotalNumberOfDocumentsForRange(startRange, endRange);
                assertBucketContentSize(numberOfCreatedDocuments, count);
            }
        }

        print("************************************************************************************" + "\n");
    }

    private void assertExpectedBucketContent_Year(List<Tuple> buckets, boolean startInclusive, boolean endInclusive, Instant start, Instant end)
    {
        LocalDateTime endDate = LocalDateTime.ofInstant(end, zoneId);
        LocalDateTime startDate = LocalDateTime.ofInstant(start, zoneId);

        print("\n"+ "Start date: " + start);
        print("End date: " + end);
        print("Difference between end date and start date: " + difference(start, end));

        ListIterator<Tuple> iterator = buckets.listIterator();
        int yearCounter = 0;
        while (iterator.hasNext())
        {
            boolean hasPrevious = iterator.hasPrevious();
            Tuple tuple = iterator.next();
            boolean hasNext = iterator.hasNext();
            String createdDate = tuple.getString("cm_created_year");
            long count = tuple.getLong("EXPR$1").longValue();

            print("\n"+ "Creation date: " + createdDate + ".");

            LocalDateTime startRange;
            LocalDateTime endRange;
            int numberOfCreatedDocuments;
            if (!hasPrevious)
            {
                if (buckets.size() == 1)
                {
                    startRange = startDate;
                    endRange = endDate;
                }
                else
                {
                    startRange = startDate.plusYears(yearCounter++);
                    endRange = startDate.plusYears(yearCounter);
                }

                int range = startInclusive ? 0 : -1;
                numberOfCreatedDocuments = getTotalNumberOfDocumentsForRange(startRange, endRange);
                assertBucketContentSize(numberOfCreatedDocuments + range, count);
            }
            else if (!hasNext)
            {
                startRange = startDate.plusYears(yearCounter++);
                endRange = endDate;
                numberOfCreatedDocuments = getTotalNumberOfDocumentsForRange(startRange, endRange);
                assertBucketContentSize(numberOfCreatedDocuments, count);
            }
            else
            {
                startRange = startDate.plusYears(yearCounter++);
                endRange = startDate.plusYears(yearCounter);
                numberOfCreatedDocuments = getTotalNumberOfDocumentsForRange(startRange, endRange);
                assertBucketContentSize(numberOfCreatedDocuments, count);
            }
        }

        print("************************************************************************************" + "\n");
    }

    private int getTotalNumberOfDocumentsForRange(LocalDateTime start, LocalDateTime end)
    {
        int total = 0;
        for (LocalDateTime date = start; date.isBefore(end); date = date.plusDays(1))
        {
            Integer numberOfDocuments = createdDay.get(date.toLocalDate().toString());
            total += (numberOfDocuments == null ? 0 : numberOfDocuments);
        }
        return total;
    }

    private void print (String message)
    {
        if (debugEnabled)
        {
            System.out.println(message);
        }
    }
}
