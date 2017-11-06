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
import static org.alfresco.solr.sql.SolrTable.DEFAULT_END_DATE_DAY;
import static org.alfresco.solr.sql.SolrTable.DEFAULT_END_DATE_MONTH;
import static org.alfresco.solr.sql.SolrTable.DEFAULT_END_DATE_YEAR;
import static org.alfresco.solr.sql.SolrTable.DEFAULT_START_DATE_DAY;
import static org.alfresco.solr.sql.SolrTable.DEFAULT_START_DATE_MONTH;
import static org.alfresco.solr.sql.SolrTable.DEFAULT_START_DATE_YEAR;

import java.io.IOException;
import java.text.ParseException;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
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
    private int years = 5;
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
    public void testSearch() throws IOException, ParseException
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
        startDate = parseDateMath(DEFAULT_START_DATE_DAY);
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
        solrStartDate = DEFAULT_START_DATE_DAY;
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
        startDate = parseDateMath(DEFAULT_START_DATE_DAY);
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
        solrStartDate = DEFAULT_START_DATE_DAY;
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
        endDate = parseDateMath(DEFAULT_END_DATE_DAY);
        end = endDate.toInstant(UTC);
        startDate = endDate.toLocalDate().atStartOfDay().minus(1, MONTHS).minus(5, DAYS);
        start = startDate.toInstant(UTC);
        numberOfBuckets = calculateNumberOfBuckets_Day(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created > '" + start.toString() + "' group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Day(buckets, false, true, start, end, false);

        // Start date exclusive, no end date specified
        solrStartDate = "/DAY-5DAYS";
        solrEndDate = DEFAULT_END_DATE_DAY;
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets_Day(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created > 'NOW" + solrStartDate + "' group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Day(buckets, false, true, start, end, false);

        // Start date inclusive, no end date specified
        endDate = parseDateMath(DEFAULT_END_DATE_DAY);
        end = endDate.toInstant(UTC);
        startDate = endDate.toLocalDate().atStartOfDay().minus(3, MONTHS).minus(18, DAYS);
        start = startDate.toInstant(UTC);
        numberOfBuckets = calculateNumberOfBuckets_Day(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created >= '" + start.toString() + "' group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Day(buckets, true, true, start, end, false);

        // Start date inclusive, no end date specified
        solrStartDate = "-1MONTH/DAY+24HOURS";
        solrEndDate = DEFAULT_END_DATE_DAY;
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets_Day(start, end);

        sql = "select cm_created_day, count(*) from alfresco where cm_created >= 'NOW" + solrStartDate + "' group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Day(buckets, true, true, start, end, false);

        // No start date specified, no end date specified
        start = parseDateMath(DEFAULT_START_DATE_DAY).toInstant(UTC);
        end = parseDateMath(DEFAULT_END_DATE_DAY).toInstant(UTC);
        numberOfBuckets = calculateNumberOfBuckets_Day(start, end);

        sql = "select cm_created_day, count(*) from alfresco group by cm_created_day";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Day(buckets, true, true, start, end, false);

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
        startDate = parseDateMath(DEFAULT_START_DATE_MONTH);
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
        solrStartDate = DEFAULT_START_DATE_MONTH;
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
        startDate = parseDateMath(DEFAULT_START_DATE_MONTH);
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
        solrStartDate = DEFAULT_START_DATE_MONTH;
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
        endDate = parseDateMath(DEFAULT_END_DATE_MONTH);
        end = endDate.toInstant(UTC);
        startDate = endDate.toLocalDate().atStartOfDay().minus(3, MONTHS).plus(18, DAYS);
        start = startDate.toInstant(UTC);
        numberOfBuckets = calculateNumberOfBuckets_Month(start, end);

        sql = "select cm_created_month, count(*) from alfresco where cm_created > '" + start.toString() + "' group by cm_created_month";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Month(buckets, false, true, start, end, false);

        // Start date exclusive, no end date specified
        solrStartDate = "/DAY-25DAYS";
        solrEndDate = DEFAULT_END_DATE_MONTH;
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets_Month(start, end);

        sql = "select cm_created_month, count(*) from alfresco where cm_created > 'NOW" + solrStartDate + "' group by cm_created_month";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Month(buckets, false, true, start, end, false);

        // Start date inclusive, no end date specified
        endDate = parseDateMath(DEFAULT_END_DATE_MONTH);
        end = endDate.toInstant(UTC);
        startDate = endDate.toLocalDate().atStartOfDay().minus(1, MONTHS).minus(5, DAYS);
        start = startDate.toInstant(UTC);
        numberOfBuckets = calculateNumberOfBuckets_Month(start, end);

        sql = "select cm_created_month, count(*) from alfresco where cm_created >= '" + start.toString() + "' group by cm_created_month";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Month(buckets, true, true, start, end, false);

        // Start date inclusive, no end date specified
        solrStartDate = "-2MONTH/DAY+24HOURS";
        solrEndDate = DEFAULT_END_DATE_MONTH;
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets_Month(start, end);

        sql = "select cm_created_month, count(*) from alfresco where cm_created >= 'NOW" + solrStartDate + "' group by cm_created_month";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Month(buckets, true, true, start, end, false);

        // No start date specified, no end date specified
        start = parseDateMath(DEFAULT_START_DATE_MONTH).toInstant(UTC);
        end = parseDateMath(DEFAULT_END_DATE_MONTH).toInstant(UTC);
        numberOfBuckets = calculateNumberOfBuckets_Month(start, end);

        sql = "select cm_created_month, count(*) from alfresco group by cm_created_month";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Month(buckets, true, true, start, end, false);

        // Start date inclusive, end date exclusive
        startDate = LocalDateTime.of(currentYear + 1, 8, 3, 0, 0, 0);
        endDate = startDate.plus(1, YEARS).minus(7, MONTHS).minus(13, DAYS);
        start = startDate.toInstant(UTC);
        end = endDate.toInstant(UTC);
        numberOfBuckets = calculateNumberOfBuckets_Year(start, end);

        sql = "select cm_created_year, count(*) from alfresco where cm_created >= '" + start.toString() + "' and cm_created < '" + end.toString() + "' group by cm_created_year";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Year(buckets, true, false, start, end);

        // Start date inclusive, end date exclusive
        solrStartDate = "/YEAR-2YEARS+1MONTHS+3DAYS/DAY";
        solrEndDate = "/DAY+10MONTH-10DAYS";
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets_Year(start, end);

        sql = "select cm_created_year, count(*) from alfresco where cm_created >= 'NOW" + solrStartDate + "' and cm_created < 'NOW" + solrEndDate + "' group by cm_created_year";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Year(buckets, true, false, start, end);

        // Start date inclusive, end date inclusive
        startDate = LocalDateTime.of(currentYear, 2, 1, 0, 0, 0);
        endDate = startDate.plus(1, YEARS).plus(10, MONTHS).minus(30, DAYS);
        start = startDate.toInstant(UTC);
        end = endDate.toInstant(UTC);
        numberOfBuckets = calculateNumberOfBuckets_Year(start, end);

        sql = "select cm_created_year, count(*) from alfresco where cm_created >= '" + start.toString() + "' and cm_created <= '" + end.toString() + "' group by cm_created_year";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Year(buckets, true, true, start, end);

        // Start date inclusive, end date inclusive
        solrStartDate = "/DAY-3YEARS+12DAYS";
        solrEndDate = "/MONTH+4MONTH-20DAYS";
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets_Year(start, end);

        sql = "select cm_created_year, count(*) from alfresco where cm_created >= 'NOW" + solrStartDate + "' and cm_created <= 'NOW" + solrEndDate + "' group by cm_created_year";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Year(buckets, true, true, start, end);

        // Start date exclusive, end date inclusive
        startDate = LocalDateTime.of(currentYear - 1, 2, 10, 0, 0, 0);
        endDate = startDate.plus(10, MONTHS).plus(1, DAYS);
        start = startDate.toInstant(UTC);
        end = endDate.toInstant(UTC);
        numberOfBuckets = calculateNumberOfBuckets_Year(start, end);

        sql = "select cm_created_year, count(*) from alfresco where cm_created > '" + start.toString() + "' and cm_created <= '" + end.toString() + "' group by cm_created_year";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Year(buckets, false, true, start, end);

        // Start date exclusive, end date inclusive
        solrStartDate = "/YEAR+3DAYS-3MONTHS";
        solrEndDate = "+1MONTH/DAY-5DAYS";
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets_Year(start, end);

        sql = "select cm_created_year, count(*) from alfresco where cm_created > 'NOW" + solrStartDate + "' and cm_created <= 'NOW" + solrEndDate + "' group by cm_created_year";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Year(buckets, false, true, start, end);

        // Start date exclusive, end date exclusive
        startDate = LocalDateTime.of(currentYear, 8, 8, 0, 0, 0);
        endDate = startDate.plus(5, MONTHS).plus(10, DAYS);
        start = startDate.toInstant(UTC);
        end = endDate.toInstant(UTC);
        numberOfBuckets = calculateNumberOfBuckets_Year(start, end);

        sql = "select cm_created_year, count(*) from alfresco where cm_created > '" + start.toString() + "' and cm_created < '" + end.toString() + "' group by cm_created_year";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Year(buckets, false, false, start, end);

        // Start date exclusive, end date exclusive
        solrStartDate = "/YEAR-3MONTHS";
        solrEndDate = "+25DAYS/DAY+3MONTH";
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets_Year(start, end);

        sql = "select cm_created_year, count(*) from alfresco where cm_created > 'NOW" + solrStartDate + "' and cm_created < 'NOW" + solrEndDate + "' group by cm_created_year";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Year(buckets, false, false, start, end);

        // No start date specified, end date exclusive
        startDate = parseDateMath(DEFAULT_START_DATE_YEAR);
        endDate = startDate.plus(1, YEARS).plus(4, MONTHS).minus(30, DAYS);
        start = startDate.toInstant(UTC);
        end = endDate.toInstant(UTC);
        numberOfBuckets = calculateNumberOfBuckets_Year(start, end);

        sql = "select cm_created_year, count(*) from alfresco where cm_created < '" + end.toString() + "' group by cm_created_year";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Year(buckets, true, false, start, end);

        // No start date specified, end date exclusive
        solrStartDate = DEFAULT_START_DATE_YEAR;
        solrEndDate = "/YEAR+23DAY+2MONTH";
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets_Year(start, end);

        sql = "select cm_created_year, count(*) from alfresco where cm_created < 'NOW" + solrEndDate + "' group by cm_created_year";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Year(buckets, true, false, start, end);

        // No start date specified, end date inclusive
        startDate = parseDateMath(DEFAULT_START_DATE_YEAR);
        endDate = startDate.plus(2, YEARS).minus(3, MONTHS).plus(2, DAYS);
        start = startDate.toInstant(UTC);
        end = endDate.toInstant(UTC);
        numberOfBuckets = calculateNumberOfBuckets_Year(start, end);

        sql = "select cm_created_year, count(*) from alfresco where cm_created <= '" + end.toString() + "' group by cm_created_year";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Year(buckets, true, true, start, end);

        // No start date specified, end date inclusive
        solrStartDate = DEFAULT_START_DATE_YEAR;
        solrEndDate = "/DAY-35DAYS";
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets_Year(start, end);

        sql = "select cm_created_year, count(*) from alfresco where cm_created <= 'NOW" + solrEndDate + "' group by cm_created_year";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Year(buckets, true, true, start, end);

        // Start date exclusive, no end date specified
        endDate = parseDateMath(DEFAULT_END_DATE_YEAR);
        end = endDate.toInstant(UTC);
        startDate = endDate.toLocalDate().atStartOfDay().minus(12, MONTHS).plus(5, DAYS);
        start = startDate.toInstant(UTC);
        numberOfBuckets = calculateNumberOfBuckets_Year(start, end);

        sql = "select cm_created_year, count(*) from alfresco where cm_created > '" + start.toString() + "' group by cm_created_year";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Year(buckets, false, true, start, end, false);

        // Start date exclusive, no end date specified
        solrStartDate = "/DAY-5DAYS-1YEAR";
        solrEndDate = DEFAULT_END_DATE_YEAR;
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets_Year(start, end);

        sql = "select cm_created_year, count(*) from alfresco where cm_created > 'NOW" + solrStartDate + "' group by cm_created_year";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Year(buckets, false, true, start, end, false);

        // Start date inclusive, no end date specified
        endDate = parseDateMath(DEFAULT_END_DATE_YEAR);
        end = endDate.toInstant(UTC);
        startDate = endDate.toLocalDate().atStartOfDay().minus(2, YEARS).plus(3, MONTHS).minus(15, DAYS);
        start = startDate.toInstant(UTC);
        numberOfBuckets = calculateNumberOfBuckets_Year(start, end);

        sql = "select cm_created_year, count(*) from alfresco where cm_created >= '" + start.toString() + "' group by cm_created_year";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Year(buckets, true, true, start, end, false);

        // Start date inclusive, no end date specified
        solrStartDate = "-2YEARS-2MONTH/DAY+24HOURS+2MONTHS";
        solrEndDate = DEFAULT_END_DATE_YEAR;
        start = dateMathParser.parseMath(solrStartDate).toInstant();
        end = dateMathParser.parseMath(solrEndDate).toInstant();
        numberOfBuckets = calculateNumberOfBuckets_Year(start, end);

        sql = "select cm_created_year, count(*) from alfresco where cm_created >= 'NOW" + solrStartDate + "' group by cm_created_year";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Year(buckets, true, true, start, end, false);

        // No start date specified, no end date specified
        start = parseDateMath(DEFAULT_START_DATE_YEAR).toInstant(UTC);
        end = parseDateMath(DEFAULT_END_DATE_YEAR).toInstant(UTC);
        numberOfBuckets = calculateNumberOfBuckets_Year(start, end);

        sql = "select cm_created_year, count(*) from alfresco group by cm_created_year";
        buckets = executeQuery(sql);
        bucketSize = buckets.size();

        assertBucketSize(numberOfBuckets, bucketSize);
        assertExpectedBucketContent_Year(buckets, true, true, start, end, false);
    }

    @Before
    private void createData() throws Exception
    {
        print("Creating test data...");

        TimeZone.setDefault(TimeZone.getTimeZone(zoneId));

        int year = currentYear - 3;
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

        print("\n" + "Data created for _day: " + createdDay);
        print("\n" + "Data created for _month: " + createdMonth);
        print("\n" + "Data created for _year: " + createdYear);

        print("\n" + "Start indexing test data...");
        indexTransaction(txn, nodes, nodeMetaDatas);
        waitForDocCount(new TermQuery(new Term("content@s___t@{http://www.alfresco.org/model/content/1.0}content", "world")), totalNumberOfDocuments - failedDateCount + 4, 200000);
        print("\n" + "Test data indexing completed..." + "\n");
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
        int numberOfBuckets = (int) Math.ceil(days);
        return numberOfBuckets > 0 ? numberOfBuckets : 0;
    }

    private int calculateNumberOfBuckets_Month(Instant startDate, Instant endDate)
    {
        LocalDateTime difference = difference(startDate, endDate);

        if (difference.getHour() > 0 || difference.getMinute() > 0 || difference.getSecond() > 0 || difference.getNano() > 0)
        {
            difference = difference.plusDays(1).withHour(0).withMinute(0).withSecond(0).withNano(0);
        }

        int numberOfBuckets = difference.getYear() * 12 + difference.getMonthValue() + (difference.getDayOfMonth() > 0 ? 1 : 0);
        return numberOfBuckets > 0 ? numberOfBuckets : 0;
    }

    private int calculateNumberOfBuckets_Year(Instant startDate, Instant endDate)
    {
        LocalDateTime difference = difference(startDate, endDate);

        if (difference.getDayOfMonth() > 0 || difference.getHour() > 0 || difference.getMinute() > 0 || difference.getSecond() > 0 || difference.getNano() > 0)
        {
            difference = difference.plusMonths(1).withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0).withNano(0);
        }

        int numberOfBuckets = difference.getYear() + (difference.getMonthValue() > 0 ? 1 : 0);
        return numberOfBuckets > 0 ? numberOfBuckets : 0;
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

    private LocalDateTime parseDateMath(String expression) throws ParseException
    {
        Date date = dateMathParser.parseMath(expression);
        return LocalDateTime.ofInstant(date.toInstant(), zoneId);
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
        assertExpectedBucketContent_Day(buckets, startInclusive, endInclusive, start, end, true);
    }

    private void assertExpectedBucketContent_Day(List<Tuple> buckets, boolean startInclusive, boolean endInclusive, Instant start, Instant end, boolean endDateSpecified)
    {
        assertExpectedBucketContent(buckets, startInclusive, endInclusive, start, end, endDateSpecified, "day", createdDay);
    }

    private void assertExpectedBucketContent_Month(List<Tuple> buckets, boolean startInclusive, boolean endInclusive, Instant start, Instant end)
    {
        assertExpectedBucketContent_Month(buckets, startInclusive, endInclusive, start, end, true);
    }

    private void assertExpectedBucketContent_Month(List<Tuple> buckets, boolean startInclusive, boolean endInclusive, Instant start, Instant end, boolean endDateSpecified)
    {
        assertExpectedBucketContent(buckets, startInclusive, endInclusive, start, end, endDateSpecified, "month", createdMonth);
    }

    private void assertExpectedBucketContent_Year(List<Tuple> buckets, boolean startInclusive, boolean endInclusive, Instant start, Instant end)
    {
        assertExpectedBucketContent_Year(buckets, startInclusive, endInclusive, start, end, true);
    }

    private void assertExpectedBucketContent_Year(List<Tuple> buckets, boolean startInclusive, boolean endInclusive, Instant start, Instant end, boolean endDateSpecified)
    {
        assertExpectedBucketContent(buckets, startInclusive, endInclusive, start, end, endDateSpecified, "year", createdYear);
    }

    private void assertExpectedBucketContent(List<Tuple> buckets, boolean startInclusive, boolean endInclusive, Instant start, Instant end, boolean endDateSpecified, String type, Map<String, Integer> createdDocumentsMap)
    {
        LocalDateTime endDate = LocalDateTime.ofInstant(end, zoneId);
        LocalDateTime startDate = LocalDateTime.ofInstant(start, zoneId);

        print("\n"+ "Start date: " + start);
        print("End date: " + end);
        print("Difference between end date and start date: " + difference(start, end));

        ListIterator<Tuple> iterator = buckets.listIterator();
        int counter = 0;
        while (iterator.hasNext())
        {
            boolean hasPrevious = iterator.hasPrevious();
            Tuple tuple = iterator.next();
            boolean hasNext = iterator.hasNext();
            String createdDate = tuple.getString("cm_created_" + type);
            long count = tuple.getLong("EXPR$1").longValue();

            print("\n"+ "Creation date: " + createdDate + ".");

            Integer createdDocuments = createdDocumentsMap.get(createdDate);
            if (createdDocuments == null && buckets.size() == 1)
            {
                assertEquals(0, count);
                continue;
            }

            LocalDateTime startRange;
            LocalDateTime endRange;
            int numberOfCreatedDocuments;
            if (!hasNext)
            {
                int range = endInclusive ? 1 : 0;
                if (buckets.size() == 1)
                {
                    if (startInclusive && endInclusive)
                    {
                        range = 1;
                    }
                    else if (!startInclusive && !endInclusive)
                    {
                        range = -1;
                    }
                    else
                    {
                        range = 0;
                    }
                }

                if (endDateSpecified)
                {
                    startRange = addPeriod(startDate, counter++, type);
                    endRange = endDate;
                    numberOfCreatedDocuments = getTotalNumberOfDocumentsForRange(startRange, endRange);
                    assertBucketContentSize(numberOfCreatedDocuments > 0 ? numberOfCreatedDocuments + range : numberOfCreatedDocuments, count);
                }
                else
                {
                    assertBucketContentSize(createdDocuments + range, count);
                }
            }
            else if (!hasPrevious)
            {
                startRange = addPeriod(startDate, counter++, type);
                endRange = addPeriod(startDate, counter, type);
                int range = startInclusive ? 0 : -1;
                numberOfCreatedDocuments = getTotalNumberOfDocumentsForRange(startRange, endRange);
                assertBucketContentSize(numberOfCreatedDocuments > 0 ? numberOfCreatedDocuments + range : numberOfCreatedDocuments, count);
            }
            else
            {
                startRange = addPeriod(startDate, counter++, type);
                endRange = addPeriod(startDate, counter, type);
                numberOfCreatedDocuments = getTotalNumberOfDocumentsForRange(startRange, endRange);
                assertBucketContentSize(numberOfCreatedDocuments, count);
            }
        }

        print("************************************************************************************" + "\n");
    }

    private LocalDateTime addPeriod(LocalDateTime date, int period, String type)
    {
        LocalDateTime result = null;

        if (type.equals("day"))
        {
            result = date.plusDays(period);
        }
        else if (type.equals("month"))
        {
            result = date.plusMonths(period);
        }
        else if (type.equals("year"))
        {
            result = date.plusYears(period);
        }
        else
        {
            throw new IllegalArgumentException("Type '" + type + "' is not support. Allowed values are: day, month or year!");
        }

        return result;
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
