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

import java.io.IOException;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;

import org.alfresco.model.ContentModel;
import org.alfresco.service.cmr.repository.datatype.DefaultTypeConverter;
import org.alfresco.solr.client.ContentPropertyValue;
import org.alfresco.solr.client.Node;
import org.alfresco.solr.client.NodeMetaData;
import org.alfresco.solr.client.StringPropertyValue;
import org.alfresco.solr.client.Transaction;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.io.Tuple;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Joel
 */
@SolrTestCaseJ4.SuppressSSL
@LuceneTestCase.SuppressCodecs({"Appending","Lucene3x","Lucene40","Lucene41","Lucene42","Lucene43", "Lucene44", "Lucene45","Lucene46","Lucene47","Lucene48","Lucene49"})
public class DistributedSqlTimeSeriesTest extends AbstractStreamTest
{
    private final Map<Integer, Integer> dayCount = new HashMap<>();
    private final Map<Integer, Integer> daySum = new HashMap<>();

    private final Map<String, Integer> dayCount2 = new HashMap<>();
    private final Map<String, Integer> yearCount = new HashMap<>();
    private final Map<String, Integer> monthCount = new HashMap<>();

    private long contentSize = 100;
    private int numDocs = 250;
    private String alfrescoJson = "{ \"authorities\": [ \"jim\", \"joel\" ], \"tenants\": [ \"\" ] }";

    @BeforeClass
    private static void initData() throws Throwable
    {
        initSolrServers(2, getClassName(), null);
    }

    @Before
    private void loadData() throws Exception
    {
        loadTimeSeriesData();
    }
    
    @AfterClass
    private static void destroyData()
    {
        dismissSolrServers();
    }

    @Test
    public void testSearch() throws Exception
    {
        //Time period 2010
        String sql = "select cm_created_day, count(*), sum(cm_fiveStarRatingSchemeTotal), avg(cm_fiveStarRatingSchemeTotal), min(cm_fiveStarRatingSchemeTotal), max(cm_fiveStarRatingSchemeTotal) from alfresco where cm_owner='jim' and cm_content='hello world' and cm_created >= '2010-02-01T01:01:01Z' and cm_created <= '2010-02-14T23:59:59Z' group by cm_created_day";
        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);

        assertEquals(14, tuples.size());

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

        sql = "select " +
                "cm_created_month as ReferenceMonth, " +
                "sum(exif_exposureTime) as TotalExposureTime " +
                "from alfresco " +
                "where " +
                "exif_exposureTime > 0 and " +
                "cm_created >= '2010-02-01T01:01:01Z' " +
                "group by cm_created_month " +
                "order by sum(exif_exposureTime) desc";

        tuples = sqlQuery(sql, alfrescoJson);
        tuples.forEach(tuple ->
        {
            String referenceMonth = tuple.getString("ReferenceMonth");
            switch(referenceMonth)
            {
                case "2010-02":
                    assertEquals(2500.0, tuple.getDouble("TotalExposureTime"), 0);
                    break;
                default:
                    assertEquals(0, tuple.getDouble("TotalExposureTime"), 0);
            }
        });
    }

    @Test
    public void timeseries_customFieldDate_shouldBuildTimeseries() throws Exception
    {
        //Time period 2010
        String sql = "select expense_Recorded_At_day, count(*), sum(cm_fiveStarRatingSchemeTotal), avg(cm_fiveStarRatingSchemeTotal), min(cm_fiveStarRatingSchemeTotal), max(cm_fiveStarRatingSchemeTotal) from alfresco where cm_owner='jim' and cm_content='hello world' and expense_Recorded_At >= '2010-02-01T01:01:01Z' and expense_Recorded_At <= '2010-02-14T23:59:59Z' group by expense_Recorded_At_day";
        //String sql = "select expense_Recorded_At, cm_created from alfresco where cm_created >= '2010-02-01T01:01:01Z' and cm_created <= '2010-02-14T23:59:59Z'";

        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);

        assertEquals(14, tuples.size());

        for(Tuple tuple : tuples) {
            String dayString = tuple.getString("expense_Recorded_At_day");
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

        sql = "select " +
            "expense_Recorded_At_month as ReferenceMonth, " +
            "sum(exif_exposureTime) as TotalExposureTime " +
            "from alfresco " +
            "where " +
            "exif_exposureTime > 0 and " +
            "expense_Recorded_At >= '2010-02-01T01:01:01Z' " +
            "group by expense_Recorded_At_month " +
            "order by sum(exif_exposureTime) desc";

        tuples = sqlQuery(sql, alfrescoJson);
        tuples.forEach(tuple ->
        {
            String referenceMonth = tuple.getString("ReferenceMonth");
            switch(referenceMonth)
            {
                case "2010-02":
                    assertEquals(2500.0, tuple.getDouble("TotalExposureTime"), 0);
                    break;
                default:
                    assertEquals(0, tuple.getDouble("TotalExposureTime"), 0);
            }
        });
    }

    @Test
    public void timeseries_phrases_shouldReturnCorrectResults() throws IOException
    {
        //Test that phrases are working
        String sql = "select cm_created_day, count(*), sum(cm_fiveStarRatingSchemeTotal), avg(cm_fiveStarRatingSchemeTotal), min(cm_fiveStarRatingSchemeTotal), max(cm_fiveStarRatingSchemeTotal) from alfresco where cm_owner='jim' and cm_content='world hello' and cm_created >= '2010-02-01T01:01:01Z' and cm_created <= '2010-02-14T23:59:59Z' group by cm_created_day";
        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);
        assertEquals(0, tuples.size());


        sql = "select cm_created_day, count(*) as ct, sum(cm_fiveStarRatingSchemeTotal) as sm, avg(cm_fiveStarRatingSchemeTotal) as av, min(cm_fiveStarRatingSchemeTotal) as mn, max(cm_fiveStarRatingSchemeTotal) as mx from alfresco where cm_owner='jim' and cm_created >= '2010-02-01T01:01:01Z' and cm_created <= '2010-02-14T23:59:59Z' group by cm_created_day";
        tuples = sqlQuery(sql, alfrescoJson);
        assertEquals(14, tuples.size());
        for(Tuple tuple : tuples)
        {
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
    }
    
    @Test
    public void timeseries_descDayOrder() throws IOException
    {
        // Test desc day order
        String sql = "select cm_created_day, count(*) as ct, sum(cm_fiveStarRatingSchemeTotal) as sm, avg(cm_fiveStarRatingSchemeTotal) as av, min(cm_fiveStarRatingSchemeTotal) as mn, max(cm_fiveStarRatingSchemeTotal) as mx from alfresco where cm_owner='jim' and cm_created >= '2010-02-01T01:01:01Z' and cm_created <= '2010-02-14T23:59:59Z' group by cm_created_day order by cm_created_day desc";
        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);
        assertEquals(14, tuples.size());
        String lastDay = "3000-12-01";
        for(Tuple tuple : tuples)
        {
            String dayString = tuple.getString("cm_created_day");
            assertTrue(dayString.compareTo(lastDay) < 0);
            lastDay = dayString;
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
    }

    @Test
    public void timeseries_customFieldDate_descDayOrder() throws IOException
    {
        // Test desc day order
        String sql = "select expense_Recorded_At_day, count(*) as ct, sum(cm_fiveStarRatingSchemeTotal) as sm, avg(cm_fiveStarRatingSchemeTotal) as av, min(cm_fiveStarRatingSchemeTotal) as mn, max(cm_fiveStarRatingSchemeTotal) as mx from alfresco where cm_owner='jim' and expense_Recorded_At >= '2010-02-01T01:01:01Z' and expense_Recorded_At <= '2010-02-14T23:59:59Z' group by expense_Recorded_At_day order by expense_Recorded_At_day desc";
        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);
        assertEquals(14, tuples.size());
        String lastDay = "3000-12-01";
        for(Tuple tuple : tuples)
        {
            String dayString = tuple.getString("expense_Recorded_At_day");
            assertTrue(dayString.compareTo(lastDay) < 0);
            lastDay = dayString;
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
    }
    
    @Test
    public void timeseries_descAggregationOrder() throws IOException
    {
        // Test desc aggregation order
        String sql = "select cm_created_day, sum(audio_trackNumber) as sm from alfresco where cm_owner='jim' and cm_created >= '2010-02-01T01:01:01Z' and cm_created <= '2010-02-14T23:59:59Z' group by cm_created_day order by sum(audio_trackNumber) desc";
        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);
        assertEquals(14, tuples.size());
        double lastSum = Double.MAX_VALUE;
        for(Tuple tuple : tuples)
        {
            String dayString = tuple.getString("cm_created_day");
            int day = Integer.parseInt(dayString.split("-")[2]);
            double indexSum = daySum.get(day).doubleValue();
            double sum = tuple.getDouble("sm");
            assertEquals(indexSum, sum, 0.0);

            // Asserts that the current tuple is respecting the descending order
            assertTrue(lastSum >= sum);
            lastSum = sum;
        } 
    }
    
    
    @Test
    public void timeseries_rangeBeyondAvailableData_shouldReturn0AggregationBucketsForNoData() throws IOException
    {
        // In this test the range goes beyond the available data in the index. 
        // This will cause tuples to be generated for the full range, with
        // a value of 0 in the aggregation fields for buckets with no data available.
        String sql = "select cm_created_day, sum(audio_trackNumber) as sm from alfresco where cm_owner='jim' and cm_created >= '2010-02-01T01:01:01Z' and cm_created <= '2010-02-27T23:59:59Z' group by cm_created_day order by sum(audio_trackNumber) desc";
        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);
        assertEquals(27, tuples.size());
        for(Tuple tuple : tuples)
        {
            String dayString = tuple.getString("cm_created_day");
            assertTrue(tuple.fields.containsKey("sm"));
            if(dayString.equals("2010-02-21"))
            {
                assertEquals(0L, tuple.get("sm"));
            }
        }
    }
    
    
    @Test
    public void timeseries_1recordDaySums() throws IOException
    {
        //Test having

        //Get 1 record from the daySums

        int havingValue = -1;
        for(Map.Entry entry : daySum.entrySet())
        {
            havingValue = (Integer)entry.getValue();
        }

        String sql = "select cm_created_day, sum(audio_trackNumber) as sm from alfresco where cm_owner='jim' and cm_created >= '2010-02-01T01:01:01Z' and cm_created <= '2010-02-14T23:59:59Z' group by cm_created_day having sum(audio_trackNumber) ="+havingValue;
        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);
        assertEquals(1, tuples.size());
        for(Tuple tuple : tuples)
        {
            String dayString = tuple.getString("cm_created_day");
            int day = Integer.parseInt(dayString.split("-")[2]);
            double indexSum = daySum.get(day).doubleValue();
            double sum = tuple.getDouble("sm");
            assertEquals(indexSum, sum, 0.0);
        }  
    }
    
    
    @Test
    public void timeseries_dateMath() throws IOException
    {
        //Test the date math
        String sql = "select cm_created_day, count(*) as ct from alfresco where cm_owner='vigo' and cm_created >= 'NOW/DAY-11DAYS' group by cm_created_day";
        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);
        assertEquals(12, tuples.size());

        for(Tuple tuple : tuples)
        {
            String dayString = tuple.getString("cm_created_day");
            int indexedCount = dayCount2.get(dayString);
            long count = tuple.getLong("ct");
            assertEquals(indexedCount, count);
        }
    }
    
    
    @Test
    public void timeseries_noDatePredicate_shouldDefaultToLastMonth() throws Exception
    {
        //Test no date predicate / should default to past 1 month
        String sql = "select cm_created_day, count(*) as ct from alfresco where cm_owner='vigo' group by cm_created_day";
        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);
        LocalDate now = LocalDate.now();
        // end date is exclusive for method "until" so we have to add 1 day
        assertEquals(tuples.size(), (now.minusMonths(1).until(now, ChronoUnit.DAYS) + 1));

        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calendar.setTime(new Date());

        for(int i=tuples.size()-1; i>=0; --i)
        {
            Tuple t = tuples.get(i);
            assertTrue(thisDay(t.getString("cm_created_day"), calendar));
            calendar.add(Calendar.DAY_OF_MONTH, -1);
        }
    }
    
    @Test
    public void testYearTimeGrain() throws IOException
    {
        //Test year time grain

        String sql = "select cm_created_year, count(*) as ct from alfresco where cm_owner = 'morton' and cm_created >= 'NOW/YEAR-4YEARS' group by cm_created_year";
        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);

        assertEquals(5, tuples.size());

        for(Tuple tuple : tuples)
        {
            String dayString = tuple.getString("cm_created_year");
            int indexedCount = yearCount.get(dayString);
            long count = tuple.getLong("ct");
            assertEquals(indexedCount, count);
        } 
    }
    
    @Test
    public void testMonthTimeGrain() throws IOException
    {
        //Test month time grain
        String sql = "select cm_created_month, count(*) as ct from alfresco where cm_owner = 'jimmy' and cm_created >= 'NOW/MONTH-6MONTHS' group by cm_created_month";
        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);

        assertEquals(7, tuples.size());

        for(Tuple tuple : tuples)
        {
            String monthString = tuple.getString("cm_created_month");
            int indexedCount = monthCount.get(monthString);
            long count = tuple.getLong("ct");
            assertEquals(indexedCount, count);
        }
    }
    
    @Test
    public void testNoStartPredicate() throws Exception
    {
        //Test no start predicate
        String sql = "select cm_created_month, count(*) as ct from alfresco where cm_owner = 'jimmy' group by cm_created_month";
        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);
        assertEquals(25, tuples.size());

        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calendar.setTime(new Date());

        for(int i=tuples.size()-1; i>=0; --i)
        {
            Tuple t = tuples.get(i);
            assertTrue(thisMonth(t.getString("cm_created_month"), calendar));
            calendar.add(Calendar.MONTH, -1);
        }
    }

    @Test
    public void testNoStartPredicate2() throws Exception
    {
        //Test no start predicate

        String sql = "select cm_created_year, count(*) as ct from alfresco where cm_owner = 'morton' group by cm_created_year";
        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);

        assertEquals(6, tuples.size());

        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calendar.setTime(new Date());

        for(int i=tuples.size()-1; i>=0; --i)
        {
            Tuple t = tuples.get(i);
            assertTrue(thisYear(t.getString("cm_created_year"), calendar));
            calendar.add(Calendar.YEAR, -1);
        }
    }
    
    @Test
    public void testSearch639() throws IOException
    {
        // SEARCH-639: Test sum(`cm:content.size`)
        int numberOfYears = 4;
        String sql = String.format("select cm_created_month, sum(`cm:content.size`), max(`cm:content.size`) from alfresco where cm_created >= 'NOW/YEAR-%sYEARS' group by cm_created_month", numberOfYears);
        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);
        DateTime dateTime = new DateTime();
        assertEquals(tuples.size(), (numberOfYears * 12 + dateTime.getMonthOfYear()));

        // We will only check last year as the content size is only added as part of loadTimeSeriesData3() which adds content
        // every year in December. If we would use the current year the total sum would be different when the test is run in
        // December this year. In order to avoid test failures we check documents created last year in December.
        int lastYear = dateTime.getYear() - 1;
        long sumTotal = 0;
        long maxTotal = 0;

        for (Tuple tuple : tuples)
        {
            Long sum = tuple.getLong("EXPR$1");
            Long max = tuple.getLong("EXPR$2");

            if (tuple.getString("cm_created_month").startsWith(String.valueOf(lastYear)))
            {
                if (sum != null)
                {
                    sumTotal += sum;
                }

                if (max != null)
                {
                    maxTotal += max;
                }
            }
        }

        int lastYearCount = yearCount.get(String.valueOf(lastYear));

        // The number of documents created last year in December is "numDocs / (numberOfYears + 1)"
        assertEquals((lastYearCount * contentSize), sumTotal);
        // The max total of documents added last year in December must be equal to the contentSize defined.
        assertEquals(contentSize, maxTotal);
    }

    private boolean thisDay(String YYYY_MM_DD, Calendar calendar) throws Exception
    {
        String[] parts = YYYY_MM_DD.split("-");
        int year = Integer.parseInt(parts[0]);
        int month = Integer.parseInt(parts[1]);
        int day = Integer.parseInt(parts[2]);

        int _year = calendar.get(Calendar.YEAR);
        int _month = calendar.get(Calendar.MONTH)+1;
        int _day = calendar.get(Calendar.DAY_OF_MONTH);

        if(year != _year)
        {
            throw new Exception("Invalid year:"+year);
        }

        if(month != _month)
        {
            throw new Exception("Invalid year:"+month);
        }

        if(day != _day)
        {
            throw new Exception("Invalid year:"+day);
        }

        return true;
    }

    private boolean thisMonth(String YYYY_MM, Calendar calendar) throws Exception
    {
        String[] parts = YYYY_MM.split("-");
        int year = Integer.parseInt(parts[0]);
        int month = Integer.parseInt(parts[1]);

        int _year = calendar.get(Calendar.YEAR);
        int _month = calendar.get(Calendar.MONTH)+1;

        if(year != _year)
        {
            throw new Exception("Invalid year:"+year);
        }

        if(month != _month)
        {
            throw new Exception("Invalid year:"+month);
        }

        return true;
    }

    private boolean thisYear(String YYYY, Calendar calendar) throws Exception
    {
        int year = Integer.parseInt(YYYY);

        int _year = calendar.get(Calendar.YEAR);

        if(year != _year)
        {
            throw new Exception("Invalid year:"+year);
        }

        return true;
    }


    private void loadTimeSeriesData() throws Exception
    {
        Random random = random();

        Transaction txn = getTransaction(0, numDocs);

        List<Node> nodes = new ArrayList<>();
        List<NodeMetaData> nodeMetaDatas = new ArrayList<>();

        for(int i=0; i<numDocs; i++)
        {
            int track = random.nextInt(500);

            Node node = getNode(txn, acl, Node.SolrApiNodeStatus.UPDATED);
            nodes.add(node);
            NodeMetaData nodeMetaData1 = getNodeMetaData(node, txn, acl, "mike", null, false);
            int day = (i%14)+1;

            if(dayCount.containsKey(day))
            {
                Integer count = dayCount.get(day);
                dayCount.put(day, count + 1);
            }
            else
            {
                dayCount.put(day, 1);
            }

            if(daySum.containsKey(day))
            {
                Integer sum = daySum.get(day);
                daySum.put(day, sum + track);
            }
            else
            {
                daySum.put(day, track);
            }

            Date date1 = getDate(2010, 1, day);


            nodeMetaData1.getProperties().put(ContentModel.PROP_CREATED, new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, date1)));
            nodeMetaData1.getProperties().put(PROP_CUSTOM_EXPENSE_MODEL_DATE, new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, date1)));
            nodeMetaData1.getProperties().put(PROP_RATING, new StringPropertyValue("10"));
            nodeMetaData1.getProperties().put(PROP_EXPOSURE_TIME, new StringPropertyValue("10.0"));
            nodeMetaData1.getProperties().put(PROP_TRACK, new StringPropertyValue(Integer.toString(track)));
            nodeMetaData1.getProperties().put(PROP_MANUFACTURER, new StringPropertyValue("Nikon"));
            nodeMetaData1.getProperties().put(ContentModel.PROP_NAME, new StringPropertyValue("name1"));
            nodeMetaData1.getProperties().put(ContentModel.PROP_TITLE, new StringPropertyValue("title1"));
            nodeMetaData1.getProperties().put(ContentModel.PROP_CREATOR, new StringPropertyValue("creator1"));
            nodeMetaData1.getProperties().put(ContentModel.PROP_OWNER, new StringPropertyValue("jim"));
            nodeMetaDatas.add(nodeMetaData1);
        }

        indexTransaction(txn, nodes, nodeMetaDatas);
        loadTimeSeriesData2(numDocs);
        loadTimeSeriesData3(numDocs);
        loadTimeSeriesData4(numDocs);
        waitForDocCount(new TermQuery(new Term("content@s___t@{http://www.alfresco.org/model/content/1.0}content", "world")), (numDocs * 4) + 4, 80000);
    }

    private void loadTimeSeriesData2(int numDocs)
    {
        Transaction txn = getTransaction(0, numDocs);

        List<Node> nodes = new ArrayList<>();
        List<NodeMetaData> nodeMetaDatas = new ArrayList<>();

        for(int i=0; i<numDocs; i++)
        {
            Node node = getNode(txn, acl, Node.SolrApiNodeStatus.UPDATED);
            nodes.add(node);
            NodeMetaData nodeMetaData1 = getNodeMetaData(node, txn, acl, "mike", null, false);
            int day = (i%14);

            Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            calendar.setTime(new Date());
            //calendar.clear();
            calendar.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DAY_OF_MONTH), 01, 01, 01);
            calendar.add(Calendar.DAY_OF_YEAR, -day);
            String key =
                    calendar.get(Calendar.YEAR) + "-" +
                            pad((calendar.get(Calendar.MONTH) + 1)) + "-" +
                            pad(calendar.get(Calendar.DAY_OF_MONTH));

            if(dayCount2.containsKey(key))
            {
                Integer count = dayCount2.get(key);
                dayCount2.put(key, count + 1);
            }
            else
            {
                dayCount2.put(key, 1);
            }

            nodeMetaData1.getProperties().put(ContentModel.PROP_CREATED, new StringPropertyValue(getSolrDay(calendar)));
            nodeMetaData1.getProperties().put(PROP_CUSTOM_EXPENSE_MODEL_DATE, new StringPropertyValue(getSolrDay(calendar)));
            nodeMetaData1.getProperties().put(PROP_RATING, new StringPropertyValue("10"));
            nodeMetaData1.getProperties().put(PROP_TRACK, new StringPropertyValue("12"));
            nodeMetaData1.getProperties().put(PROP_MANUFACTURER, new StringPropertyValue("Nikon"));
            nodeMetaData1.getProperties().put(ContentModel.PROP_NAME, new StringPropertyValue("name1"));
            nodeMetaData1.getProperties().put(ContentModel.PROP_TITLE, new StringPropertyValue("title1"));
            nodeMetaData1.getProperties().put(ContentModel.PROP_CREATOR, new StringPropertyValue("creator1"));
            nodeMetaData1.getProperties().put(ContentModel.PROP_OWNER, new StringPropertyValue("vigo"));
            nodeMetaDatas.add(nodeMetaData1);
        }

        indexTransaction(txn, nodes, nodeMetaDatas);

    }

    private String getSolrDay(Calendar cal)
    {
        return cal.get(Calendar.YEAR) + "-" + pad((cal.get(Calendar.MONTH)+1))+"-"+pad(cal.get(Calendar.DAY_OF_MONTH))+"T01:01:01Z";
    }

    private void loadTimeSeriesData3(int numDocs)
    {
        Transaction txn = getTransaction(0, numDocs);

        List<Node> nodes = new ArrayList<>();
        List<NodeMetaData> nodeMetaDatas = new ArrayList<>();

        for(int i=0; i<numDocs; i++)
        {
            Node node = getNode(txn, acl, Node.SolrApiNodeStatus.UPDATED);
            nodes.add(node);
            NodeMetaData nodeMetaData1 = getNodeMetaData(node, txn, acl, "mike", null, false);
            int year = (i%5);

            Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            calendar.setTime(new Date());
            calendar.set(calendar.get(Calendar.YEAR), 11, 31, 23, 59, 59);
            calendar.add(Calendar.YEAR, -year);
            String key = Integer.toString(calendar.get(Calendar.YEAR));

            if(yearCount.containsKey(key))
            {
                Integer count = yearCount.get(key);
                yearCount.put(key, count + 1);
            }
            else
            {
                yearCount.put(key, 1);
            }

            nodeMetaData1.getProperties().put(ContentModel.PROP_CREATED, new StringPropertyValue(getSolrDay(calendar)));
            nodeMetaData1.getProperties().put(PROP_CUSTOM_EXPENSE_MODEL_DATE, new StringPropertyValue(getSolrDay(calendar)));
            nodeMetaData1.getProperties().put(PROP_RATING, new StringPropertyValue("10"));
            nodeMetaData1.getProperties().put(PROP_TRACK, new StringPropertyValue("12"));
            nodeMetaData1.getProperties().put(PROP_MANUFACTURER, new StringPropertyValue("Nikon"));
            nodeMetaData1.getProperties().put(ContentModel.PROP_NAME, new StringPropertyValue("name1"));
            nodeMetaData1.getProperties().put(ContentModel.PROP_TITLE, new StringPropertyValue("title1"));
            nodeMetaData1.getProperties().put(ContentModel.PROP_CREATOR, new StringPropertyValue("creator1"));
            nodeMetaData1.getProperties().put(ContentModel.PROP_OWNER, new StringPropertyValue("morton"));
            nodeMetaData1.getProperties().put(ContentModel.PROP_CONTENT, new ContentPropertyValue(Locale.US, contentSize, "UTF-8", "text/plain", null));
            nodeMetaDatas.add(nodeMetaData1);
        }

        indexTransaction(txn, nodes, nodeMetaDatas);
    }


    private void loadTimeSeriesData4(int numDocs)
    {
        Transaction txn = getTransaction(0, numDocs);

        List<Node> nodes = new ArrayList<>();
        List<NodeMetaData> nodeMetaDatas = new ArrayList<>();

        for(int i = 0; i < numDocs; i++)
        {
            Node node = getNode(txn, acl, Node.SolrApiNodeStatus.UPDATED);
            nodes.add(node);
            NodeMetaData nodeMetaData1 = getNodeMetaData(node, txn, acl, "mike", null, false);
            int month = (i%12);

            Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            calendar.setTime(new Date());
            calendar.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), 1);
            calendar.add(Calendar.MONTH, -month);
            String key = calendar.get(Calendar.YEAR) + "-" +
                    pad((calendar.get(Calendar.MONTH) + 1));

            if(monthCount.containsKey(key))
            {
                Integer count = monthCount.get(key);
                monthCount.put(key, count + 1);
            }
            else
            {
                monthCount.put(key, 1);
            }

            nodeMetaData1.getProperties().put(ContentModel.PROP_CREATED, new StringPropertyValue(getSolrDay(calendar)));
            nodeMetaData1.getProperties().put(PROP_CUSTOM_EXPENSE_MODEL_DATE, new StringPropertyValue(getSolrDay(calendar)));
            nodeMetaData1.getProperties().put(PROP_RATING, new StringPropertyValue("10"));
            nodeMetaData1.getProperties().put(PROP_TRACK, new StringPropertyValue("12"));
            nodeMetaData1.getProperties().put(PROP_MANUFACTURER, new StringPropertyValue("Nikon"));
            nodeMetaData1.getProperties().put(ContentModel.PROP_NAME, new StringPropertyValue("name1"));
            nodeMetaData1.getProperties().put(ContentModel.PROP_TITLE, new StringPropertyValue("title1"));
            nodeMetaData1.getProperties().put(ContentModel.PROP_CREATOR, new StringPropertyValue("creator1"));
            nodeMetaData1.getProperties().put(ContentModel.PROP_OWNER, new StringPropertyValue("jimmy"));
            nodeMetaDatas.add(nodeMetaData1);
        }

        indexTransaction(txn, nodes, nodeMetaDatas);
    }

    private String pad(int i)
    {
        String s = Integer.toString(i);
        if(s.length() == 1)
        {
            s="0"+s;
        }

        return s;
    }
}