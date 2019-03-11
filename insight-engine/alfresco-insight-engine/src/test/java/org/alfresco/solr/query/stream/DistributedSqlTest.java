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

import static java.util.Arrays.asList;
import static java.util.Optional.of;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.IntStream.range;
import static org.apache.calcite.util.Pair.zip;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.util.Pair;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.io.Tuple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * @author Joel
 */
@SolrTestCaseJ4.SuppressSSL
@LuceneTestCase.SuppressCodecs({"Appending","Lucene3x","Lucene40","Lucene41","Lucene42","Lucene43", "Lucene44", "Lucene45","Lucene46","Lucene47","Lucene48","Lucene49"})
public class DistributedSqlTest extends AbstractStreamTest
{
    @Rule public ExpectedException exceptionRule = ExpectedException.none();

    private String sql = "select DBID, LID from alfresco where cm_content = 'world' order by DBID limit 10 ";
    private String alfrescoJson = "{ \"authorities\": [ \"jim\", \"joel\" ], \"tenants\": [ \"\" ] }";

    private static Properties getSQLFields()
    {
        Properties p = new Properties();
        p.put("solr.sql.alfresco.fieldname.cmlockOwner", "cm:lockOwner");
        p.put("solr.sql.alfresco.fieldtype.cmlockOwner", "solr.StrField");
        p.put("solr.sql.alfresco.fieldname.cmcreated","cm_created");
        p.put("solr.sql.alfresco.fieldtype.cmcreated","solr.TrieDateField");
        p.put("solr.sql.alfresco.fieldname.cmowner","cm_owner");
        p.put("solr.sql.alfresco.fieldtype.cmowner","solr.StrField");
        p.put("solr.sql.alfresco.fieldname.cmtitle","cm_title");
        p.put("solr.sql.alfresco.fieldtype.cmtitle","AlfrescoCollatableMLsolr.TextFieldType");
        p.put("solr.sql.alfresco.fieldname.aspect","ASPECT");
        p.put("solr.sql.alfresco.fieldtype.aspect","solr.StrField");
        p.put("solr.sql.alfresco.fieldname.type","TYPE");
        p.put("solr.sql.alfresco.fieldtype.type","solr.StrField");
        p.put("solr.sql.alfresco.fieldname.properties","PROPERTIES");
        p.put("solr.sql.alfresco.fieldtype.properties","solr.StrField");
        p.put("solr.sql.alfresco.fieldname.audioTrackNumber","audio:trackNumber");
        p.put("solr.sql.alfresco.fieldtype.audioTrackNumber","solr.TrieLongField");
        return p;
    }

    @BeforeClass
    public static void initData() throws Throwable
    {
        initSolrServers(1, getClassName(), getSQLFields());
        JettySolrRunner localJetty = jettyContainers.values().iterator().next();
        System.setProperty("solr.solr.home", localJetty.getSolrHome());
    }

    @AfterClass
    public static void destroyData()
    {
        dismissSolrServers();
    }

    @Test
    public void testSearch() throws Exception
    {
        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);
        assertEquals(4, tuples.size());
        assertNodes(tuples, node1, node2, node3, node4);
        assertFieldNotNull(tuples, "LID");
        String alfrescoJson2 = "{ \"authorities\": [ \"joel\" ], \"tenants\": [ \"\" ] }";

        tuples = sqlQuery(sql, alfrescoJson2);
        assertEquals(2, tuples.size());
        assertNodes(tuples, node1, node2);
        assertFieldNotNull(tuples, "LID");
        
        //SEARCH-679
        sql = "SELECT DBID,cm_created FROM alfresco order by `cm:created`";
        tuples = sqlQuery(sql, alfrescoJson2);
        assertNotNull(tuples);
        assertEquals(2, tuples.size());
        sql = "SELECT DBID,cm_created FROM alfresco order by cm_created";
        tuples = sqlQuery(sql, alfrescoJson2);
        assertNotNull(tuples);
        assertEquals(2, tuples.size());

        try
        {
            sql = "SELECT DBID,cm_created FROM alfresco order by cm_fake";
            sqlQuery(sql, alfrescoJson2);
        }
        catch (Exception e)
        {
            assertNotNull(e);
        }

        sql = "select DBID, LID from alfresco where `cm:content` = 'world' order by DBID limit 1";
        tuples = sqlQuery(sql, alfrescoJson2);
        assertEquals(1, tuples.size());
        assertFieldNotNull(tuples, "DBID");
        assertFieldNotNull(tuples, "LID");

        //This phrase search should find results
        sql = "select DBID, LID from alfresco where `cm:content` = 'hello world' order by DBID limit 1";
        tuples = sqlQuery(sql, alfrescoJson2);
        assertEquals(1, tuples.size());
        assertFieldNotNull(tuples, "DBID");
        assertFieldNotNull(tuples, "LID");

        //This query will be treated as a conjunction
        sql = "select DBID, LID from alfresco where `cm:content` = '(world hello)' order by DBID limit 1";
        tuples = sqlQuery(sql, alfrescoJson2);
        assertEquals(1, tuples.size());
        assertFieldNotNull(tuples, "DBID");
        assertFieldNotNull(tuples, "LID");

        //This phrase search should not find results
        sql = "select DBID, LID from alfresco where `cm:content` = 'world hello' order by DBID limit 1";
        tuples = sqlQuery(sql, alfrescoJson2);
        assertEquals(0, tuples.size());

        sql = "select DBID, LID from alfresco where `cm:content` = 'world' order by DBID";
        tuples = sqlQuery(sql, alfrescoJson2);
        assertEquals(2, tuples.size());
        assertFieldNotNull(tuples, "DBID");
        assertFieldNotNull(tuples, "LID");

        sql = "select DBID, LID from alfresco where `cm:content` = 'world'";
        tuples = sqlQuery(sql, alfrescoJson2);
        assertEquals(2, tuples.size());
        assertFieldNotNull(tuples, "DBID");
        assertFieldNotNull(tuples, "LID");

        sql = "select DBID, LID from alfresco";
        tuples = sqlQuery(sql, alfrescoJson2);
        assertEquals(2, tuples.size());
        assertFieldNotNull(tuples, "DBID");
        assertFieldNotNull(tuples, "LID");

        sql = "select TYPE, SITE from alfresco";
        tuples = sqlQuery(sql, alfrescoJson2);
        assertEquals(2, tuples.size());
        assertFieldNotNull(tuples, "TYPE");
        assertFieldNotNull(tuples, "SITE");

        sql = "select DBID from alfresco where cm_creator = 'creator1'";
        tuples = sqlQuery(sql, alfrescoJson);
        assertEquals(2, tuples.size());

        sql = "select DBID from alfresco where `cm:creator` = 'creator1'";
        List<Tuple> tuplesAgain = sqlQuery(sql, alfrescoJson);
        assertEquals(2, tuplesAgain.size());

        sql = "select DBID from alfresco where cm_created = '[2000 TO 2001]'";
        tuples = sqlQuery(sql, alfrescoJson);
        assertEquals(4, tuples.size());

        sql = "select DBID from alfresco where cm_fiveStarRatingSchemeTotal = '[4 TO 21]'";
        tuples = sqlQuery(sql, alfrescoJson);
        assertEquals(4, tuples.size());

        sql = "select DBID from alfresco where audio_trackNumber = '[5 TO 10>'";
        tuples = sqlQuery(sql, alfrescoJson);
        assertEquals(2, tuples.size());

        sql = "select DBID from alfresco where `audio:trackNumber` = '[4 TO 12]'";
        tuples = sqlQuery(sql, alfrescoJson);
        assertEquals(4, tuples.size());

        sql = "select DBID from alfresco where audio_trackNumber = '[5 TO 10]' AND cm_fiveStarRatingSchemeTotal = '[10 TO 12]'";
        tuples = sqlQuery(sql, alfrescoJson);
        assertEquals(1, tuples.size());

        //It will return null but, in the debugger, the syntax looks right.
        sql = "select DBID from alfresco where TYPE = 'content' AND NOT TYPE = 'fm:post'";
        tuples = sqlQuery(sql, alfrescoJson);
        assertEquals(0, tuples.size());

        sql = "select DBID from alfresco where `cm_content.mimetype` = 'text/plain'";
        tuples = sqlQuery(sql, alfrescoJson);
        assertEquals(3, tuples.size());

        sql = "select DBID from alfresco where `cm:content.mimetype` = 'text/javascript'";
        tuples = sqlQuery(sql, alfrescoJson);
        assertEquals(1, tuples.size());

        //Test negation
        sql = "select DBID from alfresco where `cm:content.mimetype` != 'text/javascript'";
        tuples = sqlQuery(sql, alfrescoJson);
        assertEquals(3, tuples.size());

        sql = "select DBID from alfresco where `cm:content.size` > 0";
        tuples = sqlQuery(sql, alfrescoJson);
        assertEquals(1, tuples.size());

        sql = "select DBID from alfresco where `cm:content.size` = '[1 TO *]'";
        tuples = sqlQuery(sql, alfrescoJson);
        assertEquals(1, tuples.size());

        sql = "select cm_creator, cm_name, `exif:manufacturer`, audio_trackNumber from alfresco order by `audio:trackNumber` asc";
        tuples = sqlQuery(sql, alfrescoJson2);
        assertEquals(2, tuples.size());

        for(Tuple tuple : tuples)
        {
            assertTrue(tuple.get("audio_trackNumber") instanceof Long);
            assertTrue(tuple.get("cm_creator") instanceof String);
            assertTrue(tuple.get("exif:manufacturer") instanceof String);
            assertTrue(tuple.get("cm_name") instanceof String);
        }

        Tuple t = tuples.get(0);
        assertEquals(8, (long) t.getLong("audio_trackNumber"));
        assertEquals("Nikon", t.getString("exif:manufacturer"));
        assertEquals("creator1", t.getString("cm_creator"));
        assertEquals("name2", t.getString("cm_name"));

        t = tuples.get(1);
        assertEquals(12, (long) t.getLong("audio_trackNumber"));
        assertEquals("Nikon", t.getString("exif:manufacturer"));
        assertEquals("creator1", t.getString("cm_creator"));
        assertEquals("name1", t.getString("cm_name"));


        sql = "select cm_creator, cm_name, `exif:manufacturer`, audio_trackNumber as atrack from alfresco order by `audio:trackNumber` desc";
        tuples = sqlQuery(sql, alfrescoJson2);
        assertEquals(2, tuples.size());

        for(Tuple tuple : tuples)
        {
            assertTrue(tuple.get("atrack") instanceof Long);
            assertTrue(tuple.get("cm_creator") instanceof String);
            assertTrue(tuple.get("exif:manufacturer") instanceof String);
            assertTrue(tuple.get("cm_name") instanceof String);
        }

        t = tuples.get(0);
        assertEquals(12, (long) t.getLong("atrack"));
        assertEquals("Nikon", t.getString("exif:manufacturer"));
        assertEquals("creator1", t.getString("cm_creator"));
        assertEquals("name1", t.getString("cm_name"));

        t = tuples.get(1);
        assertEquals(8, (long) t.getLong("atrack"));
        assertEquals("Nikon", t.getString("exif:manufacturer"));
        assertEquals("creator1", t.getString("cm_creator"));
        assertEquals("name2", t.getString("cm_name"));


        sql = "select `cm:name`, `cm:fiveStarRatingSchemeTotal` from alfresco";
        tuples = sqlQuery(sql, alfrescoJson2);
        assertEquals(2, tuples.size());
        for(Tuple tuple : tuples) {
            assertTrue(tuple.get("cm:fiveStarRatingSchemeTotal") instanceof Double);
            assertTrue(tuple.get("cm:name") instanceof String);
        }
        //Test sql and solr predicate
        sql = "select cm_creator from alfresco where _query_ = 'cm_creator:creator1'";
        tuples = sqlQuery(sql, alfrescoJson2);
        assertNotNull(tuples);
        assertEquals(2, tuples.size());
        for(Tuple tuple : tuples) {
            assertTrue(tuple.get("cm_creator") instanceof String);
            assertEquals("creator1", tuple.get("cm_creator"));
        }

        //Test select *
        sql = "select * from alfresco order by cm_created";
        assertResult(sqlQuery(sql, alfrescoJson2));
        
        //Test upper case
        assertResult(sqlQuery("SELECT * from alfresco", alfrescoJson2));
        assertResult(sqlQuery("select * FROM alfresco", alfrescoJson2));
        
        //Test * with fields that are not indexed.
        tuples = sqlQuery("select * from alfresco where TYPE ='cm:content'", alfrescoJson2);
        assertNotNull(tuples);
        
        tuples = sqlQuery("select * from alfresco where ASPECT ='cm:titled'", alfrescoJson);
        assertNotNull(tuples);
        
        tuples = sqlQuery("select * from alfresco where PROPERTIES ='title'", alfrescoJson);
        assertNotNull(tuples);
        //Test predefined fields
        tuples = sqlQuery("select * from alfresco where audio_trackNumber = '12'", alfrescoJson);
        assertNotNull(tuples);
        tuples = sqlQuery("select * from alfresco where `audio:trackNumber` = '12'", alfrescoJson);
        assertNotNull(tuples);
    }

    @Test 
    public void distributedSearch_customModelFieldInSharedProperties_shouldReturnCorrectResults() throws Exception
    {
        Set<String> expectedColumns = new HashSet<>(asList("Expense Name","finance_amount"));
        sql = "select cm_name as `Expense Name`, finance_amount from alfresco";

        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);
        assertEquals(indexedNodesCount, tuples.size());
        for (Tuple t : tuples)
        {
            assertEquals("Mismatched columns", expectedColumns, t.fields.keySet());
        }
    }

    @Test 
    public void distributedSearch_customModelFieldInSharedPropertiesQueryVariant_shouldReturnCorrectResults()
    {
        Set<String> expectedColumns = new HashSet<>(asList("Expense Name","finance:amount"));
        sql = "select cm_name as `Expense Name`, `finance:amount` from alfresco";

        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);
        assertEquals(indexedNodesCount, tuples.size());
        for (Tuple t : tuples)
        {
            assertEquals("Mismatched columns", expectedColumns, t.fields.keySet());
        }
    }

    @Test
    public void selectStarDefaultFieldsInPredicateAreCaseInsensitive()
    {
        long expectedCount = expectedCount("TYPE = '{http://www.alfresco.org/test/solrtest}testSuperType'");

        List<String> queries =
                asList(
                        "select cm_name,TYPE from alfresco where type = '{http://www.alfresco.org/test/solrtest}testSuperType'",
                        "select TYPE from alfresco where TyPe = '{http://www.alfresco.org/test/solrtest}testSuperType'",
                        "select * from alfresco where TYPE = '{http://www.alfresco.org/test/solrtest}testSuperType'");

        queries.stream()
                .map(query -> sqlQuery(query, alfrescoJson))
                .peek(tuples -> assertEquals("Wrong number of tuples in result", expectedCount, tuples.size()))
                .flatMap(Collection::stream)
                .map(tuple -> tuple.getString("TYPE"))
                .forEach(type -> assertEquals("{http://www.alfresco.org/test/solrtest}testSuperType", type));
    }

    @Test
    public void defaultModelFieldsInPredicateAreCaseInsensitive()
    {
        long expectedCount = expectedCount("cm_creator = 'creator1'");

        List<String> queries =
                asList(
                        "select cm_creator from alfresco where cm_creator = 'creator1'",
                        "select cm_creator,TYPE from alfresco where CM_CREATOR = 'creator1'",
                        "select * from alfresco where cM_CrEAtOR = 'creator1'");

        queries.stream()
                .map(query -> sqlQuery(query, alfrescoJson))
                .peek(tuples -> assertEquals("Wrong number of tuples in result", expectedCount, tuples.size()))
                .flatMap(Collection::stream)
                .map(tuple -> tuple.getString("cm_creator"))
                .forEach(type -> assertEquals("creator1", type));
    }

    @Test
    public void customFieldsInPredicateAreCaseInsensitive()
    {
        long expectedCount = expectedCount("finance_Emp = 'emp1'");

        List<String> queries =
                asList(
                        "select finance_Emp from alfresco where finance_Emp = 'emp1'",
                        "select finance_Emp,TYPE from alfresco where FINANCE_EMP = 'emp1'",
                        "select * from alfresco where FiNaNcE_eMP = 'emp1'");

        queries.stream()
                .map(query -> sqlQuery(query, alfrescoJson))
                .peek(tuples -> assertEquals("Wrong number of tuples in result", expectedCount, tuples.size()))
                .flatMap(Collection::stream)
                .map(tuple -> tuple.getString("finance_Emp"))
                .forEach(type -> assertEquals("emp1", type));
    }

    @Test
    public void fieldsInSelectListAreCaseSensitive()
    {
        List<String> selectLists = asList("CM_creator,TYPE,Finance_Emp", "cm_creator,TyPe,finance_Emp", "Cm_CrEaTor,type,FINANCE_EMP");

        String whereCondition = "TYPE = '{http://www.alfresco.org/test/solrtest}testSuperType' AND cm_creator='creator1' AND finance_Emp='emp1'";
        long expectedCount = expectedCount(whereCondition);

        List<String> queries =
                selectLists.stream()
                        .map(fields -> "select " + fields + " from alfresco where " + whereCondition)
                        .collect(toList());

        range(0, queries.size())
                .mapToObj(index -> Pair.of(index, queries.get(index)))
                .map(indexAndQuery -> Pair.of(indexAndQuery.left, sqlQuery(indexAndQuery.right, alfrescoJson)))
                .peek(indexAndTuples -> assertEquals("Wrong number of tuples in result", expectedCount, indexAndTuples.right.size()))
                .forEach(indexAndTuples -> {
                    final String [] selectFields = selectLists.get(indexAndTuples.left).split(",");
                    indexAndTuples.right
                            .forEach(tuple -> {
                                assertEquals("creator1", tuple.getString(selectFields[0]));
                                assertEquals("{http://www.alfresco.org/test/solrtest}testSuperType", tuple.getString(selectFields[1]));
                                assertEquals("emp1", tuple.getString(selectFields[2]));
                            });
                });
    }

    /**
     * Although, as this test name says, fields in expressions can be used in a case insensitive way, the same names
     * (with the same case) needs to be in the select lists so, as reported in {@link #fieldsInSelectListAreCaseSensitive()} test,
     * the returned tuples will include the requested fields with the *same exact case*.
     */
    @Test
    public void fieldsInExpressionsAreCaseInsensitive() {
        Set<String> expectedNames = asSet("name1", "name2", "name3");
        List<String> cmNameCases = asList("cm_name", "CM_NAME", "cM_nAmE");

        List<String> queries = cmNameCases.stream()
                .map(cmName ->
                        "select " +
                        cmName +
                        " from alfresco where TYPE = '{http://www.alfresco.org/test/solrtest}testSuperType' group by " +
                        cmName)
                .collect(Collectors.toList());

        List<List<Tuple>> results =
                queries.stream()
                        .map(query -> sqlQuery(query, alfrescoJson))
                        .peek(tuples -> assertEquals(3, tuples.size()))
                        .collect(toList());

        zip(cmNameCases, results)
                .forEach(fieldNameAndTuples -> {
                    String fieldName = fieldNameAndTuples.left;
                    Set<String> namesInResult =
                            fieldNameAndTuples.right.stream()
                                    .map(tuple -> tuple.getString(fieldName))
                                    .collect(toSet());
                    assertEquals(expectedNames, namesInResult);
                });
    }

    @Test
    public void fieldsInSortExpressionAreCaseInsensitive_descendingOrder()
    {
        String whereCondition = "TYPE = '{http://www.alfresco.org/test/solrtest}testSuperType'";
        long expectedCount = expectedCount(whereCondition);

        List<String> queriesInDescOrder =
                asList(
                        "select cm_name, TYPE, DBID from alfresco where " + whereCondition + " order by dbid desc",
                        "select * from alfresco where " + whereCondition + " order by DBID desc");

        List<List<String>> descendingResults =
                queriesInDescOrder.stream()
                    .map(query -> sqlQuery(query, alfrescoJson))
                    .peek(tuples -> assertEquals("Wrong number of tuples in result", expectedCount, tuples.size()))
                    .map(tuples -> tuples.stream().map(tuple -> tuple.getString("DBID")).collect(toList()))
                    .collect(toList());


        descendingResults.forEach(values -> {
            List<String> expected = new ArrayList<>(values);
            expected.sort(Comparator.reverseOrder());

            assertEquals(expected, values);
        });
    }

    @Test
    public void fieldsInSortExpressionAreCaseInsensitive_ascendingOrder()
    {
        String whereCondition = "TYPE = '{http://www.alfresco.org/test/solrtest}testSuperType'";
        long expectedCount = expectedCount(whereCondition);

        List<String> queriesInAscOrder =
                asList(
                        "select cm_name, TYPE, DBID from alfresco where " + whereCondition + " order by dbid asc",
                        "select * from alfresco where " + whereCondition + " order by DBID asc");

        List<List<String>> ascendingResults =
                queriesInAscOrder.stream()
                        .map(query -> sqlQuery(query, alfrescoJson))
                        .peek(tuples -> assertEquals("Wrong number of tuples in result", expectedCount, tuples.size()))
                        .map(tuples -> tuples.stream().map(tuple -> tuple.getString("DBID")).collect(toList()))
                        .collect(toList());

        ascendingResults.forEach(values -> {
            List<String> expected = new ArrayList<>(values);
            expected.sort(Comparator.naturalOrder());

            assertEquals(expected, values);
        });
    }

    @Test 
    public void distributedSearch_groupingOnCustomFieldDefinedInSharedProperties_shouldReturnCorrectResults()
        throws Exception
    {
        sql = "select finance_Emp from alfresco group by finance_Emp";
        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);
        assertEquals(2, tuples.size());
    }

    /**
     * Check if select * queries work when a custom field is given in predicate with wrong case.
     * "finance:location' instead of 'finance:Location'
     */
    @Test
    public void distributedSearch_selectStarQueries_PredicatesWithCaseInsensitiveFieldNames()
        throws Exception
    {
        sql = "select * from alfresco where `finance:location` = 'London'";
        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);
        assertEquals(1, tuples.size());
    }

    /**
     * Check if select * queries work when a custom field is given in predicate with wrong case.
     * "finance_location' instead of 'finance_Location'
     */
    @Test
    public void distributedSearch_selectStarQueries_PredicatesWithCaseInsensitiveFieldNames_formattedField()
        throws Exception
    {
        sql = "select * from alfresco where finance_location = 'London'";
        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);
        assertEquals(1, tuples.size());
    }

    /**
     * #distributedSearch_selectStarQueries_PredicatesWithCaseInsensitiveFieldNames_formattedField
     *
     */
    @Test 
    public void distributedSearch_groupingOnCustomFieldDefinedInSharedPropertiesVariant_shouldReturnCorrectResults()
        throws Exception
    {
        sql = "select `finance:Emp` from alfresco group by `finance:Emp`";
        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);
        assertEquals(2, tuples.size());
    }

    @Test 
    public void distributedSearch_customModelFieldNotInSharedProperties_shouldThrowException() throws Exception
    {
        exceptionRule.expect(Exception.class);
        exceptionRule.expectMessage("Column 'bob' not found in any table");

        sql = "select bob from alfresco";
        sqlQuery(sql, alfrescoJson);
    }

    @Test
    public void distributedSearch_customModelFieldMisconfiguredInSharedProperties_shouldThrowException() throws Exception
    {
        exceptionRule.expect(Exception.class);
        exceptionRule.expectMessage("Column 'finance:misconfigured' not found in any table");

        sql = "select `finance:misconfigured` from alfresco";

        sqlQuery(sql, alfrescoJson);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void distributedSearch_selectStarQuery_shouldReturnResultsWithDefaultFieldsOnly() throws Exception
    {
        sql = "select * from alfresco";
        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);
        assertEquals(indexedNodesCount, tuples.size());
        assertNotNull(tuples);

        Set<String> selectStarFields = getSelectStarFields();

        for(Tuple t:tuples){
            /* Apparently for the hard coded list of fields, there are two copies in the response tuples, except for date fields or integers*
             * I recommend to investigate this as I am not sure why you would like to return duplicate columns to the user : SEARCH-1363
             */
            Set<String> tupleFields = ((Set<String>) t.fields.keySet()).stream().map(
                    s -> s.replaceFirst(":", "_")).collect(Collectors.toSet());
            assertEquals(selectStarFields, tupleFields);
        }
    }

    @Test
    public void distributedSearch_selectStarQueryWithPredicates_shouldReturnResultsWithDefaultFieldsOnly() throws Exception
    {
        Set<String> selectStarFields = getSelectStarFields();
        // Query select * with property in predicate belonging to select * fields
        List<Tuple> tuples = sqlQuery("select * from alfresco where cm_name = 'name1'", alfrescoJson);
        assertNotNull(tuples);
        assertEquals(tuples.size(), 1);
        checkFormattedReturnedFields(tuples, selectStarFields);

        tuples = sqlQuery("select * from alfresco where `cm:name` = 'name1'", alfrescoJson);
        assertNotNull(tuples);
        assertEquals(tuples.size(), 1);
        checkFormattedReturnedFields(tuples, selectStarFields);
    }


    /**
     * Check the correctness of the fields returned from a select * query with field in predicate not belonging
     * to selectStarFields
     */
    @Test
    public void distributedSearch_selectStarQueryWithPredicates_notDefault_shouldReturnResultsWithDefaultFieldsOnly() throws Exception
    {
        Set<String> selectStarFields = getSelectStarFields();
        selectStarFields.add("cm_author");

        // Query select * with property in predicate not belonging to select * fields
        List<Tuple> tuples = sqlQuery("select * from alfresco where cm_author != '*'", alfrescoJson);
        assertNotNull(tuples);
        assertEquals(indexedNodesCount, tuples.size());
        checkFormattedReturnedFields(tuples, selectStarFields);

        tuples = sqlQuery("select * from alfresco where `cm:author` != '*'", alfrescoJson);
        assertNotNull(tuples);
        assertEquals(indexedNodesCount, tuples.size());
        checkFormattedReturnedFields(tuples, selectStarFields);
    }

    /**
     * This test check that queries with field missing in solr index does not fail if they belongs to
     * default fields (see SEARCH-1446)
     */
    @Test
    public void distributedSearch_queryFieldsMissingInSolrIndex() throws Exception
    {
        // Query select * with property in predicate not belonging to select * fields
        List<Tuple> tuples = sqlQuery("select cm_lockOwner, count(*) as total from alfresco group by cm_lockOwner", alfrescoJson);
        assertNotNull(tuples);
        assertEquals("no results should be found", tuples.size(), 0);
    }

    @Test
    public void distributedSearch_query_shouldReturnOnlySelectedFields() throws Exception
    {
        // Query select * with property in predicate belonging to select * fields
        List<Tuple> tuples = sqlQuery("select cm_name from alfresco where cm_name = 'name1'", alfrescoJson);
        assertNotNull(tuples);
        assertEquals(tuples.size(), 1);

        Set<String> fieldsToBeReturned = new HashSet<>();
        fieldsToBeReturned.add("cm_name");

        checkFormattedReturnedFields(tuples, fieldsToBeReturned);

        tuples = sqlQuery("select `cm:name`, cm_author from alfresco where cm_author != '*'", alfrescoJson);
        assertNotNull(tuples);
        assertEquals(indexedNodesCount, tuples.size());

        // add cm_author to the list for fields to be returned.
        fieldsToBeReturned.add("cm_author");
        checkFormattedReturnedFields(tuples, fieldsToBeReturned);
    }

    private void assertResult(List<Tuple> tuples)
    {
        assertEquals(tuples.size(), 2);
        Tuple first = tuples.get(0);

        String owner1 = first.getString("cm_owner");
        String title1 = first.getString("cm_title");

        assertEquals("michael", owner1);
        assertEquals("title1", title1);

        Tuple second = tuples.get(1);
        String owner2 = second.getString("cm_owner");
        String title2 = second.getString("cm_title");

        assertEquals("michael", owner2);
        assertEquals("title2", title2);
    }

    /**
     * Asserts the correctness of a scenario where the query uses a custom date field defined in shared.properties,
     * in the form "namespace_localName".
     */
    @Test
    public void distributedSearch_dateCustomModelFieldInSharedProperties_shouldReturnCorrectResults() throws Exception
    {
        Set<String> expectedColumns = new HashSet<>(asList("Expense Name","expense_Recorded_At"));
        sql = "select cm_name as `Expense Name`, expense_Recorded_At from alfresco";

        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);
        assertEquals(indexedNodesCount, tuples.size());

        tuples.forEach(tuple ->  assertEquals("Mismatched columns", expectedColumns, tuple.fields.keySet()));
    }

    /**
     * Asserts the correctness of a scenario where the query uses a custom date field defined in shared.properties,
     * in the form "namespace:localName".
     */
    @Test
    public void distributedSearch_dateCustomModelFieldInSharedPropertiesQueryVariant_shouldReturnCorrectResults()
        throws Exception
    {
        Set<String> expectedColumns = new HashSet<>(asList("Expense Name","expense:Recorded_At"));
        sql = "select cm_name as `Expense Name`, `expense:Recorded_At` from alfresco";

        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);
        assertEquals(indexedNodesCount, tuples.size());

        tuples.forEach(tuple ->  assertEquals("Mismatched columns", expectedColumns, tuple.fields.keySet()));
    }

    /**
     * Internal method for getting the count of expected results from a given query.
     * This is used in tests methods for avoiding to hard-code the expected number of matches.
     *
     * @param whereCondition the predicate part of the query that will be used for counting
     * @return the count of expected results according with the input predicate.
     */
    private long expectedCount(String whereCondition)
    {
        String countQuery = "select count(*) from alfresco where " + whereCondition;
        long count =
                of(sqlQuery(countQuery, alfrescoJson).iterator().next())
                    .map(tuple -> tuple.getLong("EXPR$0"))
                    .orElseThrow(() -> new RuntimeException("Unable to get the count of expected results."));

        assertTrue("We expect at least 1 result, otherwise the test doesn't make sense", count > 0);

        return count;
    }
}

