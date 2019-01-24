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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.alfresco.solr.sql.SelectStarDefaultField;
import org.alfresco.solr.sql.SolrSchemaUtil;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.io.Tuple;
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
    private Properties getSQLFields()
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
    
    @Rule
    public JettyServerRule jetty = new JettyServerRule(1, this, getSQLFields());
    
    @Test
    public void testSearch() throws Exception
    {
        String alfrescoJson = "{ \"authorities\": [ \"jim\", \"joel\" ], \"tenants\": [ \"\" ] }";
        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);
        assertTrue(tuples.size() == 4);
        assertNodes(tuples, node1, node2, node3, node4);
        assertFieldNotNull(tuples, "LID");

        String alfrescoJson2 = "{ \"authorities\": [ \"joel\" ], \"tenants\": [ \"\" ] }";
        tuples = sqlQuery(sql, alfrescoJson2);
        assertTrue(tuples.size() == 2);
        assertNodes(tuples, node1, node2);
        assertFieldNotNull(tuples, "LID");
        
        //SEARCH-679
        sql = "SELECT DBID,cm_created FROM alfresco order by `cm:created`";
        tuples = sqlQuery(sql, alfrescoJson2);
        assertNotNull(tuples);
        assertTrue(tuples.size() == 2);
        sql = "SELECT DBID,cm_created FROM alfresco order by cm_created";
        tuples = sqlQuery(sql, alfrescoJson2);
        assertNotNull(tuples);
        assertTrue(tuples.size() == 2);
        try
        {
            sql = "SELECT DBID,cm_created FROM alfresco order by cm_fake";
            tuples = sqlQuery(sql, alfrescoJson2);
        }
        catch (IOException e) 
        {
            assertNotNull(e);
        }
        sql = "select DBID, LID from alfresco where `cm:content` = 'world' order by DBID limit 1";
        tuples = sqlQuery(sql, alfrescoJson2);
        assertTrue(tuples.size() == 1);
        assertFieldNotNull(tuples, "DBID");
        assertFieldNotNull(tuples, "LID");

        //This phrase search should find results
        sql = "select DBID, LID from alfresco where `cm:content` = 'hello world' order by DBID limit 1";
        tuples = sqlQuery(sql, alfrescoJson2);
        assertTrue(tuples.size() == 1);
        assertFieldNotNull(tuples, "DBID");
        assertFieldNotNull(tuples, "LID");

        //This query will be treated as a conjunction
        sql = "select DBID, LID from alfresco where `cm:content` = '(world hello)' order by DBID limit 1";
        tuples = sqlQuery(sql, alfrescoJson2);
        assertTrue(tuples.size() == 1);
        assertFieldNotNull(tuples, "DBID");
        assertFieldNotNull(tuples, "LID");

        //This phrase search should not find results
        sql = "select DBID, LID from alfresco where `cm:content` = 'world hello' order by DBID limit 1";
        tuples = sqlQuery(sql, alfrescoJson2);
        assertTrue(tuples.size() == 0);

        sql = "select DBID, LID from alfresco where `cm:content` = 'world' order by DBID";
        tuples = sqlQuery(sql, alfrescoJson2);
        assertTrue(tuples.size() == 2);
        assertFieldNotNull(tuples, "DBID");
        assertFieldNotNull(tuples, "LID");

        sql = "select DBID, LID from alfresco where `cm:content` = 'world'";
        tuples = sqlQuery(sql, alfrescoJson2);
        assertTrue(tuples.size() == 2);
        assertFieldNotNull(tuples, "DBID");
        assertFieldNotNull(tuples, "LID");

        sql = "select DBID, LID from alfresco";
        tuples = sqlQuery(sql, alfrescoJson2);
        assertTrue(tuples.size() == 2);
        assertFieldNotNull(tuples, "DBID");
        assertFieldNotNull(tuples, "LID");

        sql = "select TYPE, SITE from alfresco";
        tuples = sqlQuery(sql, alfrescoJson2);
        assertTrue(tuples.size() == 2);
        assertFieldNotNull(tuples, "TYPE");
        assertFieldNotNull(tuples, "SITE");

        sql = "select DBID from alfresco where cm_creator = 'creator1'";
        tuples = sqlQuery(sql, alfrescoJson);
        assertTrue(tuples.size() == 2);

        sql = "select DBID from alfresco where `cm:creator` = 'creator1'";
        List<Tuple> tuplesAgain = sqlQuery(sql, alfrescoJson);
        assertTrue(tuplesAgain.size() == 2);

        sql = "select DBID from alfresco where cm_created = '[2000 TO 2001]'";
        tuples = sqlQuery(sql, alfrescoJson);
        assertTrue(tuples.size() == 4);

        sql = "select DBID from alfresco where cm_fiveStarRatingSchemeTotal = '[4 TO 21]'";
        tuples = sqlQuery(sql, alfrescoJson);
        assertTrue(tuples.size() == 4);

        sql = "select DBID from alfresco where audio_trackNumber = '[5 TO 10>'";
        tuples = sqlQuery(sql, alfrescoJson);
        assertTrue(tuples.size() == 2);

        sql = "select DBID from alfresco where `audio:trackNumber` = '[4 TO 12]'";
        tuples = sqlQuery(sql, alfrescoJson);
        assertTrue(tuples.size() == 4);

        sql = "select DBID from alfresco where audio_trackNumber = '[5 TO 10]' AND cm_fiveStarRatingSchemeTotal = '[10 TO 12]'";
        tuples = sqlQuery(sql, alfrescoJson);
        assertTrue(tuples.size() == 1);

        //It will return null but, in the debugger, the syntax looks right.
        sql = "select DBID from alfresco where TYPE = 'content' AND NOT TYPE = 'fm:post'";
        tuples = sqlQuery(sql, alfrescoJson);
        assertTrue(tuples.size() == 0);

        sql = "select DBID from alfresco where `cm_content.mimetype` = 'text/plain'";
        tuples = sqlQuery(sql, alfrescoJson);
        assertTrue(tuples.size() == 3);

        sql = "select DBID from alfresco where `cm:content.mimetype` = 'text/javascript'";
        tuples = sqlQuery(sql, alfrescoJson);
        assertTrue(tuples.size() == 1);

        //Test negation
        sql = "select DBID from alfresco where `cm:content.mimetype` != 'text/javascript'";
        tuples = sqlQuery(sql, alfrescoJson);
        assertTrue(tuples.size() == 3);

        sql = "select DBID from alfresco where `cm:content.size` > 0";
        tuples = sqlQuery(sql, alfrescoJson);
        assertTrue(tuples.size() == 1);

        sql = "select DBID from alfresco where `cm:content.size` = '[1 TO *]'";
        tuples = sqlQuery(sql, alfrescoJson);
        assertTrue(tuples.size() == 1);

        sql = "select cm_creator, cm_name, `exif:manufacturer`, audio_trackNumber from alfresco order by `audio:trackNumber` asc";
        tuples = sqlQuery(sql, alfrescoJson2);
        assertTrue(tuples.size() == 2);

        for(Tuple tuple : tuples) {
            assertTrue(tuple.get("audio_trackNumber") instanceof Long);
            assertTrue(tuple.get("cm_creator") instanceof String);
            assertTrue(tuple.get("exif:manufacturer") instanceof String);
            assertTrue(tuple.get("cm_name") instanceof String);
        }

        Tuple t = tuples.get(0);
        assertTrue(t.getLong("audio_trackNumber") == 8);
        assertTrue(t.getString("exif:manufacturer").equals("Nikon"));
        assertTrue(t.getString("cm_creator").equals("creator1"));
        assertTrue(t.getString("cm_name").equals("name2"));

        t = tuples.get(1);
        assertTrue(t.getLong("audio_trackNumber") == 12);
        assertTrue(t.getString("exif:manufacturer").equals("Nikon"));
        assertTrue(t.getString("cm_creator").equals("creator1"));
        assertTrue(t.getString("cm_name").equals("name1"));


        sql = "select cm_creator, cm_name, `exif:manufacturer`, audio_trackNumber as atrack from alfresco order by `audio:trackNumber` desc";
        tuples = sqlQuery(sql, alfrescoJson2);
        assertTrue(tuples.size() == 2);

        for(Tuple tuple : tuples) {
            assertTrue(tuple.get("atrack") instanceof Long);
            assertTrue(tuple.get("cm_creator") instanceof String);
            assertTrue(tuple.get("exif:manufacturer") instanceof String);
            assertTrue(tuple.get("cm_name") instanceof String);
        }

        t = tuples.get(0);
        assertTrue(t.getLong("atrack") == 12);
        assertTrue(t.getString("exif:manufacturer").equals("Nikon"));
        assertTrue(t.getString("cm_creator").equals("creator1"));
        assertTrue(t.getString("cm_name").equals("name1"));

        t = tuples.get(1);
        assertTrue(t.getLong("atrack") == 8);
        assertTrue(t.getString("exif:manufacturer").equals("Nikon"));
        assertTrue(t.getString("cm_creator").equals("creator1"));
        assertTrue(t.getString("cm_name").equals("name2"));


        sql = "select `cm:name`, `cm:fiveStarRatingSchemeTotal` from alfresco";
        tuples = sqlQuery(sql, alfrescoJson2);
        assertTrue(tuples.size() == 2);
        for(Tuple tuple : tuples) {
            assertTrue(tuple.get("cm:fiveStarRatingSchemeTotal") instanceof Double);
            assertTrue(tuple.get("cm:name") instanceof String);
        }
        //Test sql and solr predicate
        sql = "select cm_creator from alfresco where _query_ = 'cm_creator:creator1'";
        tuples = sqlQuery(sql, alfrescoJson2);
        assertNotNull(tuples);
        assertTrue(tuples.size() == 2);
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
        Set<String> expectedColumns = new HashSet<>(Arrays.asList("Expense Name","finance_amount"));
        sql = "select cm_name as `Expense Name`, finance_amount from alfresco";
        
        JettySolrRunner localJetty = jettyContainers.values().iterator().next();
        /* This is a workaround, the solrhome is not currently properly managed in tests : SEARCH-1309*/
        System.setProperty("solr.solr.home", localJetty.getSolrHome());
        
        
        String alfrescoJson = "{ \"authorities\": [ \"jim\", \"joel\" ], \"tenants\": [ \"\" ] }";
        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);
        assertTrue(tuples.size() == 4);
        for (Tuple t : tuples)
        {
            assertEquals("Mismatched columns", expectedColumns, t.fields.keySet());
        }

        System.clearProperty("solr.solr.home");
    }

    @Test 
    public void distributedSearch_customModelFieldInSharedPropertiesQueryVariant_shouldReturnCorrectResults()
        throws Exception
    {
        Set<String> expectedColumns = new HashSet<>(Arrays.asList("Expense Name","finance:amount"));
        sql = "select cm_name as `Expense Name`, `finance:amount` from alfresco";
        
        JettySolrRunner localJetty = jettyContainers.values().iterator().next();
        /* This is a workaround, the solrhome is not currently properly managed in tests : SEARCH-1309*/
        System.setProperty("solr.solr.home", localJetty.getSolrHome());
        
        String alfrescoJson = "{ \"authorities\": [ \"jim\", \"joel\" ], \"tenants\": [ \"\" ] }";
        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);
        assertTrue(tuples.size() == 4);
        for (Tuple t : tuples)
        {
            assertEquals("Mismatched columns", expectedColumns, t.fields.keySet());
        }

        System.clearProperty("solr.solr.home");
    }

    @Test 
    public void distributedSearch_groupingOnCustomFieldDefinedInSharedProperties_shouldReturnCorrectResults()
        throws Exception
    {
        JettySolrRunner localJetty = jettyContainers.values().iterator().next();
        /* This is a workaround, the solrhome is not currently properly managed in tests : SEARCH-1309*/
        System.setProperty("solr.solr.home", localJetty.getSolrHome());

        sql = "select finance_Emp from alfresco group by finance_Emp";
        String alfrescoJson = "{ \"authorities\": [ \"jim\", \"joel\" ], \"tenants\": [ \"\" ] }";
        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);
        assertTrue(tuples.size() == 2);
        System.clearProperty("solr.solr.home");
    }

    @Test 
    public void distributedSearch_groupingOnCustomFieldDefinedInSharedPropertiesVariant_shouldReturnCorrectResults()
        throws Exception
    {
        JettySolrRunner localJetty = jettyContainers.values().iterator().next();
        /* This is a workaround, the solrhome is not currently properly managed in tests : SEARCH-1309*/
        System.setProperty("solr.solr.home", localJetty.getSolrHome());

        sql = "select `finance:Emp` from alfresco group by `finance:Emp`";
        String alfrescoJson = "{ \"authorities\": [ \"jim\", \"joel\" ], \"tenants\": [ \"\" ] }";
        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);
        assertTrue(tuples.size() == 2);
        System.clearProperty("solr.solr.home");
    }

    @Test 
    public void distributedSearch_customModelFieldNotInSharedProperties_shouldThrowException() throws Exception
    {
        exceptionRule.expect(Exception.class);
        exceptionRule.expectMessage("Column 'bob' not found in any table");

        JettySolrRunner localJetty = jettyContainers.values().iterator().next();
        /* This is a workaround, the solrhome is not currently properly managed in tests : SEARCH-1309*/
        System.setProperty("solr.solr.home", localJetty.getSolrHome());

        sql = "select bob from alfresco";
        String alfrescoJson = "{ \"authorities\": [ \"jim\", \"joel\" ], \"tenants\": [ \"\" ] }";
        sqlQuery(sql, alfrescoJson);
        System.clearProperty("solr.solr.home");
    }

    @Test
    public void distributedSearch_customModelFieldMisconfiguredInSharedProperties_shouldThrowException() throws Exception
    {
        exceptionRule.expect(Exception.class);
        exceptionRule.expectMessage("Column 'finance:misconfigured' not found in any table");

        JettySolrRunner localJetty = jettyContainers.values().iterator().next();
        /* This is a workaround, the solrhome is not currently properly managed in tests : SEARCH-1309*/
        System.setProperty("solr.solr.home", localJetty.getSolrHome());

        sql = "select `finance:misconfigured` from alfresco";
        String alfrescoJson = "{ \"authorities\": [ \"jim\", \"joel\" ], \"tenants\": [ \"\" ] }";
        sqlQuery(sql, alfrescoJson);
        System.clearProperty("solr.solr.home");
    }

    @Test
    public void distributedSearch_selectStarQuery_shouldReturnResultsWithDefaultFieldsOnly() throws Exception
    {
        JettySolrRunner localJetty = jettyContainers.values().iterator().next();
        /* This is a workaround, the solrhome is not currently properly managed in tests : SEARCH-1309*/
        System.setProperty("solr.solr.home", localJetty.getSolrHome());

        sql = "select * from alfresco";
        String alfrescoJson = "{ \"authorities\": [ \"jim\", \"joel\" ], \"tenants\": [ \"\" ] }";
        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);
        assertTrue(tuples.size() == 4);
        assertNotNull(tuples);

        /* Set containing the hard coded select * fields and the fields taken from shared.properties.
         */
        Set<String> selectStarFields =  Stream.concat(
                SolrSchemaUtil.fetchCustomFieldsFromSharedProperties().keySet().stream(),
                Arrays.asList(SelectStarDefaultField.values()).stream().map(s -> s.getFieldName()))
                        .map(s -> s.replaceFirst(":","_")).collect(Collectors.toSet());

        for(Tuple t:tuples){
            /* Apparently for the hard coded list of fields, there are two copies in the response tuples, except for date fields or integers*
             * I recommend to investigate this as I am not sure why you would like to return duplicate columns to the user : SEARCH-1363
             */
            Set<String> tupleFields = ((Set<String>) t.fields.keySet()).stream().map(
                    s -> s.replaceFirst(":", "_")).collect(Collectors.toSet());
            assertTrue(selectStarFields.equals(tupleFields));

        }
        System.clearProperty("solr.solr.home");
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
}

