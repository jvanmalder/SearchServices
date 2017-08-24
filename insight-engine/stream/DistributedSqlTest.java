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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.io.Tuple;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author Joel
 */
@SolrTestCaseJ4.SuppressSSL
@LuceneTestCase.SuppressCodecs({"Appending","Lucene3x","Lucene40","Lucene41","Lucene42","Lucene43", "Lucene44", "Lucene45","Lucene46","Lucene47","Lucene48","Lucene49"})
public class DistributedSqlTest extends AbstractStreamTest
{
    private String sql = "select DBID, LID from alfresco where cm_content = 'world' order by DBID limit 10 ";
    
    @Rule
    public JettyServerRule jetty = new JettyServerRule(2, this);
    
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

        sql = "select DBID, LID from alfresco where `cm:content` = 'world' order by DBID limit 1";
        tuples = sqlQuery(sql, alfrescoJson2);
        assertTrue(tuples.size() == 1);
        assertFieldNotNull(tuples, "DBID");
        assertFieldNotNull(tuples, "LID");

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

        try {
            sql = "select DBID from alfresco where `cm:content.size` > 0";
            tuples = sqlQuery(sql, alfrescoJson);
            assertFalse("Should never get here",true);
        } catch (IOException sqe) {
            //The sql above produces the following invalid query q=(cm:content.size:+{+0+TO+*+])
            assertTrue(sqe.getMessage().contains("no viable alternative at input"));
        }

        sql = "select DBID from alfresco where `cm:content.size` = '[1 TO *]'";
        tuples = sqlQuery(sql, alfrescoJson);
        assertTrue(tuples.size() == 1);

        sql = "select cm_creator, cm_name, `exif:manufacturer`, audio_trackNumber from alfresco order by `audio:trackNumber`";
        tuples = sqlQuery(sql, alfrescoJson2);
        assertTrue(tuples.size() == 2);
        assertFieldNotNull(tuples, "cm_creator");
        assertFieldNotNull(tuples, "cm_name");
        assertFieldNotNull(tuples, "exif:manufacturer");
        assertFieldNotNull(tuples, "audio_trackNumber");
        assertFieldIsLong (tuples, "audio_trackNumber");

        sql = "select `cm:name`, `cm:fiveStarRatingSchemeTotal` from alfresco";
        tuples = sqlQuery(sql, alfrescoJson2);
        assertTrue(tuples.size() == 2);
        assertFieldNotNull(tuples, "cm:name");
        assertFieldIsDouble(tuples, "cm:fiveStarRatingSchemeTotal");

    }

}

