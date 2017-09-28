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

import java.util.List;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.io.Tuple;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Joel
 */
@SolrTestCaseJ4.SuppressSSL
@LuceneTestCase.SuppressCodecs({"Appending","Lucene3x","Lucene40","Lucene41","Lucene42","Lucene43", "Lucene44", "Lucene45","Lucene46","Lucene47","Lucene48","Lucene49"})
public class DistributedGroupBySqlTest extends AbstractStreamTest
{
    @Rule
    public JettyServerRule jetty = new JettyServerRule(1, this);
    
    @Test
    public void testSearch() throws Exception
    {
        String sql = "select ACLID, count(*) from alfresco where `cm:content` = 'world' group by ACLID";
        String alfrescoJson = "{ \"authorities\": [ \"jim\", \"joel\" ], \"tenants\": [ \"\" ] }";
        List<Tuple> tuples = sqlQuery(sql, alfrescoJson);

        //We are grouping by ACLID so there will be two with a count(*) of 2.
        //The first 2 nodes on indexed with the same acl and the second 2 nodes are indexed with their same acl.
        assertTrue(tuples.size() == 2);
        assertTrue(tuples.get(0).getLong("EXPR$1") == 2);
        assertTrue(tuples.get(1).getLong("EXPR$1") == 2);
        
        if(node1.getAclId() != tuples.get(0).getLong("ACLID")) {
            throw new Exception("Incorrect Acl ID, found "+tuples.get(0).getLong("ACLID")+" expected "+node1.getAclId());
        }
        if(node3.getAclId() != tuples.get(1).getLong("ACLID")) {
            throw new Exception("Incorrect Acl ID, found "+tuples.get(0).getLong("ACLID")+" expected "+node3.getAclId());
        }

        sql = "select ACLID, count(*) AS barb from alfresco where `cm:content` = 'world' group by ACLID";
        tuples = sqlQuery(sql, alfrescoJson);

        //Now uses an alias
        assertTrue(tuples.size() == 2);
        assertTrue(tuples.get(0).getLong("barb") == 2);
        assertTrue(tuples.get(1).getLong("barb") == 2);

        sql = "select SITE, count(*) AS docsPerSite from alfresco where `cm:content` = 'world' group by SITE having count(*) > 1";
        tuples = sqlQuery(sql, alfrescoJson);
        assertTrue(tuples.size() == 1);
        assertTrue("_REPOSITORY_".equals(tuples.get(0).getString(("SITE"))));
        assertTrue(tuples.get(0).getLong("docsPerSite") == 4);

        sql = "select `cm:fiveStarRatingSchemeTotal` as rating, avg(`audio:trackNumber`) as track from alfresco where `cm:content` = 'world' group by `cm:fiveStarRatingSchemeTotal`";
        tuples = sqlQuery(sql, alfrescoJson);
        assertTrue(tuples.size() == 3);
        assertTrue(tuples.get(0).getDouble("rating") == 10);
        assertTrue(tuples.get(0).getLong(("track")) == 9);
        assertTrue(tuples.get(1).getDouble("rating") == 15);
        assertTrue(tuples.get(1).getLong(("track")) == 8);
        assertTrue(tuples.get(2).getDouble("rating") == 20);
        assertTrue(tuples.get(2).getLong(("track")) == 4);

        sql = "select cm_fiveStarRatingSchemeTotal, avg(audio_trackNumber) as track from alfresco where cm_content = 'world' group by cm_fiveStarRatingSchemeTotal";
        tuples = sqlQuery(sql, alfrescoJson);
        assertTrue(tuples.size() == 3);
        assertTrue(tuples.get(0).getDouble("cm_fiveStarRatingSchemeTotal") == 10);
        assertTrue(tuples.get(0).getLong(("track")) == 9);
        assertTrue(tuples.get(1).getDouble("cm_fiveStarRatingSchemeTotal") == 15);
        assertTrue(tuples.get(1).getLong(("track")) == 8);
        assertTrue(tuples.get(2).getDouble("cm_fiveStarRatingSchemeTotal") == 20);
        assertTrue(tuples.get(2).getLong(("track")) == 4);

        sql = "select exif_manufacturer as manu, count(*) as tot, max(cm_fiveStarRatingSchemeTotal) AS cre from alfresco where cm_content = 'world' group by exif_manufacturer order by tot asc";
        tuples = sqlQuery(sql, alfrescoJson);
        assertTrue(tuples.size() == 2);
        assertTrue("canon".equals(tuples.get(0).getString(("manu"))));
        assertTrue(tuples.get(0).getLong("tot") == 1);
        assertTrue(tuples.get(0).getDouble("cre") == 10);
        assertTrue("nikon".equals(tuples.get(1).getString(("manu"))));
        assertTrue(tuples.get(1).getLong("tot") == 3);
        assertTrue(tuples.get(1).getDouble("cre") == 20);

        sql = "select exif_manufacturer as manu, sum(cm_fiveStarRatingSchemeTotal), min(cm_fiveStarRatingSchemeTotal) from alfresco where cm_content = 'world' group by exif_manufacturer order by manu asc";
        tuples = sqlQuery(sql, alfrescoJson);
        assertTrue(tuples.size() == 2);
        assertTrue("canon".equals(tuples.get(0).getString(("manu"))));
        assertTrue(tuples.get(0).getDouble("EXPR$1") == 10);
        assertTrue(tuples.get(0).getDouble("EXPR$2") == 10);
        assertTrue("nikon".equals(tuples.get(1).getString(("manu"))));
        assertTrue(tuples.get(1).getDouble("EXPR$1") == 45);
        assertTrue(tuples.get(1).getDouble("EXPR$2") == 10);

        sql = "select cm_creator, count(*) from alfresco group by cm_creator having count(*) = 2";
        tuples = sqlQuery(sql, alfrescoJson);
        assertTrue(tuples.size() == 1);
        assertTrue("creator1".equals(tuples.get(0).getString(("cm_creator"))));

        sql = "select `cm_content.mimetype`, count(*) from alfresco group by `cm_content.mimetype` having count(*) < 4 order by count(*) asc";
        tuples = sqlQuery(sql, alfrescoJson);
        assertTrue(tuples.size() == 2);
        assertTrue("text/javascript".equals(tuples.get(0).getString(("cm_content.mimetype"))));
        assertTrue(tuples.get(0).getDouble("EXPR$1") == 1);

        assertTrue("text/plain".equals(tuples.get(1).getString(("cm_content.mimetype"))));
        assertTrue(tuples.get(1).getDouble("EXPR$1") == 3);

        sql = "select `cm_content.mimetype`, count(*) as mcount from alfresco group by `cm_content.mimetype` having count(*) = 3";
        tuples = sqlQuery(sql, alfrescoJson);
        assertTrue(tuples.size() == 1);
        assertTrue("text/plain".equals(tuples.get(0).getString(("cm_content.mimetype"))));
        assertTrue(tuples.get(0).getDouble("mcount") == 3);

        sql = "select cm_name, count(*) from alfresco where `cm:name` = 'name*' group by cm_name";
        tuples = sqlQuery(sql, alfrescoJson);
        assertTrue(tuples.size() == 3);
        tuples.forEach(tuple -> assertTrue(tuple.getString("cm_name").startsWith("name")));

        sql = "select cm_name, count(*) from alfresco where cm_name = '*3' group by cm_name";
        tuples = sqlQuery(sql, alfrescoJson);
        assertTrue(tuples.size() == 1);
        assertTrue("name3".equals(tuples.get(0).getString(("cm_name"))));
    }

}

