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

import static org.apache.solr.client.solrj.io.sql.InsightEngineDriverUtil.isJDBCProtocol;
import static org.apache.solr.client.solrj.io.sql.InsightEngineDriverUtil.formatSQL;
import static org.apache.solr.client.solrj.io.sql.InsightEngineDriverUtil.buildJson;

import org.junit.Assert;
import org.junit.Test;

public class InsightEngineDriverUtilTest
{
    @Test
    public void isJDBCProtocolTest()
    {
        Assert.assertFalse(isJDBCProtocol("http://localhost:8080/alfresco/api/-default-/public/search/versions/1/sql"));
        Assert.assertFalse(isJDBCProtocol(""));
        Assert.assertFalse(isJDBCProtocol(null));
        Assert.assertTrue(isJDBCProtocol("jdbc:alfresco://localhost:8983?collection=alfresco"));
    }
    @Test
    public void buildJsonTest()
    {
        String sql = "select SITE, CM_OWNER from alfresco group by SITE,CM_OWNER";
        String expected = "{" + 
                "\"stmt\":\"select SITE, CM_OWNER from alfresco group by SITE,CM_OWNER\",\"format\":\"solr\",\"locales\":[\"en_UK\",\"en_US\"],\"includeMetadata\":\"true\"}";
        String[] locales = new String[2];
        locales[0]= "en_UK";
        locales[1]= "en_US";
        Assert.assertEquals(expected, buildJson(sql, locales));
        Assert.assertEquals("{}", buildJson(null, null));
    }
    
    @Test
    public void whiteSpaceCharsAreReplacedWithBlankSpaces()
    {
        Assert.assertEquals(
                "select SITE, CM_OWNER  from alfresco group by SITE,CM_OWNER",
                formatSQL("select SITE, CM_OWNER\n from alfresco group by SITE,CM_OWNER"));

        Assert.assertEquals(
                "select SITE, CM_OWNER  from alfresco group by  SITE,CM_OWNER",
                formatSQL("select SITE, CM_OWNER\n from alfresco group by\n SITE,CM_OWNER"));

        Assert.assertEquals(
                "select SITE, CM_OWNER  from alfresco  group by  SITE,CM_OWNER",
                formatSQL("select SITE, CM_OWNER\n from alfresco\r group by\n SITE,CM_OWNER"));

        Assert.assertEquals(
                "select SITE, CM_OWNER   from alfresco  group by  SITE,CM_OWNER",
                formatSQL("select SITE, CM_OWNER\n\n from alfresco\r group by\n SITE,CM_OWNER"));

        Assert.assertEquals(
                "select  *  from alfresco  order by    SITE , CM_OWNER ",
                formatSQL("select  *  from alfresco\r order by \n  SITE , CM_OWNER "));

        Assert.assertEquals(
                "select  *  from alfresco  order by    SITE , CM_OWNER ",
                formatSQL("select  *  from alfresco\r order by \n  SITE , CM_OWNER "));

        Assert.assertEquals(
                " select      *                 from alfresco       order by SITE    , CM_OWNER    ",
                formatSQL(" select   \r\n *       \n\r        from alfresco\r      order by SITE   \n, CM_OWNER \n\r "));
    }
}