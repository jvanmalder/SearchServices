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
import static org.apache.solr.client.solrj.io.sql.InsightEngineDriverUtil.buildJson;

import org.junit.Assert;
import org.junit.Test;

public class InsightEngineDriverUtilTest
{
    @Test
    public void isJDBCProtocolTest()
    {
        Assert.assertEquals(false,
                isJDBCProtocol("http://localhost:8080/alfresco/api/-default-/public/search/versions/1/sql"));
        Assert.assertEquals(false, isJDBCProtocol(""));
        Assert.assertEquals(false, isJDBCProtocol(null));
        Assert.assertEquals(true, isJDBCProtocol("jdbc:alfresco://localhost:8983?collection=alfresco"));
    }
    @Test
    public void buildJsonTest()
    {
        String sql = "select SITE, CM_OWNER from alfresco group by SITE,CM_OWNER";
        String expected = "{" + 
                "\"stmt\":\"select SITE, CM_OWNER from alfresco group by SITE,CM_OWNER\",\"format\":\"solr\",\"locales\":[\"en_UK\",\"en_US\"]}";
        Assert.assertEquals(expected, buildJson(sql, null));
        Assert.assertEquals("{}", buildJson(null, null));
    }
}
