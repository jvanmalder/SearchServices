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
package org.apache.solr.client.solrj.io.sql;

import org.apache.solr.common.StringUtils;

/**
 * A Jdbc driver util class for the Insight Engine.
 */
public class InsightEngineDriverUtil extends DriverImpl
{
   public static boolean isJDBCProtocol(String url)
   {
       if(!StringUtils.isEmpty(url)) 
       {
           return url.startsWith("jdbc:");
       }
       return false;
   }
   /**
    * Build the json body in the format expected by Rest API as shown below.
    * {
    *    "stmt" : "Select x from alfresco",
    *    "format" : "solr",
    *    "locales": []
    * }
    * @param sql
    * @param locales
    * @return
    */
   public static String buildJson(String sql, String[] locales)
   {
       if(StringUtils.isEmpty(sql))
       {
           return "{}";
       }
       StringBuilder body = new StringBuilder();
       body.append("{");
       body.append("\"stmt\":" + "\"" + sql + "\",");
       body.append("\"format\":\"solr\",");
       body.append("\"locales\":[\"en_UK\",\"en_US\"]");
       body.append("}");
       return body.toString();
   }
}
