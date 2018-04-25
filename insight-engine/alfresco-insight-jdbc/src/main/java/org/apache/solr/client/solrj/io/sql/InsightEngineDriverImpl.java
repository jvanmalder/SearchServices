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

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import static org.apache.solr.client.solrj.io.sql.InsightEngineDriverUtil.isJDBCProtocol;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.solr.common.StringUtils;

/**
 * A Jdbc driver class for the Insight Engine
 */
public class InsightEngineDriverImpl extends DriverImpl
{
    static {
        try {
            DriverManager.registerDriver(new InsightEngineDriverImpl());
        } catch (SQLException e) {
            throw new RuntimeException("Can't register insight driver!", e);
        }
    }

    @Override
    public boolean acceptsURL(String url)
    {
        return url != null && (isJDBCProtocol(url)|| url.startsWith("http"));
    }
    @Override
    public Connection connect(String url, Properties props) throws SQLException
    {
        if(!acceptsURL(url)) 
        {
            return null;
        }
        URI uri = processUrl(url);
        loadParams(uri, props);
        String collection = (String) props.remove("collection");
        if(collection == null)
        {
            collection = "alfresco";
        }
        if (!props.containsKey("aggregationMode"))
        {
            props.setProperty("aggregationMode", "facet");
        }
        
        // JDBC requires metadata like field names from the SQLHandler. Force this property to be true.
        props.setProperty("includeMetadata", "true");
        String zkHost = uri.getAuthority() + uri.getPath();
        return new ConnectionImpl(url, zkHost, collection, props);
    }
    
    private void loadParams(URI uri, Properties props) throws SQLException
    {
        List<NameValuePair> parsedParams = URLEncodedUtils.parse(uri, "UTF-8");
        for (NameValuePair pair : parsedParams)
        {
            if (pair.getValue() != null)
            {
                props.put(pair.getName(), pair.getValue());
            } 
            else
            {
                props.put(pair.getName(), "");
            }
        }
    }
    @Override
    protected URI processUrl(String url) throws SQLException {
        URI uri;
        try 
        {
            uri = new URI(url.replaceFirst("jdbc:", ""));
        } 
        catch (URISyntaxException e)
        {
          throw new SQLException(e);
        }

        if (uri.getAuthority() == null) 
        {
          throw new SQLException("The zkHost must not be null");
        }

        return uri;
      }
}
