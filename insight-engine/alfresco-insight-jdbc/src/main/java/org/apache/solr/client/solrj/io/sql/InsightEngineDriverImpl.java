package org.apache.solr.client.solrj.io.sql;

import java.sql.DriverManager;
import java.sql.SQLException;

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
        return url != null && url.startsWith("jdbc:alfresco");
    }
}
