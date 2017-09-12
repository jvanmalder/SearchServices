package org.apache.solr.client.solrj.io.sql;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

/**
 * A Jdbc driver class for the Insight Engine
 */
public class InsightDriverImpl extends DriverImpl
{
    static {
        try {
            DriverManager.registerDriver(new InsightDriverImpl());
        } catch (SQLException e) {
            throw new RuntimeException("Can't register insight driver!", e);
        }
    }

    public Connection connect(String url, Properties props) throws SQLException {
        if(!acceptsURL(url)) {
          return null;
        }

        URI uri = processUrl(url);

        loadParams(uri, props);

        if (!props.containsKey("collection")) {
          throw new SQLException("The connection url has no connection properties. At a mininum the collection must be specified.");
        }

        url = rewriteUrl(uri, props);

        String collection = (String) props.remove("collection");

        if (!props.containsKey("aggregationMode")) {
          props.setProperty("aggregationMode", "facet");
        }

        // JDBC requires metadata like field names from the SQLHandler. Force this property to be true.
        props.setProperty("includeMetadata", "true");

        String zkHost = uri.getAuthority() + uri.getPath();

        return new ConnectionImpl(url, zkHost, collection, props);
      }

    private String rewriteUrl(URI uri, Properties props)
    {
        StringBuilder url = new StringBuilder();
        url
            .append("http://")
            .append(uri.getHost())
            .append(":")
            .append(uri.getPort())
            .append("/solr/")
            .append(props.getProperty("collection"));
        return url.toString();
    }

    @Override
    public boolean acceptsURL(String url)
    {
        return url != null && url.startsWith("jdbc:alfresco");
    }

    @Override
    protected URI processUrl(String url) throws SQLException {
        URI uri;
        try {
            uri = new URI(url.replaceFirst("jdbc:", ""));
        } catch (URISyntaxException e) {
            throw new SQLException(e);
        }

        /*
        if (uri.getAuthority() == null) {
          throw new SQLException("The zkHost must not be null");
        }
        */

        return uri;
    }

    private void loadParams(URI uri, Properties props) throws SQLException {
        List<NameValuePair> parsedParams = URLEncodedUtils.parse(uri, "UTF-8");
        for (NameValuePair pair : parsedParams) {
          if (pair.getValue() != null) {
            props.put(pair.getName(), pair.getValue());
          } else {
            props.put(pair.getName(), "");
          }
        }
      }
}
