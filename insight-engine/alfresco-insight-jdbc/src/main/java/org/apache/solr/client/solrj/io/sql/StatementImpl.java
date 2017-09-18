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

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import org.alfresco.solr.stream.AlfrescoSolrStream;
import org.apache.solr.client.solrj.io.stream.SolrStream;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;

class StatementImpl implements Statement {

  private final ConnectionImpl connection;
  private boolean closed;
  private String currentSQL;
  private ResultSetImpl currentResultSet;
  private SQLWarning currentWarning;
  private int maxRows;

  StatementImpl(ConnectionImpl connection) {
    this.connection = connection;
  }

  private void checkClosed() throws SQLException {
    if(isClosed()) {
      throw new SQLException("Statement is closed.");
    }
  }

  private ResultSet executeQueryImpl(String sql) throws SQLException {
    try {
      if(this.currentResultSet != null) {
        this.currentResultSet.close();
        this.currentResultSet = null;
      }

      if(maxRows > 0 && !containsLimit(sql)) {
        sql = sql + " limit "+Integer.toString(maxRows);
      }

      closed = false;  // If closed reopen so Statement can be reused.
      this.currentResultSet = new ResultSetImpl(this, constructStream(sql));
      return this.currentResultSet;
    } catch(Exception e) {
      throw new SQLException(e);
    }
  }

  // ******************************************************
  // ******************************************************
  // This is the only place which we had to do some changes
  // ******************************************************
  // ******************************************************
  protected SolrStream constructStream(String sql) throws IOException {
      // *************
      // *************
      // Original code
      // *************
      // *************
      /*
      try {
          ZkStateReader zkStateReader = this.connection.getClient().getZkStateReader();
          Collection<Slice> slices = CloudSolrStream.getSlices(this.connection.getCollection(), zkStateReader, true);

          List<Replica> shuffler = new ArrayList<>();
          for(Slice slice : slices) {
            Collection<Replica> replicas = slice.getReplicas();
            for (Replica replica : replicas) {
              shuffler.add(replica);
            }
          }

          Collections.shuffle(shuffler, new Random());

          ModifiableSolrParams params = new ModifiableSolrParams();
          params.set(CommonParams.QT, "/sql");
          params.set("stmt", sql);
          for(String propertyName : this.connection.getProperties().stringPropertyNames()) {
            params.set(propertyName, this.connection.getProperties().getProperty(propertyName));
          }

          Replica rep = shuffler.get(0);
          ZkCoreNodeProps zkProps = new ZkCoreNodeProps(rep);
          String url = zkProps.getCoreUrl();
          return new SolrStream(url, params);
        } catch (Exception e) {
          throw new IOException(e);
        }
      */

      try {

        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(CommonParams.QT, "/sql");
        params.set("stmt", sql);
        for(String propertyName : this.connection.getProperties().stringPropertyNames()) {
          params.set(propertyName, this.connection.getProperties().getProperty(propertyName));
        }

        URI uri = new URI(this.connection.getUrl().replaceFirst("jdbc:", ""));
        StringBuilder url = new StringBuilder();
        url
            // TODO: Hard coded as "http://" for the time being. This will be addressed with SEARCH-552
            .append("http://")
            .append(uri.getHost())
            .append(":")
            .append(uri.getPort())
            .append("/solr/")
            .append(this.connection.getCollection());

        AlfrescoSolrStream alfrescoSolrStream = new AlfrescoSolrStream(url.toString(), params);
        alfrescoSolrStream.setJson(params.get("json"));

        return alfrescoSolrStream;
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    return this.executeQueryImpl(sql);
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    return 0;
  }

  @Override
  public void close() throws SQLException {
    if(closed) {
      return;
    }

    this.closed = true;

    if(this.currentResultSet != null) {
      this.currentResultSet.close();
    }
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getMaxRows() throws SQLException {
    return this.maxRows;
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    this.maxRows = max;
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void cancel() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    checkClosed();

    return this.currentWarning;
  }

  @Override
  public void clearWarnings() throws SQLException {
    checkClosed();

    this.currentWarning = null;
  }

  @Override
  public void setCursorName(String name) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean execute(String sql) throws SQLException {

    if(this.currentResultSet != null) {
      this.currentResultSet.close();
      this.currentResultSet = null;
    }

    // TODO Add logic when update statements are added to JDBC.
    this.currentSQL = sql;
    return true;
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return this.executeQueryImpl(this.currentSQL);
  }

  @Override
  public int getUpdateCount() throws SQLException {
    checkClosed();

    // TODO Add logic when update statements are added to JDBC.
    return -1;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    checkClosed();

    // Currently multiple result sets are not possible yet
    this.currentResultSet.close();
    return false;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    checkClosed();

    if(direction != ResultSet.FETCH_FORWARD) {
      throw new SQLException("Direction must be ResultSet.FETCH_FORWARD currently");
    }
  }

  @Override
  public int getFetchDirection() throws SQLException {
    checkClosed();

    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    checkClosed();

    if(rows < 0) {
      throw new SQLException("Rows must be >= 0");
    }
  }

  @Override
  public int getFetchSize() throws SQLException {
    checkClosed();

    return 0;
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    checkClosed();

    return ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public int getResultSetType() throws SQLException {
    checkClosed();

    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clearBatch() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] executeBatch() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Connection getConnection() throws SQLException {
    return this.connection;
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isClosed() throws SQLException {
    return closed;
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isPoolable() throws SQLException {
    return true;
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new UnsupportedOperationException();
  }

  private boolean containsLimit(String sql) {
    String[] tokens = sql.split("\\s+");
    String secondToLastToken = tokens[tokens.length-2];
    return ("limit").equalsIgnoreCase(secondToLastToken);
  }
}