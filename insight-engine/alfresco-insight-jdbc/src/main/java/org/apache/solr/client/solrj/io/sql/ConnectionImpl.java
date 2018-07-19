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

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.client.solrj.io.sql;

import java.sql.*;
import java.util.Map;
import java.util.Set;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.io.SolrClientCache;

class ConnectionImpl implements Connection {

  private final String url;
  private final SolrClientCache solrClientCache = new SolrClientCache();
  private final CloudSolrClient client;
  private final Properties properties;
  private final DatabaseMetaData databaseMetaData;
  private final Statement connectionStatement;
  private String collection;
  private boolean closed;
  private SQLWarning currentWarning;

  // ******************************************************
  // ******************************************************
  // This is the only place which we had to do some changes
  // ******************************************************
  // ******************************************************
  // 09.10.2017
  // The method getCatalog in this class had to be
  // changed as well.
  // ******************************************************
  // ******************************************************
  ConnectionImpl(String url, String zkHost, String collection, Properties properties) throws SQLException {
    this.url = url;
    // *************
    // *************
    // Original code
    // *************
    // *************
    // this.client = this.solrClientCache.getCloudSolrClient(zkHost);
    this.client = null;
    this.collection = collection;
    this.properties = properties;
    this.connectionStatement = createStatement();
    this.databaseMetaData = new DatabaseMetaDataImpl(this, this.connectionStatement);

    /*
    * Add any properties that start with javax. to the system props. This will set the SSL properties where the HTTP
    * client can find them if they are passed in by the code using the JDBC driver.
    */
    
    Set keys = properties.keySet();
    for(Object k : keys) {
        String key = (String)k;
        if(key.startsWith("javax.")) {
            System.setProperty(key, properties.getProperty(key));
        } else if(key.equals("alfresco.ssl.checkPeerName")) {
            //Add the property for disabling peer name check
            System.setProperty("solr.ssl.checkPeerName", properties.getProperty(key));
        }
     }
  }

  String getUrl() {
    return url;
  }

  CloudSolrClient getClient() {
    return client;
  }

  String getCollection() {
    return collection;
  }

  Properties getProperties() {
    return properties;
  }

  SolrClientCache getSolrClientCache() {
    return this.solrClientCache;
  }

  @Override
  public Statement createStatement() throws SQLException {
    return new StatementImpl(this);
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    return new PreparedStatementImpl(this, sql);
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {

  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return false;
  }

  @Override
  public void commit() throws SQLException {

  }

  @Override
  public void rollback() throws SQLException {

  }

  @Override
  public void close() throws SQLException {
    if(closed) {
      return;
    }

    this.closed = true;

    try {
      if(this.connectionStatement != null) {
        this.connectionStatement.close();
      }
    } finally {
      if (this.solrClientCache != null) {
        this.solrClientCache.close();
      }
    }
  }

  @Override
  public boolean isClosed() throws SQLException {
    return closed;
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    return this.databaseMetaData;
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {

  }

  /*
   * When using OpenLink ODBC-JDBC bridge on Windows, it runs the method ConnectionImpl.setReadOnly(String ...).
   * The spec says that setReadOnly(boolean ...) is required. This causes the ODBC-JDBC bridge to fail on Windows.
   * OpenLink case: http://support.openlinksw.com/support/techupdate.vsp?c=21881
   */
  public void setReadOnly(String readOnly) throws SQLException {

  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return true;
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {

  }

  // ********************************************
  // ********************************************
  // This change was needed for DbVisualizer.
  // Apache Zeppelin did not require this change.
  // ********************************************
  // ********************************************
  @Override
  public String getCatalog() throws SQLException {
    // *************
    // *************
    // Original code
    // *************
    // *************
    // return this.client.getZkHost();
    return "";
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    if(isClosed()) {
      throw new SQLException("Connection is closed.");
    }

    return this.currentWarning;
  }

  @Override
  public void clearWarnings() throws SQLException {
    if(isClosed()) {
      throw new SQLException("Connection is closed.");
    }

    this.currentWarning = null;
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getHoldability() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Clob createClob() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Blob createBlob() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public NClob createNClob() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    // check that the connection isn't closed and able to connect within the timeout
    try {
      if(!isClosed()) {
        this.client.connect(timeout, TimeUnit.SECONDS);
        return true;
      }
    } catch (InterruptedException|TimeoutException ignore) {
      // Ignore error since connection is not valid
    }
    return false;
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setSchema(String schema) throws SQLException {

  }

  @Override
  public String getSchema() throws SQLException {
    return null;
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
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
}
