/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2019 SciDB, Inc.
* All Rights Reserved.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/
package org.scidb.jdbc;

import java.io.IOException;
import java.sql.*;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;

import org.scidb.client.AuthenticationFile;
import org.scidb.client.ConfigUser;
import org.scidb.client.ConfigUserException;
import org.scidb.client.SciDBException;

public class Connection implements java.sql.Connection
{
    private org.scidb.client.Connection scidbConnection;

    private final void implConnection(String host, int port, String username, String password)
            throws SQLException
    {
        Properties sessionProps = new Properties();
        sessionProps.setProperty(org.scidb.client.Connection.USERNAME_PROP, username);
        sessionProps.setProperty(org.scidb.client.Connection.PASSWORD_PROP, password);
        implConnection(host, port, sessionProps);
    }

    private final void implConnection(String host, int port, Properties props)
            throws SQLException
    {
        try {
            scidbConnection = new org.scidb.client.Connection();
            scidbConnection.connect(host, port);
            scidbConnection.startNewClient(props);
        } catch (SciDBException e) {
            throw new java.sql.SQLInvalidAuthorizationSpecException(e.getMessage());
        } catch (IOException e) {
            throw new java.sql.SQLNonTransientConnectionException(e);
        }
    }

    public Connection(String host, int port, Properties props)
            throws SQLException
    {
        implConnection(host, port, props);
    }

    /**
     * username != "" --> Username/Password authentication.
     * username == "" --> no authentication.
     */
    public Connection(String host, int port, String username, String password) throws SQLException
    {
        implConnection(host, port, username, password);
    }

    public Connection(String host, int port) throws SQLException
    {
        this(host, port, "", "");
    }

    /**
     * Username/Password from a file.
     */
    public Connection(String host, int port, String authFileName) throws SQLException
    {
        String username = "";
        String password = "";
        try {
            ConfigUser configUser =  ConfigUser.getInstance();
            configUser.verifySafeFile(authFileName, false);
            AuthenticationFile authFile = new AuthenticationFile(authFileName);
            username = authFile.getUserName();
            password = authFile.getUserPassword();
            implConnection(host, port, username, password);
        } catch (org.scidb.client.ConfigUserException e) {
            throw new java.sql.SQLInvalidAuthorizationSpecException(e);
        } catch (IOException e) {
            throw new java.sql.SQLNonTransientConnectionException(e);
        }
    }

    public org.scidb.client.Connection getSciDBConnection()
    {
        return scidbConnection;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public java.sql.Statement createStatement() throws SQLException
    {
        return new Statement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException
    {
        throw new java.sql.SQLFeatureNotSupportedException("Prepared statements not supported yet");
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException
    {
        throw new java.sql.SQLFeatureNotSupportedException("Prepared statements not supported yet");
    }

    @Override
    public String nativeSQL(String sql) throws SQLException
    {
        throw new java.sql.SQLFeatureNotSupportedException("SQL not supported yet");
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException
    {
        // TODO Auto-generated method stub
        throw new java.sql.SQLFeatureNotSupportedException("autoCommit flag not supported yet");
    }

    @Override
    public boolean getAutoCommit() throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void commit() throws SQLException
    {
        try {
            scidbConnection.commit();
        } catch (IOException e) {
            throw new java.sql.SQLTransactionRollbackException(e.getMessage());
        } catch (SciDBException e) {
            throw new java.sql.SQLTransactionRollbackException(e.getMessage());
        }
    }

    @Override
    public void rollback() throws SQLException
    {
        try {
            scidbConnection.rollback();
        } catch (IOException e) {
            throw new java.sql.SQLTransactionRollbackException(e.getMessage());
        } catch (SciDBException e) {
            throw new java.sql.SQLTransactionRollbackException(e.getMessage());
        }
    }

    @Override
    public void close() throws SQLException
    {
        try {
            scidbConnection.close();
        } catch (IOException e) {
            throw new java.sql.SQLTransientConnectionException(e.getMessage());
        } finally {
          scidbConnection = null;
        }
    }

    @Override
    public boolean isClosed() throws SQLException
    {
        return scidbConnection == null;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException
    {
        throw new java.sql.SQLFeatureNotSupportedException("readOnly flag not supported yet");
    }

    @Override
    public boolean isReadOnly() throws SQLException
    {
        return false;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException
    {
        throw new java.sql.SQLFeatureNotSupportedException("Catalog not supported yet");
    }

    @Override
    public String getCatalog() throws SQLException
    {
        return null;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException
    {
        throw new java.sql.SQLFeatureNotSupportedException("Txn isolation control not supported yet");
    }

    @Override
    public int getTransactionIsolation() throws SQLException
    {
        return TRANSACTION_NONE;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException
    {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException
    {
    }

    @Override
    public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException
    {
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException
    {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException
    {
        return prepareCall(sql);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException
    {
        // TODO Auto-generated method stub
        throw new java.sql.SQLFeatureNotSupportedException("Setting type descriptions not supported yet");

    }

    @Override
    public void setHoldability(int holdability) throws SQLException
    {
        // TODO Auto-generated method stub
        throw new java.sql.SQLFeatureNotSupportedException("Holdability not supported yet");
    }

    @Override
    public int getHoldability() throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException
    {
        // TODO Auto-generated method stub
        throw new java.sql.SQLFeatureNotSupportedException("Rollback not supported yet");

    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException
    {
        // TODO Auto-generated method stub
        throw new java.sql.SQLFeatureNotSupportedException("Savepoints not supported yet");
    }

    @Override
    public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException
    {
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException
    {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException
    {
        return prepareCall(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException
    {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException
    {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException
    {
        return prepareStatement(sql);
    }

    @Override
    public Clob createClob() throws SQLException
    {
        throw new java.sql.SQLFeatureNotSupportedException("Blobs not supported");
    }

    @Override
    public Blob createBlob() throws SQLException
    {
        throw new java.sql.SQLFeatureNotSupportedException("Blobs not supported");
    }

    @Override
    public NClob createNClob() throws SQLException
    {
        throw new java.sql.SQLFeatureNotSupportedException("Blobs not supported");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException
    {
        throw new java.sql.SQLFeatureNotSupportedException("SQLXML not supported");
    }

    @Override
    public boolean isValid(int timeout) throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException
    {
        // TODO Auto-generated method stub
        throw new SQLClientInfoException(new HashMap(),
                                         new java.sql.SQLFeatureNotSupportedException(
                                                 "Setting client information not supported yet"));
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException
    {
        // TODO Auto-generated method stub
        throw new SQLClientInfoException(new HashMap(),
                                         new java.sql.SQLFeatureNotSupportedException(
                                                 "Setting client information not supported yet"));
    }

    @Override
    public String getClientInfo(String name) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * @since Java 7
     */
    @Override
    public int getNetworkTimeout() throws SQLException
    {
        return 0;
    }

    /**
     * @since Java 7
     */
    @Override
    public void setNetworkTimeout(java.util.concurrent.Executor executor, int milliseconds) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("The Java7 method Connection.setNetworkTimeout() is not supported.");
    }

    /**
     * @since Java 7
     */
    @Override
    public void setSchema(String schema) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("The Java7 method Connection.setSchema() is not supported.");
    }

    /**
     * @since Java 7
     */
    @Override
    public String getSchema() throws SQLException
    {
        return null;
    }

    /**
     * @since Java 7
     */
    @Override
    public void abort(java.util.concurrent.Executor executor) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("The Java7 method Connection.abort() is not supported.");
    }
}
