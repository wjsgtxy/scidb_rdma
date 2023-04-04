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

package org.scidb.client;

import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.Properties;
import java.util.Base64;

import org.scidb.io.network.Message;
import org.scidb.io.network.Message.QueryResult;
import org.scidb.io.network.Network;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * SciDB connection
 */
public class Connection
{
    public static final String USERNAME_PROP="user-name";
    public static final String PASSWORD_PROP="user-password";
    public static final String PRIORITY_PROP="session-priority";
    public static final String QLANG_PROP="query-language";

    private Network _net;
    private boolean _afl = true;
    private QueryID _queryId = new QueryID();
    private String _queryStr = "";
    private WarningCallback _warningCallback;
    private List<Result> _activeQueries = new ArrayList<Result>();

    private static Logger log = Logger.getLogger(Connection.class.getName());

    private QueryID    getQueryId() { return _queryId; }
    private String     getQueryStr() { return _queryStr; }
    private List<Result> getActiveQueries() { return _activeQueries; }

    private void       setQueryId(QueryID newValue) { _queryId = newValue; }
    private void       setQueryStr(String newValue) { _queryStr = newValue; }

    /**
     * Constructor
     */
    public Connection()
    {
        _net = new Network();
    }

    /**
     * Connect to specified SciDB instance
     * @param host Host name
     * @param port Port number
     * @throws SciDBException
     * @throws SciDBException, IOException
     */
    public void connect(String host, int port) throws SciDBException, IOException
    {
        getNetwork().connect(host, port);
    }

    /**
     * Close network connection
     * @throws IOException
     */
    public void close() throws IOException
    {
        getNetwork().disconnect();
    }

    /**
     * Check if connected to server
     * @return true if connected
     */
    public boolean connected()
    {
        return getNetwork().isConnected();
    }

    /**
     * Prepare query
     * @param queryString Query string
     * @return Result with prepared query ID
     * @throws SciDBException
     * @throws IOException
     */
    public PrepareResult prepare(String queryString) throws SciDBException, IOException
    {
        setQueryStr(queryString);
        log.fine(String.format("Preparing query '%s'", queryString));
        Message msg = new Message.Query(
            null, queryString, isAfl(), "", false);  // false = no execute
        getNetwork().write(msg);
        msg = getNetwork().read();

        switch (msg.getHeader().messageType)
        {
            case Message.mtQueryResult:
                log.fine("Got result from server");
                PrepareResult res = new PrepareResult((QueryResult) msg, this);
                setQueryId(res.getQueryId());
                return res;

            case Message.mtError:
                log.fine("Got error message from server");
                throw new SciDBException((Message.Error) msg);

            default:
                log.severe("Got unhandled network message during execution");
                throw new SciDBException(String.format("Can not handle network message '%s'",
                        msg.getHeader().messageType));
        }
    }

    public Network getNetwork()
    {
        return _net;
    }

    /**
     * Set query execution mode to AFL or language
     * @param afl true - AFL, false - AQL
     */
    public void setAfl(boolean afl)
    {
        _afl = afl;
    }

    /**
     * Return AFL flag
     * @return true if AFL mode
     */
    public boolean isAfl()
    {
        return _afl;
    }

    /**
     * Return AQL flag
     * @return true if AQL mode
     */
    public boolean isAql()
    {
        return !_afl;
    }

    /**
     * Execute prepared query
     * @return Array result
     * @throws IOException
     * @throws SciDBException
     */
    public Result execute() throws IOException, SciDBException
    {
        assert(getQueryId() != null);

        log.fine(String.format("Executing query %s",
                               getQueryId().toString()));


        if (!getQueryId().isValid())
        {
            throw new SciDBException("Query not prepared"+
                                     String.format(" -- query %s",
                                                   getQueryId().toString()));
        }

        Message msg = new Message.Query(
            getQueryId(), getQueryStr(), isAfl(), "", true);

        long startTime = System.currentTimeMillis();

        getNetwork().write(msg);
        msg = getNetwork().read();

        long elapsedTimeMillis = System.currentTimeMillis() - startTime;

        switch (msg.getHeader().messageType)
        {
            case Message.mtQueryResult:
                log.fine("Got result from server");
                Result res = new Result((QueryResult) msg, this);
                res.setElapsedTimeMillis(elapsedTimeMillis);
                getActiveQueries().add(res);
                return res;

            case Message.mtError:
                log.fine("Got error message from server");
                throw new SciDBException((Message.Error) msg);

            default:
                log.severe("Got unhandled network message during execution");
                throw new SciDBException(String.format("Can not handle network message '%s'",
                        msg.getHeader().messageType));
        }
    }

    /**
     * Commit query
     */
    public void commit() throws IOException, SciDBException
    {
        List<Result> activeQueries = new ArrayList<Result>(
            getActiveQueries());
        getActiveQueries().clear();

        for (Result res: activeQueries)
        {
            if (res.getAutoCommit()) {
                continue;
            }

            log.fine(String.format("Committing query %s",
                                   res.getQueryId().toString()));
            getNetwork().write(new Message.CompleteQuery(res.getQueryId()));
            Message msg = getNetwork().read();

            switch (msg.getHeader().messageType)
            {
                case Message.mtError:
                    Message.Error err = (Message.Error) msg;
                    if (err.getRecord().getLongErrorCode() != 0)
                    {
                        log.fine("Got error message from server");
                        throw new SciDBException((Message.Error) msg);
                    }
                    log.fine("Query completed successfully");
                    break;

                default:
                    log.severe("Got unhandled network message during query completing");
                    throw new SciDBException(String.format("Can not handle network message '%s'",
                            msg.getHeader().messageType));
            }
        }
    }

    /**
     * Rollback query
     */
    public void rollback() throws IOException, SciDBException
    {
        List<Result> activeQueries = new ArrayList<Result>(
            getActiveQueries());
        getActiveQueries().clear();

        for (Result res: activeQueries)
        {
            log.fine(String.format("Rolling back query %s",
                                   res.getQueryId().toString()));

            getNetwork().write(new Message.AbortQuery(res.getQueryId()));
            Message msg = getNetwork().read();

            switch (msg.getHeader().messageType)
            {
                case Message.mtError:
                    Message.Error err = (Message.Error) msg;
                    if (err.getRecord().getLongErrorCode() != 0)
                    {
                        log.fine("Got error message from server");
                        throw new SciDBException((Message.Error) msg);
                    }
                    log.fine("Query aborted successfully");
                    break;

                default:
                    log.severe("Got unhandled network message during query aborting");
                    throw new SciDBException(String.format("Can not handle network message '%s'",
                            msg.getHeader().messageType));
            }
        }
    }

    /**
     * Set warning callback for registering execution warnings
     * @param callback Callback object
     */
    public void setWarningCallback(WarningCallback warningCallback)
    {
        _warningCallback = warningCallback;
    }

    /**
     * Returns warning callback
     * @return Callback object
     */
    public WarningCallback getWarningCallback()
    {
        return _warningCallback;
    }

    public void setTimeout(int timeout) throws SocketException
    {
        getNetwork().setTimeout(timeout);
    }

    public int getTimeout() throws SocketException
    {
        return getNetwork().getTimeout();
    }


    /**
     * Tells SciDB that a new client is starting.
     * Performs the authentication handshake (even in trust mode).
     *
     * @param userName - passed to SciDB in AuthLogon message
     * @param userPassword - passed to SciDB in AuthResponse message
     */
    public void startNewClient(
        String userName,
        String userPassword)
        throws IOException, SciDBException, java.sql.SQLException
    {
        startNewClient(new SessionProperties(), userName, userPassword);
    }

    /**
     * Tells SciDB that a new client is starting.
     * Performs the authentication handshake (even in trust mode).
     *
     * @param props - session properties to request when connecting
     * Available properties:
     * USERNAME_PROP = <string>
     * PASSWORD_PROP = <string>
     * PRIORITY_PROP = "NORMAL" | "ADMIN"
     * QLANG_PROP    = "AFL" | "AQL"
     */
    public void startNewClient(Properties props)
            throws IOException, SciDBException, java.sql.SQLException
    {
        String userName     = props.getProperty(USERNAME_PROP,"");
        String userPassword = props.getProperty(PASSWORD_PROP,"");
        String priorityStr  = props.getProperty(PRIORITY_PROP,"NORMAL");
        String lang  = props.getProperty(QLANG_PROP,"afl");

        if (!lang.toLowerCase().equals("afl")) {
            setAfl(false);
        }

        SessionProperties sessProps = null;
        if (priorityStr.equals("ADMIN")) {
            sessProps = new SessionProperties(SessionProperties.PRTY_ADMIN);
        } else {
            sessProps = new SessionProperties();
        }

        startNewClient(sessProps, userName, userPassword);
    }

    private void validateCredential(String cred)
            throws java.sql.SQLInvalidAuthorizationSpecException
    {
        if (cred == null || cred.length() == 0) {
            throw new java.sql.SQLInvalidAuthorizationSpecException(
                    "Credential(s) not specified");
        }
    }

    /**
     * Tells SciDB that a new client is starting.
     *
     * @param props - session properties to request when connecting
     * @param userName - passed to SciDB in AuthLogon message
     * @param userPassword - passed to SciDB in AuthResponse message
     */
    public void startNewClient( SessionProperties props,
                                String userName,
                                String userPassword)
            throws IOException, SciDBException, java.sql.SQLException
    {
        getNetwork().write(new Message.AuthLogon(null, userName, props));
        Message msg = getNetwork().read();
        if (msg.getHeader().messageType == Message.mtAuthError) {
            Message.Error err = (Message.Error) msg;
            log.fine("Got error message from server");
            throw new SciDBException((Message.Error) msg);
        }
        if (msg.getHeader().messageType != Message.mtAuthChallenge) {
            String err = String.format(
                "Bad authentication handshake, expecting %s but got %s",
                Message.mtAuthChallenge, msg.getHeader().messageType);
            log.severe(err);
            throw new java.sql.SQLNonTransientConnectionException(err);
        }
        Message.AuthChallenge challengeMsg = (Message.AuthChallenge) msg;

        // In the future we'll do some computation based on the
        // challenge text, but for AUTH_RAW we send the unsalted
        // SHA-512 hash of the password if we have one.  If we don't,
        // hope that the SciDB server is in trust mode.  Either way,
        // the server demands an AuthResponse.

        if (userPassword != null && userPassword.length() != 0) {
            try {
                // Digest with SHA-512
                MessageDigest messageDigest = MessageDigest.getInstance("SHA-512");
                messageDigest.update(userPassword.getBytes());
                byte[] messageDigestSHA512 = messageDigest.digest();

                // Encode with Base64
                Base64.Encoder encoder = Base64.getEncoder();
                String encodeResult = encoder.encodeToString(messageDigestSHA512);

                userPassword = encodeResult;
            }
            catch (NoSuchAlgorithmException ex) {
                log.severe("Unable to get the MsgDigest");
                throw new SciDBException("Unable to get the SHA-512 MsgDigest");
            }
        }
        else {
            userPassword = "Trust me!";
        }

        // Transmit the AuthResponse.
        getNetwork().write(new Message.AuthResponse(
                               null, challengeMsg.getRecord().getCookie(), userPassword));
        msg = getNetwork().read();
        if (msg.getHeader().messageType == Message.mtAuthError) {
            Message.Error err = (Message.Error) msg;
            log.fine("Got error message from server");
            throw new SciDBException((Message.Error) msg);
        }
        if (msg.getHeader().messageType != Message.mtAuthComplete) {
            String err = String.format(
                "Bad authentication handshake, expecting %s but got %s",
                Message.mtAuthComplete, msg.getHeader().messageType);
            log.severe(err);
            throw new java.sql.SQLNonTransientConnectionException(err);
        }
        Message.AuthComplete doneMsg = (Message.AuthComplete) msg;
        if (!doneMsg.getRecord().getAuthenticated()) {
            String err = String.format(
                "newClientStart authentication error: %s",
                doneMsg.getRecord().getReason());
            log.severe(err);
            throw new java.sql.SQLInvalidAuthorizationSpecException(err);
        }
    }
}
