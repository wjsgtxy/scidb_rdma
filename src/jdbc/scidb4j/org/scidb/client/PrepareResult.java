/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2015-2019 SciDB, Inc.
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

import org.scidb.io.network.Message.QueryResult;
import org.scidb.io.network.ScidbMsg;

/**
 * Response received from the server, in response to a mtPrepareQuery message.
 * @see ClientMessageHandleJob::postPrepareQuery() for what is in the message.
 * @see org.scidb.client.Result.java for the result message of an mtExecuteQuery.
 */
public class PrepareResult
{
    private QueryID queryId;
    private boolean selective;
    private String explainLogical;
    private boolean exclusiveArrayAccess;

    /**
     * Constructor
     * @param result Query result network message
     * @param conn Connection
     */
    public PrepareResult(QueryResult result, Connection conn)
    {
        ScidbMsg.QueryResult rec = result.getRecord();

        this.queryId = result.getHeader().queryID;
        this.selective = rec.getSelective();
        this.explainLogical = rec.getExplainLogical();
        this.exclusiveArrayAccess = rec.getExclusiveArrayAccess();

        if (rec.getWarningsCount() > 0 && conn.getWarningCallback() != null)
        {
            for (ScidbMsg.QueryResult.Warning warn : rec.getWarningsList())
            {
                conn.getWarningCallback().handleWarning(warn.getWhatStr());
            }
        }
    }

    /**
     * Returns result query ID
     * @return Query ID
     */
    public QueryID getQueryId()
    {
        return queryId;
    }

    /// @return whether the query will return any result array.
    public boolean getSelective()
    {
        return selective;
    }

    /**
     * Returns explained logical plan
     * @return Logical plan
     */
    public String getExplainLogical()
    {
        return explainLogical;
    }

    /// @return exclusiveArrayAccess.
    public boolean getExclusiveArrayAccess()
    {
        return exclusiveArrayAccess;
    }
}
