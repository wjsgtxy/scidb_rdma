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

import org.scidb.io.network.Message.QueryResult;
import org.scidb.io.network.ScidbMsg;

/**
 * Query result
 */
public class Result
{
    private Schema schema;
    private QueryID queryId;
    private boolean selective;
    private boolean autoCommit;
    private String explainLogical;
    private String explainPhysical;
    private long elapsedTimeMillis;

    // The result array.
    private Array array;

    /**
     * Constructor
     * @param result Query result network message
     * @param conn Connection
     */
    public Result(QueryResult result, Connection conn)
    throws SciDBException
    {
        ScidbMsg.QueryResult rec = result.getRecord();
        this.queryId = result.getHeader().queryID;
        this.selective = rec.getSelective();
        this.autoCommit = rec.hasAutoCommit() ? rec.getAutoCommit() : false;
        this.explainLogical = rec.getExplainLogical();
        this.explainPhysical = rec.getExplainPhysical();

        if (rec.getWarningsCount() > 0 && conn.getWarningCallback() != null)
        {
            for (ScidbMsg.QueryResult.Warning warn : rec.getWarningsList())
            {
                conn.getWarningCallback().handleWarning(warn.getWhatStr());
            }
        }

        // Set result array.
        this.array = null;

        if (this.selective) {
            String schemaName = rec.getArrayName();
            Schema.Attribute[] attributes = new Schema.Attribute[rec.getAttributesCount()];
            Schema.Dimension[] dimensions = new Schema.Dimension[rec.getDimensionsCount()];

            int i = 0;
            for (ScidbMsg.QueryResult.AttributeDesc att : rec.getAttributesList())
            {
                attributes[i] = new Schema.Attribute(att.getId(), att.getName(), att.getType(), att.getFlags());
                i++;
            }

            i = 0;
            for (ScidbMsg.QueryResult.DimensionDesc dim : rec.getDimensionsList())
            {
                dimensions[i] = new Schema.Dimension(dim.getName(),
                                                     dim.getStartMin(),
                                                     dim.getCurrStart(),
                                                     dim.getCurrEnd(),
                                                     dim.getEndMax(),
                                                     dim.getChunkInterval());
                i++;
            }

            this.schema = new Schema(schemaName, attributes, dimensions);
            this.array = new Array(this.queryId, this.schema, conn.getNetwork());
        }
    }

    public void setElapsedTimeMillis(long elapsed)
    {
        elapsedTimeMillis = elapsed;
    }

    public long getElapsedTimeMillis()
    {
        return elapsedTimeMillis;
    }

    /**
     * Returns result schema
     * @return Schema
     */
    public Schema getSchema()
    {
        return schema;
    }

    /**
     * Returns result query ID
     * @return Query ID
     */
    public QueryID getQueryId()
    {
        return queryId;
    }

    /// @return the result array.
    public Array getArray()
    {
        return this.array;
    }

    /// @return whether the query result has a result array.
    public boolean getSelective()
    {
        return selective;
    }

    /// @return whether the query result has a result array.
    public boolean getAutoCommit()
    {
        return autoCommit;
    }

    /**
     * Returns explained logical plan
     * @return Logical plan
     */
    public String getExplainLogical()
    {
        return explainLogical;
    }

    /**
     * Returns explained physical plan
     * @return Physical plan
     */
    public String getExplainPhysical()
    {
        return explainPhysical;
    }
}
