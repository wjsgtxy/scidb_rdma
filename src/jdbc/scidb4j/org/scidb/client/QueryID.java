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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class QueryID
{
    public static final int size = 16;
    private long coordId; // uint64_t
    private long id; // uint64_t

    /**
     * Default constructor
     */
    public QueryID()
    {
        coordId = ~0;
        id = 0;
    }

    /**
     * Construct QueryID from
     *
     * @param coordinatorId query coordinator ID
     * @param ID per-coordinator unique ID
     */
    public QueryID(long coordId, long id)
    {
        this.coordId = coordId;
        this.id = id;
    }

    public long getCoordinatorId()
    {
        return coordId;
    }

    public long getId()
    {
        return id;
    }

    public boolean isValid()
    {
        return (coordId >= 0 && id > 0);
    }

    public static QueryID parseFromBuffer(ByteBuffer buf)
    throws IOException
    {
        buf.order(ByteOrder.LITTLE_ENDIAN);
        long cId = buf.getLong();
        long id = buf.getLong();
        QueryID res = new QueryID(cId, id);
        return res;
    }

    public void writeToBuffer(ByteBuffer buf) throws IOException
    {
        buf.putLong(coordId);
        buf.putLong(id);
    }

    public String toString()
    {
        return String.format("%d.%d", coordId, id);
    }
}
