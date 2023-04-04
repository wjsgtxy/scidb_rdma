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

import org.scidb.io.network.Message;

/**
 * Simple SciDB exception
 */
public class SciDBException extends java.lang.Exception
{
    private static final long serialVersionUID = 1L;

    /**
     * Construct exception from string
     * @param message String message
     */
    public SciDBException(String message)
    {
        super(message);
    }

    /**
     * Construct exception from network error
     * @param err Network error
     */
    public SciDBException(Message.Error err)
    {
        super(err.getRecord().getWhatStr());
    }
}
