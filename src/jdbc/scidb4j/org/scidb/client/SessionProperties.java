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

/// Session properties requested when connecting to SciDB
public class SessionProperties
{
    public static final int PRTY_NORMAL = 0;
    public static final int PRTY_ADMIN = 1;
    public static final int size = 4;
    private int _priority; // int32_t


    /// Runtime exception raised when an invalid priority is specified
    public static class InvalidPriorityException extends java.lang.RuntimeException
    {
        /**
         * Construct exception from string
         * @param message String message
         */
        public InvalidPriorityException(String message)
        {
            super(message);
        }
    }

    /**
     * Default constructor
     */
    public SessionProperties()
    {
        _priority = PRTY_NORMAL;
    }

    /**
     * Construct SessionProperties with a given priority
     *
     * @param priority of the session
     * @throws InvalidPriorityException
     */
    public SessionProperties(int priority)
    {
        this._priority = priority;
        if (!isValid())
        {
            throw new InvalidPriorityException(toString()+" session priority is invalid");
        }
    }

    /// @return session priority
    public int getPriority()
    {
        return _priority;
    }

    /// @return true if the current priority is valid
    private boolean isValid()
    {
        return (_priority == PRTY_NORMAL || _priority == PRTY_ADMIN);
    }

    /// @return string representation of thid object
    public String toString()
    {
        return String.format("%d", _priority);
    }
}
