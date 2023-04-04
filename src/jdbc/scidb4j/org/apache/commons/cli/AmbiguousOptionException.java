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
/**
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

package org.apache.commons.cli;

import java.util.Collection;
import java.util.Iterator;

/**
 * Exception thrown when an option can't be identified from a partial name.
 *
 * @version $Id: AmbiguousOptionException.java 1669814 2015-03-28 18:09:26Z britter $
 * @since 1.3
 */
public class AmbiguousOptionException extends UnrecognizedOptionException
{
    /**
     * This exception {@code serialVersionUID}.
     */
    private static final long serialVersionUID = 5829816121277947229L;

    /** The list of options matching the partial name specified */
    private final Collection<String> matchingOptions;

    /**
     * Constructs a new AmbiguousOptionException.
     *
     * @param option          the partial option name
     * @param matchingOptions the options matching the name
     */
    public AmbiguousOptionException(String option, Collection<String> matchingOptions)
    {
        super(createMessage(option, matchingOptions), option);
        this.matchingOptions = matchingOptions;
    }

    /**
     * Returns the options matching the partial name.
     * @return a collection of options matching the name
     */
    public Collection<String> getMatchingOptions()
    {
        return matchingOptions;
    }

    /**
     * Build the exception message from the specified list of options.
     *
     * @param option
     * @param matchingOptions
     * @return
     */
    private static String createMessage(String option, Collection<String> matchingOptions)
    {
        StringBuilder buf = new StringBuilder("Ambiguous option: '");
        buf.append(option);
        buf.append("'  (could be: ");

        Iterator<String> it = matchingOptions.iterator();
        while (it.hasNext())
        {
            buf.append("'");
            buf.append(it.next());
            buf.append("'");
            if (it.hasNext())
            {
                buf.append(", ");
            }
        }
        buf.append(")");

        return buf.toString();
    }
}
