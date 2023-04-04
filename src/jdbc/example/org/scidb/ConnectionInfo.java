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
package org.scidb;

import org.scidb.jdbc.IResultSetWrapper;
import org.scidb.client.AuthenticationFile;
import org.scidb.client.ConfigUser;
import org.scidb.client.ConfigUserException;
import org.scidb.jdbc.Connection;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * This class supports the other example code by parsing command-line parameters.
 *
 * A user calling one of the example code provides up to 3 command-line parameters:
 *   [hostname [port [authfile]]]
 *
 * This class parses whatever is provided, into a quadruple
 *   hostname, port, username, password.
 *
 * @author Donghui Zhang
 */
class ConnectionInfo
{
    public String hostname = "localhost";
    public int port = 1239;
    public String username = "";
    public String password = "";

    /**
     * Programs in this example directory take up to three command-line parameters:
     * hostname, port, authFile.
     */
    public ConnectionInfo(String[] args) throws IOException, NumberFormatException
    {
        if (args.length >= 1)
        {
            hostname = args[0];
        }
        if (args.length >= 2)
        {
            port = Short.parseShort(args[1]);
        }
        if (args.length >= 3)
        {
            String authFileName = args[2];
            try
            {
                ConfigUser configUser =  ConfigUser.getInstance();
                configUser.verifySafeFile(authFileName, false);
            }
            catch (ConfigUserException e)
            {
                throw new IOException(e.getMessage());
            }
            AuthenticationFile authFile = new AuthenticationFile(authFileName);
            username = authFile.getUserName();
            password = authFile.getUserPassword();
        }
    }
}
