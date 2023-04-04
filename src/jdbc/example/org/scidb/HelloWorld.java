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

import org.scidb.ConnectionInfo;
import org.scidb.jdbc.Connection;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * A HelloWorld example that sends a build() query to the server to build a 'hello world' string.
 *
 * @author Donghui Zhang
 */
class HelloWorld
{
    public static void main(String [] args) throws IOException
    {
        // Connect to the server.
        Connection conn = null;
        Statement st = null;

        try
        {
            ConnectionInfo info = new ConnectionInfo(args);
            conn = new Connection(info.hostname, info.port, info.username, info.password);
            st = conn.createStatement();
        }
        catch (SQLException e)
        {
            System.err.println(e);
            System.exit(1);
        }

        // Send an AFL query, and print the result.
        try {
            ResultSet res = st.executeQuery("build(<v:string>[i=0:0,1,0], 'hello world')");

            // Print the header.
            System.out.println("{i} v");

            // Print the record.
            if (res.next())
            {
                System.out.print("{0} '");
                System.out.print(res.getString("v"));
                System.out.print("'\n");
            }
        }
        catch (SQLException e)
        {
            System.err.println(e);
            System.exit(1);
        }
    }
}
