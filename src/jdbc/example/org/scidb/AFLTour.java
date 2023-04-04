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

import java.io.IOException;
import java.nio.file.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.scidb.jdbc.Connection;

/**
 * This class is essentially the same as the AFLTour sample code in the SciDB-JDBC user manual.
 * It demonstrates JDBC queries that leave an array on the server.
 *
 * @author Donghui Zhang
 */
class AFLTour
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
            System.out.println(e);
            System.exit(1);
        }

        // Create an array, and store data into it.
        try {
            st.executeQuery("create array persistent<value:int64>[i=0:5,6,0]");
            st.executeQuery("store(build(persistent, i), persistent)");
            conn.commit();
        }
        catch (SQLException e)
        {
            System.err.println(e);
            System.exit(1);
        }

        // Issue a filter() query.
        try {
            ResultSet res = st.executeQuery("filter(persistent, value>3)");

            // Print the header.
            System.out.println("{i} value");

            // Print the result.
            while (res.next())
            {
                // The dimensions.
                System.out.print("{");
                System.out.print(res.getLong("i"));
                System.out.print("} ");

                // The attributes.
                System.out.print(res.getLong("value"));
                System.out.print("\n");
            }
        }
        catch (SQLException e)
        {
            System.err.println(e);
            System.exit(1);
        }
    }
}
