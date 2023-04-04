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
import java.io.IOException;
import java.nio.file.*;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.scidb.jdbc.Connection;

/**
 * Testing building data for all builtin types, and getString()/getDouble().
 *
 * An array is created that contains at least one attribute for each builtin type.
 * For each numeric field two types are created, that are filled with the min and max values of this type.
 * The function ResultSet.getString() may be used to retrieve all types.
 * The function ResultSet.getDouble() may be used to retrieve all numeric types.
 *
 * @author Donghui Zhang
 */
class Types
{
    public static void main (String[]args) throws IOException, SQLException
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

        // Send a query that builds min/max values for all builtin-types.
        ResultSet res =
            st.executeQuery("join(build(<v_bool:bool>[i=0:0,1,0], true)," +
                "join(build(<v_char:char>[i=0:0,1,0], 'c')," +
                "join(build(<v_datetime:datetime>[i=0:0,1,0], '2016-04-04 13:30:27')," +
                "join(build(<v_datetimetz:datetimetz>[i=0:0,1,0], '2016-04-04 13:30:27 -04:00')," +
                "join(build(<v_double:double>[i=0:0,1,0], double(3.14))," +
                "join(build(<v_float:float>[i=0:0,1,0], float(3.14))," +
                "join(build(<v_int8min:int8>[i=0:0,1,0], int8(-128))," +
                "join(build(<v_int8max:int8>[i=0:0,1,0], int8(127))," +
                "join(build(<v_int16min:int16>[i=0:0,1,0], int16(-32768))," +
                "join(build(<v_int16max:int16>[i=0:0,1,0], int16(32767))," +
                "join(build(<v_int32min:int32>[i=0:0,1,0], int32(-2147483648))," +
                "join(build(<v_int32max:int32>[i=0:0,1,0], int32(2147483647))," +
                "join(build(<v_int64min:int64>[i=0:0,1,0], int64(-9223372036854775807-1))," +
                "join(build(<v_int64max:int64>[i=0:0,1,0], int64(9223372036854775807))," +
                "join(build(<v_string:string>[i=0:0,1,0], 'hello world')," +
                "join(build(<v_uint8min:uint8>[i=0:0,1,0], uint8(0))," +
                "join(build(<v_uint8max:uint8>[i=0:0,1,0], uint8(-1))," +
                "join(build(<v_uint16min:uint16>[i=0:0,1,0], uint16(0))," +
                "join(build(<v_uint16max:uint16>[i=0:0,1,0], uint16(-1))," +
                "join(build(<v_uint32min:uint32>[i=0:0,1,0], uint32(0))," +
                "join(build(<v_uint32max:uint32>[i=0:0,1,0], uint32(-1))," +
                "join(build(<v_uint64min:uint32>[i=0:0,1,0], uint32(0))," +
                "     build(<v_uint64max:uint64>[i=0:0,1,0], uint64(-1))))))))))))))))))))))))"
		       );
        ResultSetMetaData meta = res.getMetaData ();
        while (res.next())
        {
            for (int j = 1; j <= meta.getColumnCount (); ++j)
            {
                String resultOfGetDouble = "EXCEPTION";
                try
                {
                    resultOfGetDouble = "" + res.getDouble(j);
                } catch (SQLException e)
                {}

                System.out.print("<" + meta.getColumnName(j) + ": " + meta.getColumnTypeName(j) + ">" +
                                   "  getColumnType()=" + JDBCType.valueOf(meta.getColumnType(j)).getName());
                switch (meta.getColumnType(j)) {
                case java.sql.Types.BOOLEAN:
                    System.out.println("  getBoolean()=" + res.getBoolean(j));
                    break;
                case java.sql.Types.CHAR:
                    System.out.println("  getString()=" + res.getString(j));
                    break;
                case java.sql.Types.DOUBLE:
                    System.out.println("  getDouble()=" + res.getDouble(j));
                    break;
                case java.sql.Types.FLOAT:
                    System.out.println("  getFloat()=" + res.getFloat(j));
                    break;
                case java.sql.Types.TINYINT:
                    System.out.println("  getByte()=" + res.getByte(j) + ", getShort()=" + res.getShort(j) + ", getInt()=" + res.getInt(j) + ", getLong()=" + res.getLong(j));
                    break;
                case java.sql.Types.SMALLINT:
                    System.out.println("  getShort()=" + res.getShort(j) + ", getInt()=" + res.getInt(j) + ", getLong()=" + res.getLong(j));
                    break;
                case java.sql.Types.INTEGER:
                    System.out.println("  getInt()=" + res.getInt(j) + ", getLong()=" + res.getLong(j));
                    break;
                case java.sql.Types.BIGINT:
                    System.out.println("  getLong()=" + res.getLong(j));
                    break;
                case java.sql.Types.VARCHAR:
                    System.out.println("  getString()=" + res.getString(j));
                    break;
                default:
                    System.out.println("  NO_SPECIAL_GETTER");
                }
                System.out.println("  getString()='" + res.getString(j) + "'" +
                                   "  getDouble()=" + resultOfGetDouble);
            }
        }
    }
}
