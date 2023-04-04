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
package org.scidb.iquery;

import java.util.Arrays;
import java.util.Date;
import java.util.TimeZone;
import static java.lang.System.exit;
import static java.lang.System.out;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.PrintStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.ByteArrayOutputStream;
import java.text.SimpleDateFormat;
import java.sql.SQLException;

import org.scidb.client.*;
import org.scidb.jdbc.ResultSet;
import org.scidb.client.SciDBException;

/**
 * A class that helps format data of some SciDB built-in type according to dsv, csv, or tsv format (plus csv:l and tsv:l).
 * @author Donghui Zhang
 * @see Value.h::Value::ValueFormatter.
 * @see ArrayWriter.cpp.
 */
public class XsvFormatter
{
    /**
     * The parameters that differentiates the different formats.
     * @author Donghui Zhang
     */
    public class Parameters
    {
        boolean printPretty = false; // whether to surround coordinates with curly brackets.
        boolean printLabel = false;  // whether to print label.
        boolean printCoords = false; // whether to print coordinates.
        char delim = ',';            // the deliminator.
        int precision;               // the precision used to print floating-point types.

        boolean getPrintLabel()
        {
            return printLabel;
        }

        boolean getPrintPretty()
        {
            return printPretty;
        }

        boolean getPrintCoords()
        {
            return printCoords;
        }

        char getDelim()
        {
            return delim;
        }

        int getPrecision()
        {
            return precision;
        }

        public Parameters(String format, int precision)
        throws SciDBException
        {
            // To lower case.
            String baseFmt = format.toLowerCase();

            // Get rid of the colon phrase at the end.
            int posOfColon = baseFmt.indexOf(':');
            if ( posOfColon != -1) {
                baseFmt = baseFmt.substring(0, posOfColon);
            }

            if (baseFmt.equals("csv")) {
                printLabel = true;
            } else if (baseFmt.equals("csv+")) {
                printCoords = true;
                printLabel = true;
            } else if (baseFmt.equals("lcsv+")) {
                printCoords = true;
                printLabel = true;
            } else if (baseFmt.equals("dcsv")) {
                printCoords = true;
                printLabel = true;
                printPretty = true;
            } else if (baseFmt.equals("tsv")) {
                delim = '\t';
                printLabel = true;
            } else if (baseFmt.equals("tsv+")) {
                delim = '\t';
                printCoords = true;
                printLabel = true;
            } else if (baseFmt.equals("ltsv+")) {
                delim = '\t';
                printCoords = true;
                printLabel = true;
            } else {
                throw new SciDBException("The format string \"" + format + "\" is not recognized.");
            }

            this.precision = precision;
        }
    }

    /**
     * A class that simulates C++ Value::Formatter, to format a single value.
     * @author Donghui Zhang
     */
    public static class ValueFormatter
    {
        private static char quoteChar = '\'';
        private static String nullStr = "null";
        private static String nan = "nan";
        private static String inf = "inf";
        private static String negativeInf = "-inf";

        public static String formatNull(int missingReason)
        {
            StringBuilder sb = new StringBuilder();
            if (missingReason == 0) {
                sb.append(nullStr);
            } else {
                sb.append("?");
                sb.append(missingReason);
            }
            return sb.toString();
        }

        public static String formatChar(char ch)
        {
            StringBuilder sb = new StringBuilder();
            sb.append(quoteChar);
            if (ch == '\0') {
                sb.append("\\0");
            } else if (ch == '\n') {
                sb.append("\\n");
            } else if (ch == '\r') {
                sb.append("\\r");
            } else if (ch == '\t') {
                sb.append("\\t");
            } else if (ch == '\f') {
                sb.append("\\f");
            } else {
                if (ch == '\'' || ch == '\\') {
                    sb.append('\\');
                }
                sb.append(ch);
            }
            sb.append(quoteChar);

            return sb.toString();
        }

        public static String formatString(String s)
        {
            StringBuilder sb = new StringBuilder();
            sb.append(quoteChar);
            for (int i=0; i<s.length(); ++i) {
                char c = s.charAt(i);
                if (c == quoteChar) {
                    sb.append('\\');
                    sb.append(c);
                } else if (c == '\\') {
                    sb.append("\\\\");
                } else {
                    sb.append(c);
                }
            }
            sb.append(quoteChar);

            return sb.toString();
        }

        public static String formatDouble(double val, int precision)
        {
            if (Double.isNaN(val)) {
                // Technically this is not needed; but Java would print 'NaN' which does not match C++.
                return nan;
            }
            else if (Double.isInfinite(val)) {
                // Technically this is not needed; but Java would print 'Infinity' or '-Infinity' which does not match C++.
                return val>0 ? inf : negativeInf;
            }

            // Set precision (default is 6). E.g. 12.34567 --> 12.3457.
            String ret = String.format("%." + precision + "g", val);

            // Remove dot and subsequent zeros. E.g. 32.000 --> 32.
            ret = ret.replaceAll("\\.0+$", "");

            // Remove trailing zeros, if there is a dot. E.g. 3.14000 --> 3.14.
            if (ret.indexOf('.') != -1) {
                ret = ret.replaceAll("0+$", "");
            }
            return ret;
        }

        public static String formatBoolean(boolean b)
        {
            return b ? "true" : "false";
        }

        public static String formatDatetime(long numSeconds)
        {
            return formatDatetime(numSeconds, true);  // print quoteChar
        }

        public static String formatDatetime(long numSeconds, boolean printQuote)
        {
            StringBuilder sb = new StringBuilder();
            if (printQuote)
            {
                sb.append(quoteChar);
            }
            Date date = new Date(numSeconds*1000); // Java wants milliseconds
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            sb.append( dateFormat.format(date) );
            if (printQuote)
            {
                sb.append(quoteChar);
            }

            return sb.toString();
        }

        public static String formatDatetimetz(long[] numSecondsWithOffset)
        {
            return formatDatetimetz(numSecondsWithOffset, true);  // print quoteChar
        }

        public static String formatDatetimetz(long[] numSecondsWithOffset, boolean printQuote)
        {
            long numSeconds = numSecondsWithOffset[0];
            long offset = numSecondsWithOffset[1];

            StringBuilder sbOverall = new StringBuilder();
            if (printQuote)
            {
                sbOverall.append(quoteChar);
            }

            // Build a string in the form of "+04:00"
            StringBuilder sbOffset = new StringBuilder();
            if (offset>0) {
                sbOffset.append('+');
            } else {
                sbOffset.append('-');
                offset = -offset;
            }
            long hour = offset / 3600;
            if (hour<10) {
                sbOffset.append('0');
            }
            sbOffset.append(hour);
            sbOffset.append(':');
            long minute = (offset % 3600) / 60;
            if (minute < 10) {
                sbOffset.append('0');
            }
            sbOffset.append(minute);
            String strOffset = sbOffset.toString();

            // Format the date part.
            Date date = new Date(numSeconds*1000); // Java wants milliseconds
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            sbOverall.append(dateFormat.format(date));

            // Format the offset part.
            sbOverall.append(" " + strOffset);

            if (printQuote)
            {
                sbOverall.append(quoteChar);
            }
            return sbOverall.toString();
        }
    }

    /**
     * Prints the content of an array to a PrintStream according to formatting-instruction parameters.
     * @param array      an input SciDB array.
     * @param f          an output PrintStream.
     * @param parameters instructions how to print.
     *
     * @see ArrayWriter.cpp::saveXsvFormat().
     */
    public static long printArray(
            Array array,
            PrintStream f,
            Parameters parameters)
    throws IOException, SQLException, SciDBException
    {
        // Note from Donghui Zhang:
        // iquery is fine both when there is no bitmap, or when there is no attribute.
        // We assume there is always a bitmap attribute, plus a real data attribute.
        Schema schema = array.getSchema();
        Schema.Attribute[] attributes = schema.getAttributes();
        Schema.Dimension[] dimensions = schema.getDimensions();
        assert(attributes.length>=1);
        int numAttrsWithoutBitmap = attributes.length - 1;
        if (schema.getEmptyIndicator()==null) {
            numAttrsWithoutBitmap = attributes.length;
        }

        // Print the labels.
        // Note that this is on the client side, so parallel save is not an option.
        if (parameters.getPrintLabel()) {
            // Dimensions first.
            if (parameters.getPrintCoords()) {
                if (parameters.getPrintPretty())
                    f.print('{');
                for (int i = 0; i < dimensions.length; ++i) {
                    if (i!=0) {
                        f.print(parameters.getDelim());
                    }
                    f.print(dimensions[i].getName());
                }
                if (parameters.getPrintPretty()) {
                    f.print("} ");
                } else {
                    f.print(parameters.getDelim());
                }
            }

            // Then attributes.
            for (int i = 0; i < numAttrsWithoutBitmap; ++i) {
                if (i!=0) {
                    f.print(parameters.getDelim());
                }
                f.print(attributes[i].getName());
            }
            f.println();
        }

        // Time to walk the chunks!
        long count = 0; // how many rows printed.
        ResultSet resultSet = new ResultSet(array);

        while(resultSet.next())
        {
            // Print the coordinates.
            if (parameters.getPrintCoords()) {
                if (parameters.getPrintPretty()) {
                    f.print( '{' );
                }
                for (int i = 0; i < dimensions.length; ++i) {
                    int columnIndex = i+1; // column index is 1-based, not 0-based.
                    if (i!=0) {
                        f.print( parameters.getDelim() );
                    }
                    f.print( resultSet.getLong(columnIndex) );
                }
                if (parameters.getPrintPretty()) {
                    f.print( "} " );
                } else {
                    f.print( parameters.getDelim() );
                }
            }

            // Print the attributes.
            for (int i = 0; i < numAttrsWithoutBitmap; ++i) {
                int columnIndex = dimensions.length + i + 1;
                if (i!=0) {
                    f.print(parameters.getDelim());
                }

                switch (attributes[i].getTypeEnum()) {
                case TE_CHAR:
                {
                    String s = resultSet.getString(columnIndex);
                    if (resultSet.wasNull()) {
                        f.print(ValueFormatter.formatNull(resultSet.getMissingReason()));
                        break;
                    }
                    assert(s!=null && s.length()==1);
                    char ch = s.charAt(0);
                    f.print(ValueFormatter.formatChar(ch));
                    break;
                }
                case TE_STRING:
                {
                    String str = resultSet.getString(columnIndex);
                    if (resultSet.wasNull()) {
                        f.print(ValueFormatter.formatNull(resultSet.getMissingReason()));
                        break;
                    }
                    assert(str!=null);
                    f.print(ValueFormatter.formatString(str));
                    break;
                }
                case TE_BOOL:
                {
                    boolean b = resultSet.getBoolean(columnIndex);
                    if (resultSet.wasNull()) {
                        f.print(ValueFormatter.formatNull(resultSet.getMissingReason()));
                        break;
                    }
                    f.print(ValueFormatter.formatBoolean(b));
                    break;
                }
                case TE_INT8:
                case TE_INT16:
                case TE_INT32:
                case TE_INT64:
                {
                    long l = resultSet.getLong(columnIndex);
                    if (resultSet.wasNull()) {
                        f.print(ValueFormatter.formatNull(resultSet.getMissingReason()));
                        break;
                    }
                    f.print(l);
                    break;
                }
                case TE_UINT8:
                case TE_UINT16:
                {
                    int v = resultSet.getInt(columnIndex);
                    if (resultSet.wasNull()) {
                        f.print(ValueFormatter.formatNull(resultSet.getMissingReason()));
                        break;
                    }
                    f.print(Integer.toUnsignedLong(v));
                    break;
                }
                case TE_UINT32:
                {
                    long v = resultSet.getLong(columnIndex);
                    if (resultSet.wasNull()) {
                        f.print(ValueFormatter.formatNull(resultSet.getMissingReason()));
                        break;
                    }
                    f.print(Long.toUnsignedString(v));
                    break;
                }
                case TE_UINT64:
                {
                    BigDecimal v = resultSet.getBigDecimal(columnIndex);
                    if (resultSet.wasNull()) {
                        f.print(ValueFormatter.formatNull(resultSet.getMissingReason()));
                        break;
                    }
                    f.print(Long.toUnsignedString(v.longValue()));
                    break;
                }
                case TE_FLOAT:
                case TE_DOUBLE:
                {
                    double val = resultSet.getDouble(columnIndex);
                    if (resultSet.wasNull()) {
                        f.print(ValueFormatter.formatNull(resultSet.getMissingReason()));
                        break;
                    }
                    f.print(ValueFormatter.formatDouble(val, parameters.getPrecision()));
                    break;
                }
                case TE_DATETIME:
                {
                    long val = resultSet.getDatetime(columnIndex);
                    if (resultSet.wasNull()) {
                        f.print(ValueFormatter.formatNull(resultSet.getMissingReason()));
                        break;
                    }
                    f.print(ValueFormatter.formatDatetime(val));
                    break;
                }
                case TE_DATETIMETZ:
                {
                    long[] val = resultSet.getDatetimetz(columnIndex);
                    if (resultSet.wasNull()) {
                        f.print(ValueFormatter.formatNull(resultSet.getMissingReason()));
                        break;
                    }
                    f.print(ValueFormatter.formatDatetimetz(val));
                    break;
                }
                default:
                    throw new SciDBException("Type \"" + attributes[i].getType() + "\" is not supported.");
                }
            }

            // Print "\n".
            count += 1;
            f.println();
        } // end while (resultSet.next())

        // Return how many cells were printed.
        return count;
    }
}
