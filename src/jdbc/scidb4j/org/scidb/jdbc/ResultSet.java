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
package org.scidb.jdbc;

import java.io.InputStream;
import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

import org.scidb.client.*;
import org.scidb.io.network.Message;
import org.scidb.iquery.XsvFormatter;

public class ResultSet implements java.sql.ResultSet
{
    // array==null means the ResultSet is closed.
    private org.scidb.client.Array array;
    private int precision;

    private boolean haveMoreChunks = false;
    private boolean haveMoreValues = false;
    private Chunk[] currentChunks = null;
    private Chunk[] nextChunks = null;
    private EmptyChunk currentEmptyBitmap;
    private EmptyChunk nextEmptyBitmap;
    private boolean wasNull = false;
    private int missingReason = 0;
    private ResultSetMetaData metadata;
    private int emptyId;
    private boolean isBeforeFirst = true;
    private boolean isFirst = false;

    //--------------------------------------------------------------------------------------------------
    // This section is internal helper routines for the (integer-argument form of the) supported types.
    //--------------------------------------------------------------------------------------------------

    /// Verifies that the cursor is valid.
    void verifyValidCursor() throws SQLException
    {
        if (array==null) {
            throw new SQLException("The ResultSet is closed.");
        }
        if (isBeforeFirst) {
            throw new SQLException("The ResultSet cursor is before first. You should call next() before reading data out of it.");
        }
    }

    /**
     * @return whether the requested column has a NULL value.
     *
     * If a columnIndex corresponds to an attribute which has a NULL value,
     *    set wasNull = true and set missingReason.
     * Else
     *    set wasNull = false.
     */
    boolean internalCheckNull(int columnIndex) throws SQLException
    {
        wasNull = false;
        int attId = columnToAttributeId(columnIndex);
        if (isAttribute(columnIndex) && currentChunks[attId].isNull()) {
            wasNull = true;
            missingReason = currentChunks[attId].missingReason();
        }
        return wasNull;
    }

    /**
     * @return the coordinate for a dimension column.
     */
    long internalGetDimension(int columnIndex) throws SQLException
    {
        if (currentEmptyBitmap != null)
            return currentEmptyBitmap.getCoordinates()[columnToDimensionId(columnIndex)];
        else
            return currentChunks[0].getCoordinates()[columnToDimensionId(columnIndex)];
    }

    /**
     * @return the value of an Int8 attribute.
     */
    Byte internalGetInt8(int columnIndex) throws SQLException
    {
        assert(isAttribute(columnIndex));
        assert(metadata.getColumnTypeEnum(columnIndex) == Type.Enum.TE_INT8);
        try
        {
            int attId = columnToAttributeId(columnIndex);
            return currentChunks[attId].getInt8();
        } catch (SciDBException e)
        {
            throw new SQLException(e.getMessage());
        }
    }

    /**
     * @return the value of an Int16 attribute.
     */
    short internalGetInt16(int columnIndex) throws SQLException
    {
        assert(isAttribute(columnIndex));
        assert(metadata.getColumnTypeEnum(columnIndex) == Type.Enum.TE_INT16);
        try
        {
            int attId = columnToAttributeId(columnIndex);
            return currentChunks[attId].getInt16();
        } catch (SciDBException e)
        {
            throw new SQLException(e.getMessage());
        }
    }

    /**
     * @return the value of an Int32 attribute.
     */
    int internalGetInt32(int columnIndex) throws SQLException
    {
        assert(isAttribute(columnIndex));
        assert(metadata.getColumnTypeEnum(columnIndex) == Type.Enum.TE_INT32);
        try
        {
            int attId = columnToAttributeId(columnIndex);
            return currentChunks[attId].getInt32();
        } catch (SciDBException e)
        {
            throw new SQLException(e.getMessage());
        }
    }

    /**
     * @return the value of an Int64 attribute.
     */
    long internalGetInt64(int columnIndex) throws SQLException
    {
        assert(isAttribute(columnIndex));
        assert(metadata.getColumnTypeEnum(columnIndex) == Type.Enum.TE_INT64);
        try
        {
            int attId = columnToAttributeId(columnIndex);
            return currentChunks[attId].getInt64();
        } catch (SciDBException e)
        {
            throw new SQLException(e.getMessage());
        }
    }

    /**
     * @return the value of an Uint8 attribute.
     */
    long internalGetUint8(int columnIndex) throws SQLException
    {
        assert(isAttribute(columnIndex));
        assert(metadata.getColumnTypeEnum(columnIndex) == Type.Enum.TE_UINT8);
        try
        {
            int attId = columnToAttributeId(columnIndex);
            return Byte.toUnsignedLong(currentChunks[attId].getInt8());
        } catch (SciDBException e)
        {
            throw new SQLException(e.getMessage());
        }
    }

    /**
     * @return the value of an Uint16 attribute.
     */
    long internalGetUint16(int columnIndex) throws SQLException
    {
        assert(isAttribute(columnIndex));
        assert(metadata.getColumnTypeEnum(columnIndex) == Type.Enum.TE_UINT16);
        try
        {
            int attId = columnToAttributeId(columnIndex);
            return Short.toUnsignedLong(currentChunks[attId].getInt16());
        } catch (SciDBException e)
        {
            throw new SQLException(e.getMessage());
        }
    }

    /**
     * @return the value of an Uint32 attribute.
     */
    long internalGetUint32(int columnIndex) throws SQLException
    {
        assert(isAttribute(columnIndex));
        assert(metadata.getColumnTypeEnum(columnIndex) == Type.Enum.TE_UINT32);
        try
        {
            int attId = columnToAttributeId(columnIndex);
            return Integer.toUnsignedLong(currentChunks[attId].getInt32());
        } catch (SciDBException e)
        {
            throw new SQLException(e.getMessage());
        }
    }

    /**
     * @return the value of an Uint64 attribute as a string.
     * @note that there is no Long.toUnsignedLong().
     */
    String internalGetUint64(int columnIndex) throws SQLException
    {
        assert(isAttribute(columnIndex));
        assert(metadata.getColumnTypeEnum(columnIndex) == Type.Enum.TE_UINT64);
        try
        {
            int attId = columnToAttributeId(columnIndex);
            return Long.toUnsignedString(currentChunks[attId].getInt64());
        } catch (SciDBException e)
        {
            throw new SQLException(e.getMessage());
        }
    }

    /**
     * @return the value of an Bool attribute.
     */
    boolean internalGetBoolean(int columnIndex) throws SQLException
    {
        assert(isAttribute(columnIndex));
        assert(metadata.getColumnTypeEnum(columnIndex) == Type.Enum.TE_BOOL);
        try
        {
            int attId = columnToAttributeId(columnIndex);
            return currentChunks[attId].getBoolean();
        } catch (SciDBException e)
        {
            throw new SQLException(e.getMessage());
        }
    }

    /**
     * @return the value of an Char attribute.
     */
    char internalGetChar(int columnIndex) throws SQLException
    {
        assert(isAttribute(columnIndex));
        assert(metadata.getColumnTypeEnum(columnIndex) == Type.Enum.TE_CHAR);
        try
        {
            int attId = columnToAttributeId(columnIndex);
            return currentChunks[attId].getChar();
        } catch (SciDBException e)
        {
            throw new SQLException(e.getMessage());
        }
    }

    /**
     * @return the value of a Float attribute.
     */
    float internalGetFloat(int columnIndex) throws SQLException
    {
        assert(isAttribute(columnIndex));
        assert(metadata.getColumnTypeEnum(columnIndex) == Type.Enum.TE_FLOAT);
        try
        {
            int attId = columnToAttributeId(columnIndex);
            return currentChunks[attId].getFloat();
        } catch (SciDBException e)
        {
            throw new SQLException(e.getMessage());
        }
    }

    /**
     * @return the value of a Double attribute.
     */
    double internalGetDouble(int columnIndex) throws SQLException
    {
        assert(isAttribute(columnIndex));
        assert(metadata.getColumnTypeEnum(columnIndex) == Type.Enum.TE_DOUBLE);
        try
        {
            int attId = columnToAttributeId(columnIndex);
            return currentChunks[attId].getDouble();
        } catch (SciDBException e)
        {
            throw new SQLException(e.getMessage());
        }
    }

    /**
     * @return the value of a String attribute.
     */
    String internalGetString(int columnIndex) throws SQLException
    {
        assert(isAttribute(columnIndex));
        assert(metadata.getColumnTypeEnum(columnIndex) == Type.Enum.TE_STRING);
        try
        {
            int attId = columnToAttributeId(columnIndex);
            String str = currentChunks[attId].getString();
            return str;
        } catch (SciDBException e)
        {
            throw new SQLException(e.getMessage());
        }
    }

    /**
     * @return number of seconds since 1970-1-1 for a Datetime attribute.
     */
    long internalGetDatetime(int columnIndex) throws SQLException
    {
        assert(isAttribute(columnIndex));
        assert(metadata.getColumnTypeEnum(columnIndex) == Type.Enum.TE_DATETIME);
        try
        {
            int attId = columnToAttributeId(columnIndex);
            return currentChunks[attId].getInt64();
        } catch (SciDBException e)
        {
            throw new SQLException(e.getMessage());
        }
    }

    /**
     * @return a pair of {number of offsetted seconds since 1970-1-1, offset} for a Datetimetz attribute.
     */
    long[] internalGetDatetimetz(int columnIndex) throws SQLException
    {
        assert(isAttribute(columnIndex));
        assert(metadata.getColumnTypeEnum(columnIndex) == Type.Enum.TE_DATETIMETZ);
        try
        {
            int attId = columnToAttributeId(columnIndex);
            return currentChunks[attId].getInt128();
        } catch (SciDBException e)
        {
            throw new SQLException(e.getMessage());
        }
    }

    //----------------------------------------------------------------------------
    // This section is for the (int-argument form of the) supported types.
    // Null handling and dimension handling are explicit.
    //----------------------------------------------------------------------------

    /**
     * @return null for a NULL value; a string representation of value otherwise.
     * @note getString() works on dimension and all builtin types.
     */
    @Override
    public String getString(int columnIndex) throws SQLException
    {
        verifyValidCursor();
        if (internalCheckNull(columnIndex)) {
            return null;
        }
        if (!isAttribute(columnIndex)) {
            return "" + internalGetDimension(columnIndex);
        }
        switch(metadata.getColumnTypeEnum(columnIndex)) {
        case TE_CHAR:
            return String.valueOf(internalGetChar(columnIndex));
        case TE_INT8:
            return "" + internalGetInt8(columnIndex);
        case TE_INT16:
            return "" + internalGetInt16(columnIndex);
        case TE_INT32:
            return "" + internalGetInt32(columnIndex);
        case TE_INT64:
            return "" + internalGetInt64(columnIndex);
        case TE_UINT8:
            return "" + internalGetUint8(columnIndex);
        case TE_UINT16:
            return "" + internalGetUint16(columnIndex);
        case TE_UINT32:
            return "" + internalGetUint32(columnIndex);
        case TE_UINT64:
            return internalGetUint64(columnIndex);
        case TE_FLOAT:
            return XsvFormatter.ValueFormatter.formatDouble(internalGetFloat(columnIndex), precision);
        case TE_DOUBLE:
            return XsvFormatter.ValueFormatter.formatDouble(internalGetDouble(columnIndex), precision);
        case TE_BOOL:
            return XsvFormatter.ValueFormatter.formatBoolean(internalGetBoolean(columnIndex));
        case TE_STRING:
            return String.valueOf(internalGetString(columnIndex));
        case TE_DATETIME:
            return XsvFormatter.ValueFormatter.formatDatetime(internalGetDatetime(columnIndex), false);  // no quote
        case TE_DATETIMETZ:
            return XsvFormatter.ValueFormatter.formatDatetimetz(internalGetDatetimetz(columnIndex), false);  // no quote

        default:
            throw new TypeException(Type.enum2Type(metadata.getColumnTypeEnum(columnIndex)), "String");
        }
    }

    /**
     * Only works for a boolean attribute.
     */
    @Override
    public boolean getBoolean(int columnIndex) throws SQLException
    {
        verifyValidCursor();
        String type = "boolean";
        if (internalCheckNull(columnIndex)) {
            return false;
        }
        if (!isAttribute(columnIndex)) {
            throw new TypeException(Type.enum2Type(metadata.getColumnTypeEnum(columnIndex)), type);
        }
        switch(metadata.getColumnTypeEnum(columnIndex)) {
        case TE_BOOL:
            return internalGetBoolean(columnIndex);
        default:
            throw new TypeException(Type.enum2Type(metadata.getColumnTypeEnum(columnIndex)), type);
        }
    }

    /**
     * Only works for an int8 attribute.
     */
    @Override
    public byte getByte(int columnIndex) throws SQLException
    {
        verifyValidCursor();
        String type = "byte";
        if (internalCheckNull(columnIndex)) {
            return 0;
        }
        if (!isAttribute(columnIndex)) {
            throw new TypeException(Type.enum2Type(metadata.getColumnTypeEnum(columnIndex)), type);
        }
        switch(metadata.getColumnTypeEnum(columnIndex)) {
        case TE_INT8:
            return internalGetInt8(columnIndex);
        default:
            throw new TypeException(Type.enum2Type(metadata.getColumnTypeEnum(columnIndex)), type);
        }
    }

    /// Works for integers that can be represented in a short: INT8, UINT8, INT16.
    @Override
    public short getShort(int columnIndex) throws SQLException
    {
        verifyValidCursor();
        String type = "short";
        if (internalCheckNull(columnIndex)) {
            return 0;
        }
        if (!isAttribute(columnIndex)) {
            throw new TypeException(Type.enum2Type(metadata.getColumnTypeEnum(columnIndex)), type);
        }
        switch(metadata.getColumnTypeEnum(columnIndex)) {
        case TE_INT8:
            return internalGetInt8(columnIndex);
        case TE_UINT8:
            return (short)internalGetUint8(columnIndex);
        case TE_INT16:
            return internalGetInt16(columnIndex);
        default:
            throw new TypeException(Type.enum2Type(metadata.getColumnTypeEnum(columnIndex)), type);
        }
    }

    /// works for integers that can be represented in an int: INT8, UINT8, INT16, UINT16, INT32.
    @Override
    public int getInt(int columnIndex) throws SQLException
    {
        verifyValidCursor();
        String type = "int";
        if (internalCheckNull(columnIndex)) {
            return 0;
        }
        if (!isAttribute(columnIndex)) {
            throw new TypeException(Type.enum2Type(metadata.getColumnTypeEnum(columnIndex)), type);
        }
        switch(metadata.getColumnTypeEnum(columnIndex)) {
        case TE_INT8:
            return internalGetInt8(columnIndex);
        case TE_UINT8:
            return (short)internalGetUint8(columnIndex);
        case TE_INT16:
            return internalGetInt16(columnIndex);
        case TE_UINT16:
            return (int)internalGetUint16(columnIndex);
        case TE_INT32:
            return internalGetInt32(columnIndex);
        default:
            throw new TypeException(Type.enum2Type(metadata.getColumnTypeEnum(columnIndex)), type);
        }
    }

    /// works for all integer types except UINT64.
    /// Also works for dimension.
    @Override
    public long getLong(int columnIndex) throws SQLException
    {
        verifyValidCursor();
        String type = "long";
        if (internalCheckNull(columnIndex)) {
            return 0;
        }
        if (!isAttribute(columnIndex)) {
            return internalGetDimension(columnIndex);
        }
        switch(metadata.getColumnTypeEnum(columnIndex)) {
        case TE_INT8:
            return internalGetInt8(columnIndex);
        case TE_UINT8:
            return (short)internalGetUint8(columnIndex);
        case TE_INT16:
            return internalGetInt16(columnIndex);
        case TE_UINT16:
            return (int)internalGetUint16(columnIndex);
        case TE_INT32:
            return internalGetInt32(columnIndex);
        case TE_UINT32:
            return internalGetUint32(columnIndex);
        case TE_INT64:
            return internalGetInt64(columnIndex);
        default:
            throw new TypeException(Type.enum2Type(metadata.getColumnTypeEnum(columnIndex)), type);
        }
    }

    /// Works for float attribute plus integers up to 32 bits.
    @Override
    public float getFloat(int columnIndex) throws SQLException
    {
        verifyValidCursor();
        String type = "float";
        if (internalCheckNull(columnIndex)) {
            return 0;
        }
        if (!isAttribute(columnIndex)) {
            throw new TypeException(Type.enum2Type(metadata.getColumnTypeEnum(columnIndex)), type);
        }
        switch(metadata.getColumnTypeEnum(columnIndex)) {
        case TE_INT8:
            return internalGetInt8(columnIndex);
        case TE_UINT8:
            return (float)internalGetUint8(columnIndex);
        case TE_INT16:
            return internalGetInt16(columnIndex);
        case TE_UINT16:
            return (float)internalGetUint16(columnIndex);
        case TE_INT32:
            return internalGetInt32(columnIndex);
        case TE_UINT32:
            return (float)internalGetUint32(columnIndex);
        case TE_FLOAT:
            return internalGetFloat(columnIndex);
        default:
            throw new TypeException(Type.enum2Type(metadata.getColumnTypeEnum(columnIndex)), type);
        }
    }

    /// Works for dimension and all numeric attributes.
    @Override
    public double getDouble(int columnIndex) throws SQLException
    {
        verifyValidCursor();
        String type = "double";
        if (internalCheckNull(columnIndex)) {
            return 0;
        }
        if (!isAttribute(columnIndex)) {
            return internalGetDimension(columnIndex);
        }
        switch(metadata.getColumnTypeEnum(columnIndex)) {
        case TE_DOUBLE:
            return internalGetDouble(columnIndex);
        case TE_FLOAT:
            return internalGetFloat(columnIndex);
        case TE_INT8:
            return internalGetInt8(columnIndex);
        case TE_INT16:
            return internalGetInt16(columnIndex);
        case TE_INT32:
            return internalGetInt32(columnIndex);
        case TE_INT64:
            return internalGetInt64(columnIndex);
        case TE_UINT8:
            return internalGetUint8(columnIndex);
        case TE_UINT16:
            return internalGetUint16(columnIndex);
        case TE_UINT32:
            return internalGetUint32(columnIndex);
        case TE_UINT64:
            return (new Double(internalGetUint64(columnIndex))).doubleValue();

        default:
            throw new TypeException(Type.enum2Type(metadata.getColumnTypeEnum(columnIndex)), type);
        }
    }

    /// Works for dimension and all integer types.
    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException
    {
        verifyValidCursor();
        String type = "BigDecimal";
        if (internalCheckNull(columnIndex)) {
            return null;
        }
        if (!isAttribute(columnIndex)) {
            return new BigDecimal(internalGetDimension(columnIndex));
        }
        switch(metadata.getColumnTypeEnum(columnIndex)) {
        case TE_INT8:
            return new BigDecimal(internalGetInt8(columnIndex));
        case TE_INT16:
            return new BigDecimal(internalGetInt16(columnIndex));
        case TE_INT32:
            return new BigDecimal(internalGetInt32(columnIndex));
        case TE_INT64:
            return new BigDecimal(internalGetInt64(columnIndex));
        case TE_UINT8:
            return new BigDecimal(internalGetUint8(columnIndex));
        case TE_UINT16:
            return new BigDecimal(internalGetUint16(columnIndex));
        case TE_UINT32:
            return new BigDecimal(internalGetUint32(columnIndex));
        case TE_UINT64:
            return new BigDecimal(internalGetUint64(columnIndex));

        default:
            throw new TypeException(Type.enum2Type(metadata.getColumnTypeEnum(columnIndex)), type);
        }
    }

    @Deprecated
    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException
    {
        verifyValidCursor();
        BigDecimal dec = getBigDecimal(columnIndex);
        if (dec==null) {
            return null;
        }
        return dec.setScale(scale);
    }

    /// @return number of seconds since 1970-1-1.
    public long getDatetime(int columnIndex) throws SQLException
    {
        verifyValidCursor();
        String type = "datetime";
        if (internalCheckNull(columnIndex)) {
            return 0;
        }
        if (!isAttribute(columnIndex)) {
            throw new TypeException(Type.enum2Type(metadata.getColumnTypeEnum(columnIndex)), type);
        }
        switch(metadata.getColumnTypeEnum(columnIndex)) {
        case TE_DATETIME:
            return internalGetDatetime(columnIndex);
        default:
            throw new TypeException(Type.enum2Type(metadata.getColumnTypeEnum(columnIndex)), type);
        }
    }

    /// @return the first integer is the number of seconds since 1970-1-1 (offsetted), and the second integer is the offseted seconds.
    public long[] getDatetimetz(int columnIndex) throws SQLException
    {
        verifyValidCursor();
        String type = "datetimetz";
        if (internalCheckNull(columnIndex)) {
            return null;
        }
        if (!isAttribute(columnIndex)) {
            throw new TypeException(Type.enum2Type(metadata.getColumnTypeEnum(columnIndex)), type);
        }
        switch(metadata.getColumnTypeEnum(columnIndex)) {
        case TE_DATETIMETZ:
            return internalGetDatetimetz(columnIndex);
        default:
            throw new TypeException(Type.enum2Type(metadata.getColumnTypeEnum(columnIndex)), type);
        }
    }

    //----------------------------------------------------------------------------
    // This section is for the (string-argument form of the) supported types.
    //----------------------------------------------------------------------------

    @Override
    public String getString(String columnLabel) throws SQLException
    {
        return getString(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException
    {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException
    {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException
    {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException
    {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException
    {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException
    {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException
    {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException
    {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Deprecated
    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException
    {
        return getBigDecimal(findColumn(columnLabel), scale);
    }

    public long getDatetime(String columnLabel) throws SQLException
    {
        return getDatetime(findColumn(columnLabel));
    }

    public long[] getDatetimetz(String columnLabel) throws SQLException
    {
        return getDatetimetz(findColumn(columnLabel));
    }

    //----------------------------------------------------------------------------
    // This section is for the other supported methods.
    //----------------------------------------------------------------------------

    public ResultSet(org.scidb.client.Array array) throws SQLException
    {
        this(array, 6);
    }

    /**
     * @param array  the client array from which data are read.
     * @param precision  the precision to use when getString() is called on floating-point types.
     */
    public ResultSet(org.scidb.client.Array array, int precision) throws SQLException
    {
        this.array = array;
        this.precision = precision;
        if (array==null) {
            throw new SQLException("Trying to create a ResultSet on a null array.");
        }

        metadata = new ResultSetMetaData(this.array.getSchema());

        Schema.Attribute emptyAttribute = this.array.getSchema().getEmptyIndicator();
        emptyId = emptyAttribute != null ? this.array.getSchema().getEmptyIndicator().getId() : -1;
    }

    public boolean isAttribute(int columnIndex) throws SQLException
    {
        if (columnIndex > 0 && columnIndex < array.getSchema().getDimensions().length + 1) {
            return false;
        }
        else if (columnIndex >= array.getSchema().getDimensions().length + 1 && columnIndex <= metadata.getColumnCount()) {
            return true;
        }
        else {
            throw new SQLException("Wrong column index " + columnIndex);
        }
    }

    private int columnToAttributeId(int columnIndex) throws SQLException
    {
        return columnIndex - array.getSchema().getDimensions().length - 1;
    }

    private int columnToDimensionId(int columnIndex) throws SQLException
    {
        return columnIndex - 1;
    }

    @Override
    @SuppressWarnings(value = "unchecked") //While we checking types inside we can safely ignore warnings
    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        if (iface == IResultSetWrapper.class)
        {
            return (T) new ResultSetWrapper(this);
        }
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        return iface == IResultSetWrapper.class;
    }

    // If isBeforeFirst
    //   Set isBeforeFirst = false.
    //   Fetch chunk.
    //   If there is any record
    //     Set isFirst = true
    //     return true
    //   Else
    //     Set isFirst = false
    //     return false
    // Else
    //   Set isFirst = false
    //   Advance to the next record.
    //   return whether there is any record.
    @Override
    public boolean next() throws SQLException
    {
        if (isBeforeFirst) {
            isBeforeFirst = false;
            try {
                int attrCount = array.getSchema().getAttributes().length;
                currentChunks = new Chunk[attrCount];
                nextChunks = new Chunk[attrCount];

                //Fetch first chunks set in result and store
                array.fetch();
                for (int i = 0; i < attrCount; i++)
                {
                    if (i != emptyId)
                        currentChunks[i] = array.getChunk(i);
                }
                currentEmptyBitmap = array.getEmptyBitmap();

                //Fetch second chunks set if possible to know if we have more ahead
                if(!currentChunks[0].endOfArray())
                {
                    haveMoreValues = currentChunks[0].hasNext();
                    array.fetch();
                    for (int i = 0; i < attrCount; i++)
                    {
                        if (i != emptyId)
                            nextChunks[i] = array.getChunk(i);
                    }
                    haveMoreChunks = !nextChunks[0].endOfArray();
                    if (haveMoreChunks) {
                        nextEmptyBitmap = array.getEmptyBitmap();
                    }
                }
                else
                {
                    currentChunks = null;
                    nextChunks = null;
                }
            }
            catch (SciDBException e)
            {
                throw new SQLException(e.getMessage());
            } catch (java.io.IOException e) {
                throw new SQLException(e.getMessage());
            }

            isFirst = (currentChunks != null);
            return isFirst;
        }

        isFirst = false;
        if (currentChunks == null)
            return false;
        try
        {
            if (haveMoreValues)
            {
                for (int i = 0; i < array.getSchema().getAttributes().length; i++)
                {
                    if (i != emptyId)
                        currentChunks[i].move();
                }
                if (currentEmptyBitmap != null)
                    currentEmptyBitmap.move();
                haveMoreValues = currentChunks[0].hasNext();
            }
            //If we don't have anymore values in this chunk we check if we have more chunks with data
            //ahead and if yes move to next chunk and refresh flags
            else if(haveMoreChunks)
            {
                array.fetch();
                for (int i = 0; i < array.getSchema().getAttributes().length; i++)
                {
                    currentChunks[i] = nextChunks[i];
                    if (i != emptyId)
                        nextChunks[i] = array.getChunk(i);
                }
                haveMoreChunks = !nextChunks[0].endOfArray();
                haveMoreValues = currentChunks[0].hasNext();
                currentEmptyBitmap = nextEmptyBitmap;
                nextEmptyBitmap = array.getEmptyBitmap();
            }
            //Finally if we don't have chunks ahead clear chunks lists. Now we in "after last state"
            else
            {
                haveMoreChunks = false;
                haveMoreValues = false;
                currentChunks = null;
                nextChunks = null;
                return false;
            }
        } catch (SciDBException e)
        {
            throw new SQLException(e.getMessage());
        } catch (IOException e) {
            throw new SQLException(e.getMessage());
        }
        return true;
    }

    @Override
    public void close() throws SQLException
    {
        try {
            if (array!=null) {
                Message msg = new Message.CompleteQuery(array.getQueryId());
                array.getNetwork().write(msg);
                array.getNetwork().read();
                array = null;
            }
        }
        catch (java.io.IOException e) {
            throw new SQLException(e.getMessage());
        }
        catch (SciDBException e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public boolean isClosed() throws SQLException
    {
        return array==null;
    }

    @Override
    public boolean wasNull() throws SQLException
    {
        return wasNull;
    }

    public int getMissingReason()
    {
        return missingReason;
    }

    @Override
    public java.sql.ResultSetMetaData getMetaData() throws SQLException
    {
        return metadata;
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException
    {
        for (int i = 1; i <= metadata.getColumnCount(); i++)
        {
            if (columnLabel.equals(metadata.getColumnName(i)))
                return i;
        }
        return 0;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet::isBeforeFirst() not supported.");
    }

    @Override
    public boolean isAfterLast() throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet::isAfterLast() not supported.");
    }

    @Override
    public boolean isFirst() throws SQLException
    {
        return isFirst;
    }

    @Override
    public boolean isLast() throws SQLException
    {
        return currentChunks != null && !haveMoreChunks && !haveMoreValues;
    }

    //----------------------------------------------------------------------------
    // This section is for the (integer-argument form of the) NON-supported types.
    //----------------------------------------------------------------------------

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet::getBytes() not supported.");
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.getDate() not supported.");
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.getTime() not supported.");
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.getTimestamp() not supported.");
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.getAsciiStream() not supported.");
    }

    @Deprecated
    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.getUnicodetream() not supported.");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.getBinaryStream() not supported.");
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.getCharacterStream() not supported.");
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.getObject() not supported.");
    }

    @Override
    public String getNString(int columnIndex) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.getNString() not supported.");
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.getNCharacterStream() not supported.");
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("<T> ResultSet.getObject() is not supported.");
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.getNClob() is not supported.");
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.getSQLXML() is not supported.");
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.getObject() is not supported.");
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.getRef() is not supported.");
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.getBlob() is not supported.");
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.getClob() is not supported.");
    }

    @Override
    public java.sql.Array getArray(int columnIndex) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.getArray() is not supported.");
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.getDate() is not supported.");
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.getTime() is not supported.");
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.getTimestamp() is not supported.");
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.getURL() is not supported.");
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.getRowId() is not supported.");
    }

    //----------------------------------------------------------------------------
    // This section is for the (string-argument form of the) NON-supported types.
    //----------------------------------------------------------------------------

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException
    {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException
    {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException
    {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException
    {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException
    {
        return getAsciiStream(findColumn(columnLabel));
    }

    @Deprecated
    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException
    {
        //noinspection deprecation
        return getUnicodeStream(findColumn(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException
    {
        return getBinaryStream(findColumn(columnLabel));
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException
    {
        return getCharacterStream(findColumn(columnLabel));
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException
    {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public String getNString(String columnLabel) throws SQLException
    {
        return getNString(findColumn(columnLabel));
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException
    {
        return getNCharacterStream(findColumn(columnLabel));
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException
    {
        return getObject(findColumn(columnLabel), type);
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException
    {
        return getNClob(findColumn(columnLabel));
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException
    {
        return getSQLXML(findColumn(columnLabel));
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException
    {
        return getObject(findColumn(columnLabel), map);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException
    {
        return getRef(findColumn(columnLabel));
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException
    {
        return getBlob(findColumn(columnLabel));
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException
    {
        return getClob(findColumn(columnLabel));
    }

    @Override
    public java.sql.Array getArray(String columnLabel) throws SQLException
    {
        return getArray(findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException
    {
        return getDate(findColumn(columnLabel), cal);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException
    {
        return getTime(findColumn(columnLabel), cal);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException
    {
        return getTimestamp(findColumn(columnLabel), cal);
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException
    {
        return getURL(findColumn(columnLabel));
    }
    @Override
    public RowId getRowId(String columnLabel) throws SQLException
    {
        return getRowId(findColumn(columnLabel));
    }

    //----------------------------------------------------------------------------
    // This section is for the other NON-supported methods.
    //----------------------------------------------------------------------------

    @Override
    public SQLWarning getWarnings() throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.getWarnings() not supported.");
    }

    @Override
    public void clearWarnings() throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.clearWarnings() not supported.");
    }

    @Override
    public String getCursorName() throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.getCursorName() not supported.");
    }

    @Override
    public void beforeFirst() throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.beforeFirst() not supported.");
    }

    @Override
    public void afterLast() throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.afterLast() not supported.");
    }

    @Override
    public boolean first() throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.first() not supported.");
    }

    @Override
    public boolean last() throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.last() not supported.");
    }

    @Override
    public int getRow() throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.getRow() not supported.");
    }

    @Override
    public boolean absolute(int row) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.absolute() not supported.");
    }

    @Override
    public boolean relative(int rows) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.relative() not supported.");
    }

    @Override
    public boolean previous() throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.previous() not supported.");
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.setFetchDirection() not supported.");
    }

    @Override
    public int getFetchDirection() throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.getFetchDirection() not supported.");
    }

    @Override
    public void setFetchSize(int rows) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.setFetchSize() not supported.");
    }

    @Override
    public int getFetchSize() throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.getFetchSize() not supported.");
    }

    @Override
    public int getType() throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.getType() not supported.");
    }

    @Override
    public int getConcurrency() throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.getConcurrency() not supported.");
    }

    @Override
    public boolean rowUpdated() throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.rowUpdated() not supported.");
    }

    @Override
    public boolean rowInserted() throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.rowInserted() not supported.");
    }

    @Override
    public boolean rowDeleted() throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.rowDeleted() not supported.");
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateNull() not supported.");
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateBoolean() not supported.");
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateByte() not supported.");
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateShort() not supported.");
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateInt() not supported.");
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateLong() not supported.");
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateFloat() not supported.");
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateDouble() not supported.");
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateBigDecimal() not supported.");
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateString() not supported.");
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateBytes() not supported.");
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateDate() not supported.");
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateTime() not supported.");
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateTimeStamp() not supported.");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateAsciiStream() not supported.");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateBinaryStream() not supported.");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateCharacterStream() not supported.");
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateObject() not supported.");
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateObject() not supported.");
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateNull() not supported.");
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateBoolean() not supported.");
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateByte() not supported.");
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateShort() not supported.");
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateInt() not supported.");
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateLong() not supported.");
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateFloat() not supported.");
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateDouble() not supported.");
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateBigDecimal() not supported.");
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateString() not supported.");
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateBytes() not supported.");
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateDate() not supported.");
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateTime() not supported.");
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateTimestamp() not supported.");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateAsciiStream() not supported.");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateBinaryStream() not supported.");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateCharacterStream() not supported.");
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateObject() not supported.");
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateObject() not supported.");
    }

    @Override
    public void insertRow() throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.insertRow() not supported.");
    }

    @Override
    public void updateRow() throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateRow() not supported.");
    }

    @Override
    public void deleteRow() throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.deleteRow() not supported.");
    }

    @Override
    public void refreshRow() throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.refreshRow() not supported.");
    }

    @Override
    public void cancelRowUpdates() throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.cancelRowUpdates() not supported.");
    }

    @Override
    public void moveToInsertRow() throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.moveToInsertRow() not supported.");
    }

    @Override
    public void moveToCurrentRow() throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.moveToCurrentRow() not supported.");
    }

    @Override
    public Statement getStatement() throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.getStatement() not supported.");
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateRef() not supported.");
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateRef() not supported.");
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateBlob() not supported.");
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateBlob() not supported.");
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateClob() not supported.");
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateClob() not supported.");
    }

    @Override
    public void updateArray(int columnIndex, java.sql.Array x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateArray() not supported.");
    }

    @Override
    public void updateArray(String columnLabel, java.sql.Array x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateArray() not supported.");
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateRowId() not supported.");
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateRowId() not supported.");
    }

    @Override
    public int getHoldability() throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.getHoldability() not supported.");
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateNString() not supported.");
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateNString() not supported.");
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateNClob() not supported.");
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateNClob() not supported.");
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateSQLXML() not supported.");
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateSQLXML() not supported.");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateNCharacterStream() not supported.");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateNCharacterStream() not supported.");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateAsciiStream() not supported.");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateBinaryStream() not supported.");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateCharacterStream() not supported.");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateAsciiStream() not supported.");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateBinaryStream() not supported.");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateCharacterStream() not supported.");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateBlob() not supported.");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateBlob() not supported.");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateClob() not supported.");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateClob() not supported.");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateNClob() not supported.");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateNClob() not supported.");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateNCharacterStream() not supported.");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateNCharacterStream() not supported.");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateAsciiStream() not supported.");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateBinaryStream() not supported.");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateCharacterStream() not supported.");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateAsciiStream() not supported.");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateBinaryStream() not supported.");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateCharacterStream() not supported.");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateBlob() not supported.");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateBlob() not supported.");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateClob() not supported.");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateClob() not supported.");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateNClob() is not supported.");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet.updateNClob() is not supported.");
    }
}
