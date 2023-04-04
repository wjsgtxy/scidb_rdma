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

import java.sql.SQLException;

import org.scidb.client.Schema;
import org.scidb.client.Schema.Attribute;
import org.scidb.client.Schema.Dimension;
import org.scidb.client.Type;

public class ResultSetMetaData implements java.sql.ResultSetMetaData
{
    private Schema schema;
    private String[] columnsNames;
    private Type.Enum[] columnsTypeEnums;
    private int columnsCount;

    public ResultSetMetaData(Schema schema)
    {
        this.schema = schema;
        columnsCount = schema.getDimensions().length + schema.getAttributes().length - (schema.getEmptyIndicator() != null ? 1 : 0);
        columnsNames = new String[columnsCount];
        columnsTypeEnums = new Type.Enum[columnsCount];

        int i = 0;
        for (Dimension dim: schema.getDimensions())
        {
            columnsNames[i] = dim.getName();
            columnsTypeEnums[i] = Type.Enum.TE_INT64;
            i++;
        }

        for (Attribute att: schema.getAttributes())
        {
            if (schema.getEmptyIndicator() != null && att.getId() == schema.getEmptyIndicator().getId())
                continue;
            columnsNames[i] = att.getName();
            columnsTypeEnums[i] = att.getTypeEnum();
            i++;
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int getColumnCount() throws SQLException
    {
        return columnsCount;
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean isSigned(int column) throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException
    {
        return columnsNames[column - 1];
    }

    @Override
    public String getColumnName(int column) throws SQLException
    {
        return columnsNames[column - 1];
    }

    @Override
    public String getSchemaName(int column) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getPrecision(int column) throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException
    {
        return schema.getName();
    }

    @Override
    public String getCatalogName(int column) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getColumnType(int column) throws SQLException
    {
        switch(getColumnTypeEnum(column)) {
        case TE_CHAR:
            return java.sql.Types.CHAR;
        case TE_INT8:
            return java.sql.Types.TINYINT;
        case TE_INT16:
            return java.sql.Types.SMALLINT;
        case TE_INT32:
            return java.sql.Types.INTEGER;
        case TE_INT64:
            return java.sql.Types.BIGINT;
        case TE_FLOAT:
            return java.sql.Types.FLOAT;
        case TE_DOUBLE:
            return java.sql.Types.DOUBLE;
        case TE_BOOL:
            return java.sql.Types.BOOLEAN;
        case TE_STRING:
            return java.sql.Types.VARCHAR;

        // Unrecognized
        case TE_UINT8:
        case TE_UINT16:
        case TE_UINT32:
        case TE_UINT64:
        case TE_DATETIME:
        case TE_DATETIMETZ:
        default:
            return 0;
        }
    }

    public Type.Enum getColumnTypeEnum(int column) throws SQLException
    {
        return columnsTypeEnums[column - 1];
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException
    {
        return Type.enum2Type(columnsTypeEnums[column - 1]);
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException
    {
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException
    {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException
    {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException
    {
        return null;
    }

}
