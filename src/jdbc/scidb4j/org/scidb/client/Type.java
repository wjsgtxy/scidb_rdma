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

/**
 * Class describing all SciDB built-in types
 */
public class Type
{
    public static final String TID_INDICATOR = "indicator"; // for the bitmap.
    public static final String TID_CHAR = "char";
    public static final String TID_INT8 = "int8";
    public static final String TID_INT16 = "int16";
    public static final String TID_INT32 = "int32";
    public static final String TID_INT64 = "int64";
    public static final String TID_UINT8 = "uint8";
    public static final String TID_UINT16 = "uint16";
    public static final String TID_UINT32 = "uint32";
    public static final String TID_UINT64 = "uint64";
    public static final String TID_FLOAT = "float";
    public static final String TID_DOUBLE = "double";
    public static final String TID_BOOL = "bool";
    public static final String TID_STRING = "string";
    public static final String TID_DATETIME = "datetime";
    public static final String TID_DATETIMETZ = "datetimetz";

    public enum Enum
    {
        TE_INDICATOR,
        TE_CHAR,
        TE_INT8,
        TE_INT16,
        TE_INT32,
        TE_INT64,
        TE_UINT8,
        TE_UINT16,
        TE_UINT32,
        TE_UINT64,
        TE_FLOAT,
        TE_DOUBLE,
        TE_BOOL,
        TE_STRING,
        TE_DATETIME,
        TE_DATETIMETZ
    };

    public static final String[] types =
    {
        TID_INDICATOR,
        TID_CHAR,
        TID_INT8,
        TID_INT16,
        TID_INT32,
        TID_INT64,
        TID_UINT8,
        TID_UINT16,
        TID_UINT32,
        TID_UINT64,
        TID_FLOAT,
        TID_DOUBLE,
        TID_BOOL,
        TID_STRING,
        TID_DATETIME,
        TID_DATETIMETZ
    };

    public static Enum type2Enum(String type)
    throws SciDBException
    {
        if (type.equals(TID_INDICATOR)) {
            return Enum.TE_INDICATOR;
        }
        else if (type.equals(TID_CHAR)) {
            return Enum.TE_CHAR;
        }
        else if (type.equals(TID_INT8)) {
            return Enum.TE_INT8;
        }
        else if (type.equals(TID_INT16)) {
            return Enum.TE_INT16;
        }
        else if (type.equals(TID_INT32)) {
            return Enum.TE_INT32;
        }
        else if (type.equals(TID_INT64)) {
            return Enum.TE_INT64;
        }
        else if (type.equals(TID_UINT8)) {
            return Enum.TE_UINT8;
        }
        else if (type.equals(TID_UINT16)) {
            return Enum.TE_UINT16;
        }
        else if (type.equals(TID_UINT32)) {
            return Enum.TE_UINT32;
        }
        else if (type.equals(TID_UINT64)) {
            return Enum.TE_UINT64;
        }
        else if (type.equals(TID_FLOAT)) {
            return Enum.TE_FLOAT;
        }
        else if (type.equals(TID_DOUBLE)) {
            return Enum.TE_DOUBLE;
        }
        else if (type.equals(TID_BOOL)) {
            return Enum.TE_BOOL;
        }
        else if (type.equals(TID_STRING)) {
            return Enum.TE_STRING;
        }
        else if (type.equals(TID_DATETIME)) {
            return Enum.TE_DATETIME;
        }
        else if (type.equals(TID_DATETIMETZ)) {
            return Enum.TE_DATETIMETZ;
        }
        else {
            throw new SciDBException("Type " + type + " is not supported.");
        }
    }

    public static String enum2Type(Enum e)
    {
        return types[e.ordinal()];
    }
}
