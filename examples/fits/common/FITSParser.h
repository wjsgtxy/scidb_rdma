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
#ifndef FITS_PARSER_H
#define FITS_PARSER_H

#include <fstream>
#include <vector>

namespace scidb
{

class FITSParser
{
public:
    enum BitPixType {
        INT16,
        INT16_SCALED,
        INT32,
        INT32_SCALED,
        FLOAT32_SCALED
    };

    FITSParser(std::string const& filePath);
    ~FITSParser();

    int                 getNumberOfHDUs();

    bool                moveToHDU(uint32_t hdu, std::string& error);
    int                 getBitPix() const;
    int                 getBitPixType() const;
    const std::vector<int>&  getAxisSizes() const;
    float               getBZero() const;
    float               getBScale() const;

    void                moveToCell(int64_t cell);
    short int           readInt16();
    int                 readInt32();
    float               readFloat32();

private:
    bool                validateHDU(uint32_t hdu, std::string& error);

    std::string              readKeyword();
    void                readAndIgnoreValue();
    bool                hasKey(std::string const& key);

    bool                readFixedLogicalKeyword(std::string const& key);
    int                 readFixedIntegerKeyword(std::string const& key);
    void                readFreeStringKeyword(std::string const& key, std::string &value, bool &undefined);
    float               readFreeFloatingValue();
    int                 readFreeIntegerValue();

    static const int    kBlockSize = 2880;

    std::string              filePath;
    uint32_t            hdu;

    char                buffer[kBlockSize];
    std::ifstream       file;
    std::filebuf        *pbuffer;
    size_t              bufferPos;              // Current position in buffer
    std::streamoff      dataPos;                // Position in file where the data part of the HDU begins

    int                 bitpix;
    int                 bitpixsize;             // bitpix converted to bytes
    BitPixType          bitpixtype;
    int                 naxis;
    std::vector<int>         axissize;
    bool                scale;                  // Set to true only if bscale/bzero are present
    float               bscale;
    float               bzero;
    int                 pcount;
    int                 gcount;
    std::string              xtension;
};

}

#endif
