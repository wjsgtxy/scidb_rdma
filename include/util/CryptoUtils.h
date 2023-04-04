/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2017-2019 SciDB, Inc.
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
 * @file CryptoUtils.h
 */

#ifndef CRYPTO_UTILS_H
#define CRYPTO_UTILS_H

#include <string>

namespace scidb { namespace crut {

    /**
     * @brief Encode len bytes of data at p as base64.
     * @param p buffer pointer
     * @param len non-zero buffer length, must be < INT_MAX
     * @throws ASSERT_EXCEPTION if input parameters look bad
     */
    std::string b64_encode(const void *p, size_t len);

    /**
     * @brief Encode src string as base64.
     * @note Empty input string yields empty output string.
     */
    std::string b64_encode(std::string const& src);

    /**
     * @brief Decode base64 string into provided buffer.
     * @param src source string
     * @param p buffer pointer
     * @param len buffer length
     * @throws ASSERT_EXCEPTION if input args look bad
     */
    void b64_decode(std::string const& src, void *p, size_t len);

    /**
     * @brief Decode base64 string into string return value.
     * @note Brashly assumes that the base64 data decodes to a string
     *       with no embedded NUL bytes.
     */
    std::string b64_decode(std::string const& src);

    /** Return base64-encoded SHA-512 hash of string 's'. */
    std::string sha512_b64(std::string const& s);

} }

#endif /* ! CRYPTO_UTILS_H */
