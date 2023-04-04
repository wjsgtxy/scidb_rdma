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
 * @file CryptoUtils.cpp
 */

#include <util/CryptoUtils.h>

#include <system/Exceptions.h>
#include <system/Utils.h>

#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <openssl/evp.h>
#include <openssl/sha.h>

#include <vector>

using namespace std;

namespace scidb { namespace crut {

/**
 * @brief Use SHA512 to hash the input and then convert to Base64.
 * @param password - string to be converted
 */
string sha512_b64(string const & password)
{
    SHA512_CTX              ctx;
    vector<uint8_t>    digest(SHA512_DIGEST_LENGTH);

    SHA512_Init(&ctx);
    SHA512_Update(&ctx, password.c_str(), password.length());
    SHA512_Final(digest.data(), &ctx);

    SCIDB_ASSERT(digest.size() < std::numeric_limits<int>::max());

    return b64_encode(digest.data(), digest.size());
}


string b64_encode(string const& src)
{
    if (src.empty()) {
        return "";
    }
    return b64_encode(src.c_str(), src.size());
}


string b64_encode(const void* buffer, size_t len)
{
    ASSERT_EXCEPTION(buffer, "b64_encode: !buffer");
    ASSERT_EXCEPTION(len, "b64_encode: len == 0");
    ASSERT_EXCEPTION(len <= std::numeric_limits<int>::max(),
                     "Length too long for OpenSSL EVP functions");

    // The b64data-to-data ratio is 3 to 4. Integer divide by three
    // then multiply by 4, then add 1 for trailing NUL.
    int intlen = static_cast<int>(len);
    int b64len = (((intlen + 2) / 3) * 4) + 1;

    vector<unsigned char> result(b64len);
    int sz = EVP_EncodeBlock(&result[0],
                             reinterpret_cast<const unsigned char*>(buffer),
                             intlen);
    SCIDB_ASSERT(sz < b64len);
    SCIDB_ASSERT(result[sz] == '\0');
    return string(reinterpret_cast<const char*>(&result[0]));
}


string b64_decode(string const& src)
{
    if (src.empty()) {
        return "";
    }
    vector<char> buf(src.size());
    b64_decode(src, &buf[0], static_cast<int>(src.size()));
    return string(&buf[0]);
}


void b64_decode(string const& src, void *buffer, size_t len)
{
    ASSERT_EXCEPTION(buffer, "b64_decode: !buffer");
    ASSERT_EXCEPTION(len >= src.size(), "b64_decode: len < src.size()");
    ASSERT_EXCEPTION(len <= std::numeric_limits<int>::max(),
                     "Output length too long for OpenSSL BIO functions");
    ASSERT_EXCEPTION(src.size() <= std::numeric_limits<int>::max(),
                     "Input length too long for OpenSSL BIO functions");

    int rc = EVP_DecodeBlock(
        reinterpret_cast<unsigned char*>(buffer),
        reinterpret_cast<const unsigned char*>(src.c_str()),
        static_cast<int>(src.size()));
    ASSERT_EXCEPTION(rc >= 0, "b64_decode: Error from EVP_DecodeBlock");
}

} } // namespaces
