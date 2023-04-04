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
#ifndef CCM_SERVICE_H_
#define CCM_SERVICE_H_

// c++ standard libraries
#include <memory>

namespace scidb { namespace ccm {

/**
 * @brief The service started by SciDB to act as the Client Communication manager capable
 * or handling messages (defined in ccm/client.proto)
 */
class CcmService
{
  public:
    CcmService();
    ~CcmService() noexcept;

    void start();
    void stop();

  private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

}}  // namespace scidb::ccm

#endif
