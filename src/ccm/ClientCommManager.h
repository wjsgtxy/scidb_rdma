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
#ifndef CLIENT_COMM_MANAGER_H_
#define CLIENT_COMM_MANAGER_H_

// c++ standard libraries
#include <memory>

namespace scidb { namespace ccm {

class CcmProperties;

/**
 * @defgroup Ccm Client Communications Manager
 */

/**
 * @class ClientCommManager
 *
 * @ingroup Ccm
 *
 * A tool that listens for incoming client messages and processes them to execute queries on SciDB.
 */
class ClientCommManager
{
  public:
    explicit ClientCommManager(CcmProperties const&);
    ~ClientCommManager() noexcept;

    /**
     * Block waiting for external client requests, and service them when they arrive.
     */
    void run();

    /**
     * Stop the Client Communications Manager.
     *
     * @note The @c run method is called by a @c Job so this @c stop() method must be from
     * a separate thread.
     */
    void stop();

  private:
    class Impl;
    std::unique_ptr<Impl> _impl;
};
}}  // namespace scidb::ccm

#endif
