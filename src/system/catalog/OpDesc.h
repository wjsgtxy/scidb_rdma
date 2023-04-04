#ifndef OPDESC_H_
#define OPDESC_H_
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
#include <cstdint>
#include <string>

namespace scidb {
class LogicalOpDesc;
class PhysicalOpDesc;
typedef std::vector<LogicalOpDesc> LogicalOps;

typedef std::vector<PhysicalOpDesc> PhysicalOps;

typedef uint64_t OpID;


/**
 * Descriptor of pluggable logical operator
 */
class LogicalOpDesc
{
public:
    /**
     * Default constructor
     */
    LogicalOpDesc()
    {}

    /**
     * Construct descriptor for adding to catalog
     *
     * @param name Operator name
     * @param module Operator module
     * @param entry Operator entry in module
     */
    LogicalOpDesc(const std::string& name,
                  const std::string& module,
                  const std::string& entry) :
        _name(name),
        _module(module),
        _entry(entry)
    {}

    /**
     * Construct full descriptor
     *
     * @param logicalOpId Logical operator identifier
     * @param name Operator name
     * @param module Operator module
     * @param entry Operator entry in module
     */
    LogicalOpDesc(OpID logicalOpId, const std::string& name, const std::string& module,
                    const std::string& entry) :
        _logicalOpId(logicalOpId),
        _name(name),
        _module(module),
        _entry(entry)
    {}

    /**
     * Get logical operator identifier
     *
     * @return Operator identifier
     */
    OpID getLogicalOpId() const
    {
        return _logicalOpId;
    }

    /**
     * Get logical operator name
     *
     * @return Operator name
     */
    const std::string& getName() const
    {
        return _name;
    }

    /**
     * Get logical operator module
     *
     * @return Operator module
     */
    const std::string& getModule() const
    {
        return _module;
    }

    /**
     * Get logical operator entry in module
     *
     * @return Operator entry
     */
    const std::string& getEntry() const
    {
        return _entry;
    }

private:
    OpID        _logicalOpId;
    std::string _name;
    std::string _module;
    std::string _entry;
};

class PhysicalOpDesc
{
public:
    /**
     * Default constructor
     */
    PhysicalOpDesc()
    {}

    PhysicalOpDesc(const std::string& logicalOpName, const std::string& name,
                const std::string& module, const std::string& entry) :
        _logicalOpName(logicalOpName),
        _name(name),
        _module(module),
        _entry(entry)
    {}

    /**
     * Construct full descriptor
     *
     * @param physicalOpId Operator identifier
     * @param logicalOpName Logical operator name
     * @param name Physical operator name
     * @param module Operator module
     * @param entry Operator entry in module
     * @return
     */
    PhysicalOpDesc(OpID physicalOpId, const std::string& logicalOpName,
                   const std::string& name, const std::string& module,
                   const std::string& entry) :
        _physicalOpId(physicalOpId),
        _logicalOpName(logicalOpName),
        _name(name),
        _module(module),
        _entry(entry)
    {}

    /**
     * Get physical operator identifier
     *
     * @return Operator identifier
     */
    OpID getId() const
    {
        return _physicalOpId;
    }

    /**
     * Get logical operator name
     *
     * @return Operator name
     */
    const std::string& getLogicalName() const
    {
        return _logicalOpName;
    }

    /**
     * Get physical operator name
     *
     * @return Operator name
     */
    const std::string& getName() const
    {
        return _name;
    }

    /**
     * Get physical operator module
     *
     * @return Operator module
     */
    const std::string& getModule() const
    {
        return _module;
    }

    /**
     * Get physical operator entry in module
     *
     * @return Operator entry
     */
    const std::string& getEntry() const
    {
        return _entry;
    }

  private:
    OpID        _physicalOpId;
    std::string _logicalOpName;
    std::string _name;
    std::string _module;
    std::string _entry;
};

}  // namespace scidb
#endif
