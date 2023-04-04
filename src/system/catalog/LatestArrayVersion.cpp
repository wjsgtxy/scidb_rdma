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

#include <array/ArrayID.h>
#include <log4cxx/logger.h>
#include <util/Pqxx.h>

#include <cassert>
#include <sstream>
#include <unordered_map>

using namespace pqxx;
using namespace pqxx::prepare;

namespace scidb {

    const char* const arrayTableQuery =
        "select ARR.namespace_name, "
        "substring(ARR.array_name,'([^@]+).*') as arr_name, "
        "max(ARR.array_id) as max_arr_id "
        "from namespace_arrays as ARR "
        "group by namespace_name, arr_name";


    class LatestArrayVersion
    {
        pqxx::connection* _connection;
        log4cxx::LoggerPtr _logger;

        LatestArrayVersion() = delete;
        bool _createTable(work& tr) const;
        void _populateTable(work& tr) const;
        bool _tableExists() const;
        bool _crossCheck() const;
        void _dropTable() const;
        void _buildTable() const;
        void _rebuildTable() const;

    public:
        LatestArrayVersion(pqxx::connection* connection,
                           log4cxx::LoggerPtr logger);
        void verifyTable() const;
    };

    LatestArrayVersion::LatestArrayVersion(pqxx::connection* connection,
                                           log4cxx::LoggerPtr logger)
        : _connection(connection)
        , _logger(logger)
    {
        assert(_connection);
    }

    bool LatestArrayVersion::_createTable(work& tr) const
    {
        assert(_connection);
        bool success = true;

        try {
            // Create the table within a transaction.  This is the sync point
            // where all instances will try to create this table.  This thundering
            // herd problem needs to be addressed separately with the other Postgres
            // thundering herd issues under SDB-5497.
            // As only one instance will succeed at creating the table, the others
            // will see it as "relation ... already exists".  The instance that
            // successfully creates the table will be the one to populate it.
            tr.exec("create table latest_array_version"
                    "("
                    "    namespace_name varchar,"
                    "    array_name varchar,"
                    "    array_id bigint references \"array\" (id) on delete cascade,"
                    "    primary key(namespace_name,array_name)"
                    " )");
        }
        catch (const std::exception& e) {
            LOG4CXX_TRACE(_logger, "unable to create latest_array_version, " << e.what());
            success = false;
        }

        return success;
    }

    void LatestArrayVersion::_populateTable(work& tr) const
    {
        assert(_connection);
        auto observed = tr.exec(arrayTableQuery);

        for (const auto& row : observed) {
            auto namespace_name = row.at("namespace_name").as(std::string());
            auto array_name = row.at("arr_name").as(std::string());
            auto max_arr_id = row.at("max_arr_id").as(uint64_t());
            std::ostringstream sql;
            sql << "insert into latest_array_version (namespace_name, array_name, array_id) values ('"
                << namespace_name << "','"
                << array_name << "',"
                << max_arr_id << ")";
            tr.exec(sql.str());
        }
    }

    bool LatestArrayVersion::_tableExists() const
    {
        assert(_connection);
        work tr(*_connection);  // read-only, no commit

        auto result =
            tr.exec("select count(tablename) from pg_tables where tablename = 'latest_array_version'");

        return result[0].at("count").as(uint64_t()) == 1;
    }

    namespace {

        class ResultMap
        {
            // Maps fully-qualified array names (namespace.array_name) to their Array ID
            std::unordered_map<std::string, ArrayID> _arrayIDs;

            std::string _createKey(const std::string& namespace_name,
                                   const std::string& array_name) const
            {
                return namespace_name + '.' + array_name;
            }

        public:
            auto begin() const
            {
                return _arrayIDs.begin();
            }

            auto empty() const
            {
                return _arrayIDs.empty();
            }

            auto end() const
            {
                return _arrayIDs.end();
            }

            void insert(const std::string& namespace_name,
                        const std::string& array_name,
                        ArrayID arrayID)
            {
                auto key = _createKey(namespace_name, array_name);
                _arrayIDs[key] = arrayID;
            }

            template <typename IteratorT>
            auto find(const IteratorT& element) const
            {
                auto key = element->first;
                return _arrayIDs.find(key);
            }

            auto size() const
            {
                return _arrayIDs.size();
            }

            static ResultMap fromPqxxResult(const pqxx::result& result)
            {
                ResultMap resultMap;

                for (const auto& row : result) {
                    auto namespace_name = row.at("namespace_name").as(std::string());
                    auto array_name = row.at("arr_name").as(std::string());
                    auto max_arr_id = row.at("max_arr_id").as(uint64_t());
                    resultMap.insert(namespace_name, array_name, max_arr_id);
                }

                return resultMap;
            }

            bool contains(const ResultMap& rhs) const
            {
                bool matches = true;

                for (auto o_iter = begin(); o_iter != end() && matches; ++o_iter) {
                    auto e_iter = rhs.find(o_iter);
                    if (e_iter != rhs.end()) {
                        matches = matches && (o_iter->first == e_iter->first &&
                                              o_iter->second == e_iter->second);
                    }
                    else {
                        matches = false;
                    }
                }

                return matches;
            }

            bool operator== (const ResultMap& rhs) const
            {
                return this->size() == rhs.size() &&
                       this->contains(rhs) &&
                       rhs.contains(*this);
            }
        };

    }  // anonymous namespace

    bool LatestArrayVersion::_crossCheck() const
    {
        work tr(*_connection);  // read-only, no commit
        pqxx::result expected, observed;

        try {
            expected =
                tr.exec("select namespace_name, array_name as arr_name, "
                        "array_id as max_arr_id from latest_array_version");
            observed = tr.exec(arrayTableQuery);
        }
        catch (const pqxx::undefined_table& e) {
            // It's conceivable that one instance already came through this
            // code, saw that the table was not correct and has moved on to
            // rebuilding it.  That guy wins in the sense that we don't need
            // to verify something that's at some point in reconstruction.
            LOG4CXX_TRACE(_logger,
                          "latest_array_version removed by other instance, " << e.what());
            return true;
        }

        // Put all of the cells in the 'array' table into the collection of
        // expected entries, put all of the cells in the 'latest_array_version'
        // table into the collection of observed entries; perform a set equality
        // operation on the two collections by verifying that all elements from
        // each are present in the other.
        auto expectedMap = ResultMap::fromPqxxResult(expected);
        auto observedMap = ResultMap::fromPqxxResult(observed);
        return observedMap == expectedMap;
    }

    void LatestArrayVersion::_dropTable() const
    {
        assert(_connection);
        work tr(*_connection);

        try {
            tr.exec("drop table latest_array_version");
            tr.commit();
        }
        catch (const std::exception& e) {
            // If this instance failed to drop the table, then
            // it's likely that another instance dropped it first;
            // continue on.
            LOG4CXX_TRACE(_logger, "unable to drop latest_array_version, " << e.what());
        }
    }

    void LatestArrayVersion::_buildTable() const
    {
        // Race to create the table, whichever instance creates it
        // will be the one to populate it.
        work tr(*_connection);
        if (_createTable(tr)) {
            _populateTable(tr);
            tr.commit();  // success!  commit new table and rows
        }
    }

    void LatestArrayVersion::_rebuildTable() const
    {
        _dropTable();
        _buildTable();
    }

    void LatestArrayVersion::verifyTable() const
    {
        // Build the last_array_version table if it doesn't yet exist.
        if (!_tableExists()) {
            _buildTable();
        }
        else {
            // If the latest_array_version table exists, verify that it's correct.
            bool matches = _crossCheck();

            // If the latest_array_version table doesn't have what's expected, then
            // recover by dropping and rebuilding it.
            if (!matches) {
                _rebuildTable();
            }
        }
    }

    void verifyLatestArrayVersionTable(pqxx::connection* connection,
                                       log4cxx::LoggerPtr logger)
    {
        LatestArrayVersion tableUpgrade(connection, logger);
        tableUpgrade.verifyTable();
    }

}  // namespace scidb
