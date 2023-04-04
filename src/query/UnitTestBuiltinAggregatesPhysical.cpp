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



#include <array/MemArray.h>
#include <query/Aggregate.h>
#include <query/Expression.h>
#include <query/PhysicalOperator.h>
#include <query/Query.h>
#include <system/Exceptions.h>
#include <system/Utils.h>
#include <util/TsvParser.h>

#include <log4cxx/logger.h>

using namespace std;


// https://trac.scidb.net/wiki/Development/TestWithFakeOperators

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.unittest"));

class UnitTestBuiltinAggregatesPhysical: public PhysicalOperator
{
private:
    typedef std::map<Coordinate, Value> CoordValueMap;
    typedef std::pair<Coordinate, Value> CoordValueMapEntry;


public:

    UnitTestBuiltinAggregatesPhysical(const string& logicalName, const string& physicalName,
                    const Parameters& parameters, const ArrayDesc& schema)
    : PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

   /**
     * @brief Insert data from a map to an array.
     *
     * @param[in]    query
     * @param[inout] array  the array to receive data
     * @param[in]    m      the map of Coordinate --> Value
     */
    void insertMapDataIntoArray(
        std::shared_ptr<Query>& query,
        MemArray& array,
        CoordValueMap const& m)
    {
        Coordinates coord(1);
        coord[0] = 0;
        const auto& attrs = array.getArrayDesc().getAttributes(true);
        vector< std::shared_ptr<ArrayIterator> > arrayIters(attrs.size());
        vector< std::shared_ptr<ChunkIterator> > chunkIters(attrs.size());

        for (const auto& attr : attrs)
        {
            arrayIters[attr.getId()] = array.getIterator(attr);

            chunkIters[attr.getId()] =
                ((MemChunk&)arrayIters[attr.getId()]->newChunk(coord)).getIterator(
                    query, ChunkIterator::SEQUENTIAL_WRITE);
        }

        for (CoordValueMapEntry const& p : m) {
            coord[0] = p.first;
            for (const auto& attr : attrs)
            {
                if (!chunkIters[attr.getId()]->setPosition(coord))
                {
                    chunkIters[attr.getId()]->flush();
                    chunkIters[attr.getId()].reset();
                    chunkIters[attr.getId()] =
                        ((MemChunk&)arrayIters[attr.getId()]->newChunk(coord)).getIterator(
                            query, ChunkIterator::SEQUENTIAL_WRITE);
                    chunkIters[attr.getId()]->setPosition(coord);
                }
                chunkIters[attr.getId()]->writeItem(p.second);
            }
        }

        for (const auto& attr : attrs)
        {
            chunkIters[attr.getId()]->flush();
        }
    }

    /**
     * Get a command line parameter
     *
     * @param[in]   index - The index of the parameter to retrieve
     *
     * @author mcorbett@paradigm4.com
     */
    string getParameter(size_t index=0) const
    {
        if (index >= _parameters.size()) {
            return "";
        }

        std::shared_ptr<OperatorParamPhysicalExpression> &rExpression =
            (std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[index];

        switch(index)
        {
            case 0:
            {
                return rExpression->getExpression()->evaluate().getString();
            }

            case 1:
            {
                stringstream ss;
                ss << rExpression->getExpression()->evaluate().getUint64();
                return ss.str();
            }
        }

        return "";
    }



    /**
     * @brief Load an external tsv file into an array
     *
     * @param[in]   filename   - The name of the file
     * @param[in]   rQuery     - A reference to the requested query
     * @param[out]  rArrayInst - A reference to the array instance
     *
     * @throw SCIDB_SE_INTERNAL::SCIDB_LE_UNITTEST_FAILED
     *
     * This method was tested during development but is currently
     * not being used.
     *
     * @author mcorbett@paradigm4.com
     */
    void loadTsvFileIntoArray(
        const char *                filename,
        std::shared_ptr<Query>&   rQuery,
        std::shared_ptr<MemArray> &      rArrayInst)
    {

        size_t buflen = 128;
        char *bufp = static_cast<char*>(::malloc(buflen));
        vector<char *> fields;
        size_t lineno = 0;
        size_t i = 0;

        FILE* infile = ::fopen(filename, "rb");
        if(!infile)
        {
            free(bufp);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED)
                << __FILE__ << "Unable to open file";
        }


        CoordValueMap mapInst;
        while (EOF != ::getline(&bufp, &buflen, infile))
        {
            ++lineno;

            // Parse the line from the file into the fields vector
            if (!scidb::tsv_parse(bufp, fields, '\t')) {
                free(bufp);
                ::fclose(infile);

                stringstream ss;
                ss << "TsvParser.tsv_parse failed line=" << lineno << " i=" << i;
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED)
                     << __FILE__ << ss.str();
            }


            // Insert each of the fields into a map
            Value value;
            vector<char *>::iterator it;
            for(it = fields.begin();  it != fields.end();  ++it)
            {
                value.setUint32( static_cast<uint32_t>( atoi(*it) ) );
                mapInst[i] = value;
                ++i;
            }
        }

        free(bufp);
        ::fclose(infile);

        // Insert the map data into the array.
        insertMapDataIntoArray(rQuery, *rArrayInst, mapInst);
    }


    /**
     * @brief Load an external tsv file into "Values"
     *
     * @param[in]   filename   - The name of the file
     * @param[in]   rQuery     - A reference to the requested query
     * @param[out]  rValue     - A reference to the "Values" instance
     *
     * @throw SCIDB_SE_INTERNAL::SCIDB_LE_UNITTEST_FAILED
     *
     * @author mcorbett@paradigm4.com
     */
    void loadTsvFileIntoValue(
        const char *                filename,
        std::shared_ptr<Query>&   rQuery,
        Value &                     rValue)
    {
        size_t buflen = 128;
        char *bufp = static_cast<char*>(::malloc(buflen));
        vector<char *> fields;
        vector<char *>::iterator it;
        size_t lineno = 0;
        size_t entries = 0;

        FILE* infile = ::fopen(filename, "rb");
        if(!infile)
        {
            free(bufp);

            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED)
                << __FILE__ << "Unable to open file";
        }


        // Count the number of entries needed.
        while (EOF != ::getline(&bufp, &buflen, infile))
        {
            if (!scidb::tsv_parse(bufp, fields, '\t')) {
                free(bufp);
                ::fclose(infile);

                stringstream ss;
                ss << "TsvParser.tsv_parse failed lineno=" << lineno;
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED)
                    << __FILE__ << ss.str();
            }


            entries += fields.size();
            // for(it = fields.begin();  it != fields.end();  ++it)  entries++;
        }

        // Resize value to store i entries
        rValue.setSize<Value::IGNORE_DATA>(entries);
        ::fseek(infile, 0, SEEK_SET);
        size_t entry = 0;

        uint8_t *dest = rValue.getData<uint8_t>();
        while (EOF != ::getline(&bufp, &buflen, infile))
        {
            ++lineno;

            // Parse line and write tuple!
            if (!scidb::tsv_parse(bufp, fields, '\t')) {
                free(bufp);
                ::fclose(infile);

                stringstream ss;
                ss << "TsvParser.tsv_parse failed line=" << lineno;
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED)
                     << __FILE__ << ss.str();
            }


            for(it = fields.begin();  it != fields.end();  ++it)
            {
                dest[entry] = static_cast<uint8_t>( atoi(*it) );
                ++entry;
            }
        }

        free(bufp);
        ::fclose(infile);
    }

    /**
     * @brief Load random data into "rValue"
     *
     * @param[out]  rValue   - A reference to the "Values" to be filled
     * @param[in]   seed     - Seed to start the random # generator at
     * @param[in]   min      - The minimum value allowed
     * @param[in]   max      - The maximum value allowed
     *
     * @author mcorbett@paradigm4.com
     */
    void loadRandomDataIntoValue(
        Value &      rValue,
        uint32_t     seed,
        uint8_t      min=0,
        uint8_t      max=255)
    {
        size_t   entries = rValue.size();
        size_t   n;
        uint8_t  range;

        if(min >= max) return;
        range = safe_static_cast<uint8_t>(max - min);

        // Round down entries to 32 bit boundary
        entries = entries / sizeof(uint32_t);
        entries = entries * sizeof(uint32_t);

        // Fill in all 32 bit entries
        ::srandom(seed);
        uint8_t *dest = rValue.getData<uint8_t>();
        for(n = 0;  n < entries;  n += 4)
        {
            uint32_t value = static_cast<uint32_t>(::random());
            dest[n+0] = static_cast<uint8_t>((value % range) + min);  value >>= 8;
            dest[n+1] = static_cast<uint8_t>((value % range) + min);  value >>= 8;
            dest[n+2] = static_cast<uint8_t>((value % range) + min);  value >>= 8;
            dest[n+3] = static_cast<uint8_t>((value % range) + min);  value >>= 8;
        }

        // Fill in stragglers
        uint32_t value = static_cast<uint32_t>(::random());
        entries = rValue.size();
        if(n < entries) { dest[n++] = static_cast<uint8_t>((value % range) + min);  value >>= 8; }
        if(n < entries) { dest[n++] = static_cast<uint8_t>((value % range) + min);  value >>= 8; }
        if(n < entries) { dest[n++] = static_cast<uint8_t>((value % range) + min);  value >>= 8; }
    }



    /**
     * @brief Test the builtin aggregates.
     *
     * Currently only does a subset of the testing for the aggregates
     *
     * @param[in]   rSchema    - A reference to the output schema
     * @param[in]   rQuery     - A reference to the requested query
     * @param[out]  rArrayInst - Array to be filled with result (0=passing)
     *
     * @throw SCIDB_SE_INTERNAL::SCIDB_LE_UNITTEST_FAILED
     *
     * @author mcorbett@paradigm4.com
     */
    void testOnce_BuiltinAggregates(
        const ArrayDesc&            rSchema,
        std::shared_ptr<Query>&   rQuery,
        std::shared_ptr<MemArray> &      rArrayInst)
    {
        // Create the array
        std::shared_ptr<MemArray> arrayInst(new MemArray(rSchema,rQuery));
        rArrayInst = arrayInst;

        // Get a pointer to the approxdc aggregate
        AggregatePtr approxdcAggregate = AggregateLibrary::getInstance()->createAggregate(
            "approxdc", TypeLibrary::getType(TID_UINT32));


        CoordValueMap   mapInst;
        Value           dstValue;
        Value           srcState;

        // Set the final result to 0 by default
        dstValue.setSize<Value::IGNORE_DATA>(8);
        dstValue.setUint64(0ULL);
        mapInst[0] = dstValue;


        switch(_parameters.size())
        {
            default:
            case 0:
            {
                // No parameters were given with the command
                // so use previously generated data.

                // These patterns were tested between the 64 bit and
                // the incorrect 32bit version of finalResult and
                // differed in value.  That is what makes them good
                // candidates for this test.
                static struct {
                   uint8_t      min;
                   uint8_t      max;
                   uint32_t     seed;
                   uint64_t     expect;
                } Tests[] = {
                    { 13, 23, 1423714687, 3860823205   },
                    { 13, 23, 2999145946, 3894010025   },
                    { 13, 23, 1524282739, 3882591776   },
                    { 13, 23, 3182498643, 3893919878   },
                    { 13, 23, 1437176789, 3874137414   },
                    { 13, 23, 3007526547, 3854484617   },
                    { 13, 23, 1459483162, 3872206768   },
                    { 13, 23, 2955992490, 3864963761   },
                    { 13, 23, 1450080027, 3871096127   },
                    { 13, 23, 2966571556, 3858812117   },

                    { 10, 18, 1423715163, 389748686    },
                    { 10, 18, 1139227797, 389336584    },
                    { 10, 18, 1423176349, 389320812    },
                    { 10, 18, 1139237617, 387275318    },
                    { 10, 18, 1425160391, 389295898    },
                    { 10, 18, 1137055709, 389811638    },
                    { 10, 18, 1425676907, 386638471    },
                    { 10, 18, 1139914988, 389364304    },
                    { 10, 18, 1422164668, 389944251    },
                    { 10, 18, 1140494599, 389193379    },

                    { 14, 22, 1456637681, 6192629511   },
                    { 14, 22, 667845110,  6231206137   },
                    { 14, 22, 1420175631, 6201824738   },
                    { 14, 22, 621706477,  6217597700   },
                    { 14, 22, 1469546473, 6209211583   },
                    { 14, 22, 630092630,  6192703295   },
                    { 14, 22, 1418941545, 6228089492   },
                    { 14, 22, 665472765,  6224665826   },
                    { 14, 22, 1420726815, 6228385044   },
                    { 14, 22, 663949067,  6226049757   },

                    { 18, 26, 1422397348, 99548823900  },
                    { 18, 26, 2035977976, 99778608755  },
                    { 18, 26, 1109274763, 99210065401  },
                    { 18, 26, 1535067506, 99570016506  },
                    { 18, 26, 1974117768, 99110318575  },
                    { 18, 26, 1724240999, 99302992752  },
                    { 18, 26, 2016329495, 99821822049  },
                    { 18, 26, 1173790582, 99085212497  },
                    { 18, 26, 1409729575, 99486609368  },
                    { 18, 26, 2111564799, 99364027955  },
                };


                // Resize the srcState to be large enough to support
                // the input data needed by finalResult.
                srcState.setSize<Value::IGNORE_DATA>(128*1024);


                for(size_t n = 0;  n < sizeof(Tests)/sizeof(Tests[0]);  n++)
                {
                    // Initialize srcState with random data generated
                    // based on seed, min, and max.
                    loadRandomDataIntoValue(
                        srcState,
                        Tests[n].seed,
                        Tests[n].min,
                        Tests[n].max );

                    // Call the approxdc on the srcState to get
                    // the finalResult
                    approxdcAggregate->finalResult(dstValue, srcState);

                    // Compare the result from approxdcAggregate
                    // with the expected result
                    if(dstValue.getUint64() != Tests[n].expect)
                    {
                        // Set 1 as the output error value.  This will
                        // not really be seen except if someone changes
                        // the throw below to a log4cxx call.  In which,
                        // case I use 1 as the failing indicator.
                        dstValue.setUint64(1ULL);
                        mapInst[0] = dstValue;

                        // Throw the exception
                        stringstream ss;
                        ss << "ERROR:  "
                            << "  seed="        << Tests[n].seed
                            << "  min="         << Tests[n].min
                            << "  max="         << Tests[n].max
                            << "  received="    << dstValue.getUint64()
                            << "  expected="    << Tests[n].expect;

                        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED)
                            << __FILE__ << ss.str();
                    }
                }

                // Insert the resultant data (from the map) into the
                // array to be returned.
                insertMapDataIntoArray(rQuery, *rArrayInst, mapInst);
            } break;

            case 2:
            {
                // 2 parameters were given with the command.  The
                // first is the filename and the second is the expected
                // value correlating to the file.  The file is expected
                // to be a TSV with 16 values per line and 8192 lines.
                string filename = getParameter(0);
                string expValue = getParameter(1);

                // Convert the expValue string into the 64bit expected value.
                char *bp;
                uint64_t expected = strtoull(expValue.c_str(), &bp, 0);


                //loadFileIntoArray( filename.c_str(), rQuery, rArrayInst);
                loadTsvFileIntoValue( filename.c_str(),  rQuery, srcState );


                // Call the approxdc on the srcState to get
                // the finalResult
                approxdcAggregate->finalResult(dstValue, srcState);

                // Compare the result from approxdcAggregate
                // with the expected result
                if(expected != dstValue.getUint64())
                {
                    // Set 1 as the output error value.  This will
                    // not really be seen except if someone changes
                    // the throw below to a log4cxx call.  In which,
                    // case I use 1 as the failing indicator.
                    dstValue.setUint64(1ULL);
                    mapInst[0] = dstValue;

                    // Throw the exception
                    stringstream ss;
                    ss << "ERROR:  Expected=" << expected << "  Received=" << dstValue.getUint64();
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED)
                        << __FILE__ << ss.str();
                }

                // Insert the resultant data (from the map) into the
                // array to be returned.
                insertMapDataIntoArray(rQuery, *rArrayInst, mapInst);
            } break;
        }
     }

    /**
     * @brief Execute the fake operator
     *
     * @param[in]   inputArrays  - A reference to the output schema
     * @param[in]   query        - A reference to the requested query
     *
     * @return      Array descriptor
     *
     * @author mcorbett@paradigm4.com
     */
    std::shared_ptr<Array> execute(
        vector< std::shared_ptr<Array> >& inputArrays,
        std::shared_ptr<Query> query)
    {
        std::shared_ptr<MemArray> arrayInst;

        testOnce_BuiltinAggregates(_schema, query, arrayInst);
        return arrayInst;
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(
    UnitTestBuiltinAggregatesPhysical,
    "test_builtin_aggregates",
    "UnitTestBuiltinAggregatesPhysical");

}
