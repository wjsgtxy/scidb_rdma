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

/**
 * @file ArrayDescSerialize.cpp
 *
 * @brief boost serialization method of ArrayDesc
 *        not included in client library
 */
#include <array/ArrayDesc.h>

#include <boost/serialization/vector.hpp>
#include <boost/serialization/set.hpp>

#include <sstream>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <log4cxx/logger.h>

#ifndef SCIDB_CLIENT
#include <system/Config.h>
#endif

#include <util/PointerRange.h>
#include <system/SciDBConfigOptions.h>
#include <query/TypeSystem.h>
#include <array/ArrayDistribution.h>
#include <system/SystemCatalog.h>
#include <system/Utils.h>
#include <util/compression/Compressor.h>
#include <util/BitManip.h>

using namespace std;

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("metadataserialize"));


template<class Archive>
void ArrayDesc::serialize(Archive& ar,unsigned version)
{
        ar & _arrId;
        ar & _uAId;
        ar & _versionId;
        ar & _namespaceName;  // Note:  deal with backwards compatability of the save format
        ar & _arrayName;
        _attributes.serializer(ar);
        ar & _dimensions;
        ar & _flags;

        // XXX TODO: improve serialization of residency and distribution
        // XXX TODO: the default residency does not need to be sent out ? it is the liveness set.

        DistType dt;
        std::string distState;
        size_t redundancy;
        size_t instanceShift;
        Coordinates offsetUnused;
        std::vector<InstanceID> residency;

        // serialize distribution + residency

        if (Archive::is_loading::value)
        {
            // de-serializing data
            ar & dt;
            ar & distState;
            ar & redundancy;
            ar & instanceShift;
            ar & offsetUnused; // unused, but can't remove without breaking binary compatibility
            ar & residency;

            SCIDB_ASSERT(!_residency);
            SCIDB_ASSERT(!_distribution);

            ASSERT_EXCEPTION(instanceShift < residency.size(),
                             "Serialized distribution instance shift is "
                             "greater than residency size");

            _distribution = ArrayDistributionFactory::getInstance()->construct(dt,
                                                                               redundancy,
                                                                               distState,
                                                                               instanceShift);
            ASSERT_EXCEPTION(_distribution,
                             "Serialized array descriptor has no distribution");

            _residency = createDefaultResidency(PointerRange<InstanceID>(residency));

            _attributesWithoutBitmap = _attributes;
            _attributesWithoutBitmap.deleteEmptyIndicator();
        } else {
            // serializing data
            dt = _distribution->getDistType();
            distState = _distribution->getState();
            redundancy = _distribution->getRedundancy();
            ArrayDistributionFactory::getTranslationInfo(_distribution.get(), instanceShift);

            const size_t resSize = _residency->size();
            residency.reserve(resSize);

            for (size_t i=0; i < resSize; ++i) {
                residency.push_back(_residency->getPhysicalInstanceAt(i));
            }
            ar & dt;
            ar & distState;
            ar & redundancy;
            ar & instanceShift;
            ar & offsetUnused; // unused, but can't remove without breaking binary compatibility
            ar & residency;
        }
}

// explicit instantiation avoid publishing the above definition in the header,
template void ArrayDesc::serialize<boost::archive::text_iarchive>(boost::archive::text_iarchive&, unsigned int);
template void ArrayDesc::serialize<boost::archive::text_oarchive>(boost::archive::text_oarchive&, unsigned int);

} // namespace
