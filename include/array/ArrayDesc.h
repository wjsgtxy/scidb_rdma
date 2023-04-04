#ifndef ARRAYDESC_H_
#define ARRAYDESC_H_
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
#include <array/VersionID.h>
#include <array/Coordinate.h>
#include <query/InstanceID.h>
#include <array/DimensionDesc.h> // because of inline implementation of setDimensions

#include <array/Attributes.h>
#include <array/Dimensions.h>

#include <system/Utils.h>  // For SCIDB_ASSERT

#include <boost/operators.hpp>

#include <iosfwd>
#include <sstream>
#include <memory>
#include <string>
#include <vector>

namespace scidb {
class AttributeDesc;
class DimensionDesc;
class ArrayDistribution;
class ArrayResidency;

/**
 * Descriptor of array. Used for getting metadata of array from catalog.
 */
class ArrayDesc : boost::equality_comparable<ArrayDesc>
{
    friend class DimensionDesc;
public:
    /**
     * Various array qualifiers
     *
     * @note Does anyone recall why these begin at 0x10 instead of
     * 0x01?  The TRANSIENT flag is kept in the system catalog, so
     * perhaps for backward compatibility with an older enum version?
     */
    enum ArrayFlags
    {
        TRANSIENT    = 0x10,    ///< Represented as a MemArray held in the TransientCache
        DEAD         = 0x20,    ///< Catalog entries for transient arrays are marked DEAD on instance restart
        INCOMPLETE   = 0x40,    ///< Missing plugins (UDTs, namespaces) prevent full use of this array
    };

    /**
     * Construct empty array descriptor (for receiving metadata)
     */
    ArrayDesc();

    /**
     * Construct partial array descriptor (without id, for adding to catalog)
     *
     * @param namespaceName The name of the namespace the array is in
     * @param arrayName array name
     * @param attributes vector of attributes
     * @param dimensions vector of dimensions
     * @param arrDist array distribution
     * @param arrRes array residency
     * @param flags array flags from ArrayDesc::ArrayFlags
     */
    ArrayDesc(const std::string &namespaceName,
              const std::string &arrayName,
              const Attributes& attributes,
              const Dimensions &dimensions,
              // const ArrayDistPtr& arrDist,
              const std::shared_ptr<const ArrayDistribution>& arrDist,
              // const ArrayResPtr& arrRes,
              const std::shared_ptr<const ArrayResidency>& arrRes,
              int32_t flags = 0);

    /**
     * Construct partial array descriptor (without id, for adding to catalog)
     *
     * @param arrayName array name
     * @param attributes vector of attributes
     * @param dimensions vector of dimensions
     * @param arrDist array distribution
     * @param arrRes array residency
     * @param flags array flags from ArrayDesc::ArrayFlags
     * @param isCcmClient Are the arrRes/arrDist for a client (ccmbridge)
     */
    ArrayDesc(const std::string &arrayName,
              const Attributes& attributes,
              const Dimensions &dimensions,
              // const ArrayDistPtr& arrDist,
              const std::shared_ptr<const ArrayDistribution>& arrDist,
              const std::shared_ptr<const ArrayResidency>& arrRes,
              int32_t flags = 0,
              bool isCcmClient = false);

    /**
     * Construct full descriptor (for returning metadata from catalog)
     *
     * @param arrId the unique array ID
     * @param uAId the unversioned array ID
     * @param vId the version number
     * @param namespaceName The name of the namespace the array is in
     * @param arrayName array name
     * @param attributes vector of attributes
     * @param dimensions vector of dimensions
     * @param arrDist array distribution
     * @param arrRes array residency
     * @param flags array flags from ArrayDesc::ArrayFlags
     */
    ArrayDesc(ArrayID arrId, ArrayUAID uAId, VersionID vId,
              const std::string &namespaceName,
              const std::string &arrayName,
              const Attributes& attributes,
              const Dimensions &dimensions,
              // const ArrayDistPtr& arrDist,
              const std::shared_ptr<const ArrayDistribution>& arrDist,
              // const ArrayResPtr& arrRes,
              const std::shared_ptr<const ArrayResidency>& arrRes,
              int32_t flags = 0);

    /**
     * Construct full descriptor (for returning metadata from catalog)
     *
     * @param arrId the unique array ID
     * @param uAId the unversioned array ID
     * @param vId the version number
     * @param arrayName array name
     * @param attributes vector of attributes
     * @param dimensions vector of dimensions
     * @param arrDist array distribution
     * @param arrRes array residency
     * @param flags array flags from ArrayDesc::ArrayFlags
     */
    ArrayDesc(ArrayID arrId, ArrayUAID uAId, VersionID vId,
              const std::string &arrayName,
              const Attributes& attributes,
              const Dimensions &dimensions,
              // const ArrayDistPtr& arrDist,
              const std::shared_ptr<const ArrayDistribution>& arrDist,
              // const ArrayResPtr& arrRes,
              const std::shared_ptr<const ArrayResidency>& arrRes,
              int32_t flags = 0);

    /**
     * Copy constructor
     */
    ArrayDesc(ArrayDesc const& other);

   ~ArrayDesc() { }

    /**
     * Make a fully qualified array name for this instance of namespaceName and arrayName.
     */
    std::string getQualifiedArrayName(bool includeVersion = true) const;

    /**
     * Assignment operator
     */
    ArrayDesc& operator = (ArrayDesc const&);

    /**
     * Get the unversioned array id (id of parent array)
     * @return unversioned array id
     */
    ArrayUAID getUAId() const
    {
        return _uAId;
    }

    /**
     * Get the unique versioned array id.
     * @return the versioned array id
     */
    ArrayID getId() const
    {
        return _arrId;
    }

    /**
     * Get the array version number.
     * @return the version number
     */
    VersionID getVersionId() const
    {
        return _versionId;
    }

    /**
     * Set array identifiers
     * @param [in] arrId the versioned array id
     * @param [in] uAId the unversioned array id
     * @param [in] vId the version number
     */
    void setIds(ArrayID arrId, ArrayUAID uAId, VersionID vId)
    {
        _arrId = arrId;
        _uAId = uAId;
        _versionId = vId;
    }

    /**
     * Get name of array
     * @return versioned array name (e.g., arr@X)
     */
    const std::string& getName() const
    {
        return _arrayName;
    }

    /**
     * Set name of array
     * @param arrayName array name
     */
    void setName(const std::string& arrayName)
    {
        _arrayName = arrayName;
    }

    /**
     * Get name of the namespace the array belongs to
     * @return namespace name
     */
    const std::string& getNamespaceName() const
    {
        return _namespaceName;
    }

    /**
     * Set name of namespace the array belongs to
     * @param namespaceName The namespace name
     */
    void setNamespaceName(const std::string& namespaceName)
    {
        _namespaceName = namespaceName;
    }

    /**
     * Get static array size (number of elements within static boundaries)
     * @return array size
     */
    uint64_t getSize() const;

    /**
     * Get bitmap attribute used to mark empty cells
     * @return descriptor of the empty indicator attribute or NULL is array is regular
     */
    AttributeDesc const* getEmptyBitmapAttribute() const;

    /**
     * Get vector of array attributes.
     * @param excludeEbm When true, returns an Attributes container without
     * the empty bitmap present, regardless of whether the array has an empty
     * bitmap or not.  When false, returns an Attributes container with the
     * empty bitmap if the array has one or without an empty bitmap if the
     * array does not have one.
     * @return An Attributes container.
     */
    Attributes const& getAttributes(bool excludeEbm = false) const;

    /** Set vector of array attributes */
    ArrayDesc& setAttributes(Attributes const& attributes);

    ArrayDesc& addEmptyTagAttribute();

    /**
     * Get vector of array dimensions
     * @return array dimensions
     */
    Dimensions const& getDimensions() const {return _dimensions;}
    Dimensions&       getDimensions()       {return _dimensions;}

    /**
     * Get the current low boundary of an array
     * @note This is NOT reliable.
     *       The only time the value can be trusted is when the array schema is returned by scan().
     *       As soon as we add other ops into the mix (e.g. filter(scan()), the value is not trustworthy anymore.
     */
    Coordinates getLowBoundary() const ;

    /**
     * Get the current high boundary of an array
     * @note This is NOT reliable.
     *       The only time the value can be trusted is when the array schema is returned by scan().
     *       As soon as we add other ops into the mix (e.g. filter(scan()), the value is not trustworthy anymore.
     */
    Coordinates getHighBoundary() const ;

    /** Set vector of array dimensions */
    ArrayDesc& setDimensions(Dimensions const& dims)
    {
        _dimensions = dims;
        initializeDimensions();
        return *this;
    }

    /**
     * Find the index of a DimensionDesc by name and alias.
     * @return index of desired dimension or -1 if not found
     */
    ssize_t findDimension(const std::string& name, const std::string& alias) const;

    /**
     * Check if position belongs to the array boundaries
     */
    bool contains(Coordinates const& pos) const;

    /**
     * Get position of the chunk for the given coordinates
     * @param[in,out] pos  an element position goes in, a chunk position goes out (not including overlap).
     */
    void getChunkPositionFor(Coordinates& pos) const;

    /**
     * Get position of the chunk for the given coordinates from a supplied Dimensions vector
     * @param[in] dims     dimensions to use in computing a chunk position from an element position
     * @param[in,out] pos  an element position goes in, a chunk position goes out (not including overlap).
     */
    static void getChunkPositionFor(Dimensions const& dims, Coordinates& pos);

    /**
     * @return whether a given position is a chunk position.
     * @param[in] pos  a cell position.
     */
    bool isAChunkPosition(Coordinates const& pos) const;

    /**
     * @return whether a cellPos belongs to a chunk specified with a chunkPos.
     * @param[in] cellPos  a cell position.
     * @param[in] chunkPos a chunk position.
     */
    bool isCellPosInChunk(Coordinates const& cellPos, Coordinates const& chunkPos) const;

    /**
      * Get boundaries of the chunk
      * @param chunkPosition - position of the chunk (should be aligned (for example, by getChunkPositionFor)
      * @param withOverlap - include or not include chunk overlap to result
      * @param lowerBound - lower bound of chunk area
      * @param upperBound - upper bound of chunk area
      */
    void getChunkBoundaries(Coordinates const& chunkPosition,
                            bool withOverlap,
                            Coordinates& lowerBound,
                            Coordinates& upperBound) const;
   /**
     * Get assigned instance for chunk for the given coordinates.
     * This function is unaware of the current query liveness; therefore,
     * the returned instance may not be identified by the same logical instanceID in the current query.
     * To get the corresponding logical instance ID use @see Query::mapPhysicalToLogical()
     * @param pos chunk position
     * @param originalInstanceCount query->getNumInstances() from the creating/storing query
     *        this was not saved in the ArrayDesc, so it is supplied from without
     *        at this time.  [For a more precise rules of whether this value changes on
     *        a per-version basis or not, one must investigate how the storage manager
     *        currently persists this value.]
     *
     * TODO: consider a design where the original instance count is managed alongside _dt
     *       to alleviate a potential source of errors that happen only during failover
     */
    InstanceID getPrimaryInstanceId(Coordinates const& pos, size_t instanceCount) const;

    /**
     * Get flags associated with array
     * @return flags
     */
    int32_t getFlags() const
    {
        return _flags;
    }

    /**
     * Trim unbounded array to its actual boundaries
     */
    void trim();

    /**
     * Checks if array has non-zero overlap in any dimension
     */
    bool hasOverlap() const { return scidb::hasOverlap(_dimensions); }

    /**
     * Checks if any dimension has an unspecified (autochunked) chunk interval.
     */
    bool isAutochunked() const { return scidb::isAutochunked(_dimensions); }

    /**
     * True iff array's dimensions are dataframe dimensions.
     */
    bool isDataframe() const { return scidb::isDataframe(_dimensions); }

    /**
     * Return true if the array is marked as being 'transient'. See proposal
     * 'TransientArrays' for more details.
     */
    bool isTransient() const
    {
        return _flags & TRANSIENT;
    }

    /**
     * Mark or unmark the array as being 'transient'. See proposal
     * 'TransientArrays' for more details.
     */
    ArrayDesc& setTransient(bool transient)
    {
        if (transient)
        {
            _flags |= TRANSIENT;
        }
        else
        {
            _flags &= (~TRANSIENT);
        }

        return *this;
    }

    /**
     * Return true if all plugins used by the array are present.
     *
     * For example, an array with attributes of a user-defined type
     * will not be fully usable if that UDT plugin is not loaded.
     */
    bool isComplete() const
    {
        return (_flags & INCOMPLETE) == 0;
    }

    /**
     * Return true if a restart wiped out this transient array.
     */
    bool isDead() const
    {
        SCIDB_ASSERT((_flags & (TRANSIENT|DEAD)) != DEAD);
        return _flags & DEAD;
    }

    /**
     * Add alias to all objects of schema
     * tigor: Whenever the AFL/AQL keyword 'as ABC',
     * tigor: ABC is attached to all? the names of the metadata as aliases.
     * tigor: The algorith for assigning aliases is not formal/consistent
     * tigor: (e.i. propagation through a binary op) and mostly works as implemented.
     * @param alias alias name
     */
    void addAlias(const std::string &alias);

    /**
     * @return a string representation of the ArrayDescriptor
     * including the attributes, dimensions, array IDs and version, flags, and the DistType
     */
    std::string toString () const;

    template<class Archive>
    void serialize(Archive& ar,unsigned version);

    bool operator ==(ArrayDesc const& other) const;

    void cutOverlap();
    Dimensions grabDimensions(VersionID version) const;

    bool coordsAreAtChunkStart(Coordinates const& coords) const;
    bool coordsAreAtChunkEnd(Coordinates const& coords) const;

    void addAttribute(AttributeDesc const& newAttribute);

    double getNumChunksAlongDimension(size_t dimension,
                                      Coordinate start = CoordinateBounds::getMax(),
                                      Coordinate end = CoordinateBounds::getMin()) const;

    /**
     * Check if two array descriptors are conformant, i.e. if srcDesc one can be "stored" into
     * dstDesc.  It checks the number of dimensions, DistType, and flags.  It uses
     * checkAttributeConformity (below) to check attributes.
     *
     * @throws scidb::SystemException with SCIDB_LE_ARRAYS_NOT_CONFORMANT or a more precise error code
     * @param srcDesc source array schema
     * @param dstDesc target array schema
     * @param options bit mask of options to toggle certain conformity checks
     */
    static void checkConformity(ArrayDesc const& srcDesc, ArrayDesc const& dstDesc, unsigned options = 0);

    /**
     * Check if two array descriptors' attributes are conformant, i.e. a cell from one can be stored
     * in the other.  It checks the number and types of the attributes.
     * @param srcDesc source array schema
     * @param dstDesc target array schema
     */
    static void checkAttributeConformity(ArrayDesc const& srcDesc, ArrayDesc const& dstDesc);

    /**
     * Check if all dimension names and attribute names are unique.
     */
    bool areNamesUnique() const;

    /** Option flags for use with checkConformity() */
    enum ConformityOptions {
        IGNORE_PSCHEME  = 0x01, /**< Array distributions and residencies need not match */
        IGNORE_OVERLAP  = 0x02, /**< Chunk overlaps need not match */
        IGNORE_INTERVAL = 0x04, /**< Chunk intervals need not match */
        SHORT_OK_IF_EBM = 0x08  /**< Src dim can be shorter if src array has empty bitmap */
    };

    /**
     *  @brief      A union of the various possible schema filter attributes.
     *
     *  @details    Modelled after the Arena Options class, class
     *              SchemaFieldSelector provides a sort of union of the
     *              many parameters with which an schema can be filtered.
     *              It uses the 'named parameter idiom' to enable these
     *              options to be supplied by name in any convenient
     *              order. For example:
     *
     *  @code
     *              sameSchema(
     *                  schemaInstance,
     *                  SchemaFieldSelector()
     *                      .startMin(true)
     *                      .endMax(true));
     *  @endcode
     *
     *  @see        http://www.parashift.com/c++-faq/named-parameter-idiom.html for
     *              a description of the 'named parameter idiom'.
     *
     *  @author     mcorbett@paradigm4.com.
     */

    class SchemaFieldSelector
    {
    public:

        // Construction
        SchemaFieldSelector()
            : _startMin(false)
            , _endMax(false)
            , _chunkInterval(false)
            , _chunkOverlap(false)
            , _wildcardInterval(false)
        {

        }

        // Attributes
        bool startMin()       const { return _startMin;      }
        bool endMax()         const { return _endMax;        }
        bool chunkInterval()  const { return _chunkInterval; }
        bool chunkOverlap()   const { return _chunkOverlap;  }
        bool wildcardInterval() const { return _wildcardInterval; }

        // Operations
        SchemaFieldSelector & startMin(bool b)      { _startMin=b;       return *this; }
        SchemaFieldSelector & endMax(bool b)        { _endMax=b;         return *this; }
        SchemaFieldSelector & chunkInterval(bool b) { _chunkInterval=b;  return *this; }
        SchemaFieldSelector & chunkOverlap(bool b)  { _chunkOverlap=b;   return *this; }
        SchemaFieldSelector & wildcardInterval(bool b) { _wildcardInterval=b; return *this; }

    private:
        // Representation
        unsigned  _startMin      : 1;  // true = startMin selected
        unsigned  _endMax        : 1;  // true = endMax selected
        unsigned  _chunkInterval : 1;  // true = chunkInterval selected
        unsigned  _chunkOverlap  : 1;  // true = chunkOverlap selected
        unsigned  _wildcardInterval : 1; // Let RHS chunkInterval match AUTOCHUNK or PASSTHRU
    };

    /**
     * Determine whether two schemas are equivalent, based on a field selector
     * @returns true IFF the selected fields within the schema match
     * @throws internal error if dimension sizes do not match.
     */
    bool sameSchema(ArrayDesc const& other, SchemaFieldSelector const &sel) const;

    /**
     * Determine whether two schemas have the same dimension parameters
     * @returns true IFF the schemas match
     * @throws internal error if dimension sizes do not match.
     */
    bool sameShape(ArrayDesc const& other) const
    {
        return sameSchema(other,
            SchemaFieldSelector()
                .startMin(true)
                .endMax(true)
                .chunkInterval(true)
                .chunkOverlap(true));
    }

    /**
     * Determine whether two arrays have strictly the same partitioning.
     * @returns true IFF all dimensions have same chunk sizes and overlaps
     * @throws internal error if dimension sizes do not match.
     */
    bool samePartitioning(ArrayDesc const& other) const
    {
        return sameSchema(other,
            SchemaFieldSelector()
                .chunkInterval(true)
                .chunkOverlap(true));
    }

    /**
     * Determine whether two arrays have loosely the same partitioning.
     * @returns True IFF all dimensions have same chunk sizes and overlaps,
     *          allowing unspecified chunk sizes in @c *this to match any
     *          specified size.
     * @throws internal error if dimension sizes do not match.
     * @note The intent is to allow empty catalog arrays with unspecified chunk
     *       sizes to adopt the incoming chunk sizes of the array being stored.
     */
    bool sameLoosePartitioning(ArrayDesc const& other) const
    {
        return sameSchema(other,
            SchemaFieldSelector()
                .chunkInterval(true)
                .wildcardInterval(true)
                .chunkOverlap(true));
    }

    /**
     * Compare the dimensions of this array descriptor with those of "other"
     * @returns true IFF all dimensions have same startMin and endMax
     * @throws internal error if dimension sizes do not match.
     */
    bool sameDimensionRanges(ArrayDesc const& other) const
    {
        return sameSchema(other,
            SchemaFieldSelector()
                .startMin(true)
                .endMax(true));
    }

    void replaceDimensionValues(ArrayDesc const& other);

    std::shared_ptr<const ArrayDistribution> getDistribution() const { SCIDB_ASSERT(_distribution); return _distribution; }
    void setDistribution(std::shared_ptr<const ArrayDistribution> const& dist) { SCIDB_ASSERT(dist); _distribution=dist; }

    std::shared_ptr<const ArrayResidency> getResidency() const { SCIDB_ASSERT(_residency); return _residency; }
    void setResidency(std::shared_ptr<const ArrayResidency> const& res) { SCIDB_ASSERT(res); _residency=res; }

    // @return true if any attributes or dimensions are reserved
    // or, if isDataframe(), any attributes are reserved.
    bool hasReservedNames() const;

    /**
     * Specify the empty tag compression; default is CompressorType::NONE.  Once
     * the array has been committed to the catalog, changing this value will
     * have no effect.
     *
     * This method should be called by the catalog and operators 'create array'
     * and 'store', only.
     *
     * @param etCompression One of the compression algorithms from the
     * CompressorType enum.  If CompressorType::UNKNOWN is specified, then it is
     * interpreted as CompressorType::NONE.
     */
    void setEmptyTagCompression(CompressorType etCompression);

private:
    void initializeDimensions();

    /**
     * The Versioned Array Identifier - unique ID for every different version of a named array.
     * This is the most important number, returned by ArrayDesc::getId(). It is used all over the system -
     * to map chunks to arrays, for transaction semantics, etc.
     */
    ArrayID _arrId;

    /**
     * The Unversioned Array Identifier - unique ID for every different named array.
     * Used to relate individual array versions to the "parent" array. Some arrays are
     * not versioned. Examples are IMMUTABLE arrays as well as NID arrays.
     * For those arrays, _arrId is is equal to _uAId (and _versionId is 0)
     */
    ArrayUAID _uAId;

    /**
     * The Array Version Number - simple, aka the number 3 in "myarray@3".
     */
    VersionID _versionId;

    std::string _namespaceName;
    std::string _arrayName;
    Attributes _attributes;
    Attributes _attributesWithoutBitmap;
    Dimensions _dimensions;
    int32_t _flags;
    /// Method for distributing chunks across instances
    std::shared_ptr<const ArrayDistribution> _distribution;
    /// Instance over which array chunks are distributed
    std::shared_ptr<const ArrayResidency> _residency;
};

void printSchema(std::ostream& stream,
                 const ArrayDesc& ob,
                 bool includeDimensions = true);
std::ostream& operator<<(std::ostream&,const ArrayDesc&);

/**
 * Replaces the attributes in the destination array descriptor.
 *
 * @param source The attributes from this descriptor will replace
 *     the attributes in dest.
 * @param dest The array descriptor whose attributes will be
 *     replaced.
 */
void cloneAttributes(ArrayDesc const& source, ArrayDesc& dest);

}  // namespace scidb

#endif
