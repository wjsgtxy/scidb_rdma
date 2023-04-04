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

#ifndef ARRAY_LIST_ARRAY_BUILDER_BASE_H_
#define ARRAY_LIST_ARRAY_BUILDER_BASE_H_

/****************************************************************************/

#include <array/MemArray.h>

/****************************************************************************/
namespace scidb
{
/****************************************************************************/

/**
 * Builds a per-instance MemArray that contains a list of arbitrary elements.
 *
 * Each MemArray built with this class contains two dimensions:
 *
 *  [inst=0:numInstances-1,1,0, n=0:*,LIST_CHUNK_SIZE,0]
 *
 * where 'n' is the zero-based number of the object at that particular instance
 * (0,1,2...).
 *
 * This allows us to create a list of an arbitrary number of objects on every
 * instance and present this list seamlessly as a single array.
 *
 * Subclasses generally need to provide just two things:
 *
 * - a getAttributes() function which returns the list of the K attributes for
 * the resulting array. K must include the empty tag.
 *
 * - a function that writes an individual list element, which should begin by
 * calling beginElement() and end by calling endElement().
 */
class ListArrayBuilder : boost::noncopyable
{
protected:
    static const uint64_t                   LIST_CHUNK_SIZE = 1000000;
    std::shared_ptr<Query>                _query;
    std::shared_ptr<MemArray>             _array;
    Coordinates                             _currPos;
    Coordinates                             _nextChunkPos;
    std::vector<std::shared_ptr<ArrayIterator> > _outAIters;
    std::vector<std::shared_ptr<ChunkIterator> > _outCIters;
    size_t                                  _nAttrs;
    size_t                                  _dimIdOff;
    std::string                       const _listName;

    /**
     *  Write out a value as the value of the attribute 'a'.
     */
    void write(AttributeID a,const Value& v)
    {
        _outCIters[a]->writeItem(v);
    }

    void write(AttributeID a,const std::string& s)
    {
        Value v;
        v.setString(s);
        write(a,v);
    }

    void write(AttributeID a,const char* s)
    {
        Value v;
        v.setString(s);
        write(a,v);
    }

    template<class type>
    void write(AttributeID a,const type& t)
    {
        write(a,Value(t,Value::asData));
    }

   /**
     *  Construct and return the dimensions of the array.
     *  @param query the query context
     *  @return dimensions as described above.
     */
    virtual Dimensions getDimensions(std::shared_ptr<Query> const& query) const;

    /**
     *  Construct and return the attributes of the array. The attributes must include the empty tag.
     *  @return the attributes that the result array contains.
     */
    virtual Attributes getAttributes() const = 0;

    /**
     *  Prepare to write out the details of a single element.
     */
    void beginElement();

    /**
     *  Finish writing out the details of a single element.
     */
    void endElement();

public:
             ListArrayBuilder(const std::string &listName = "list");
    virtual ~ListArrayBuilder() {}

    /**
     * Construct and return the schema of the array
     * @return the array named "list" using getDimensions and getAttributes
     */
    ArrayDesc getSchema(std::shared_ptr<Query> const& query) const;

    /**
     * Perform initialization and reset of internal fields.
     * @note Must be called prior to calling listElement or getArray.
     * @param query the query context
     */
    void initialize(const ArrayDesc& schema, std::shared_ptr<Query> const& query);

    /**
     * Get the result array. Initialize must be called prior to this.
     * @return a well-formed MemArray that contains information.
     */
    std::shared_ptr<MemArray> getArray();
};

/****************************************************************************/
} // namespace
/****************************************************************************/
#endif
/****************************************************************************/
