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

#ifndef VALUE_H_
#define VALUE_H_

/****************************************************************************/

#include <boost/operators.hpp>                           // For op overloads
#include <limits>
#include <assert.h>

/****************************************************************************/
namespace scidb {
/****************************************************************************/

class Type;
class RLEPayload;

typedef std::string TypeId;

/**
 *  Represents a single data value.
 *
 *  Can also represent an old-style 'tile' of values, a representation that we
 *  wish to move away from, however.
 */
class Value : boost::equality_comparable<Value>
{
public:                   // Supporting Types
    enum    asData_t          {asData};                  // Has actual datum
    enum    asTile_t          {asTile};                  // Has an RLEPayload*
    enum    asView_t          {asView};                  // Unowned datum
    typedef int8_t            reason;                    // Why is it missing?
    typedef uint32_t          size_type;                 // Max supported size
    typedef void*             small_value_type;          // For static_asserts on sizeof(_data)
    class                     Formatter;                 // Forward decl

public:                   // Construction
    explicit                  Value();
    explicit                  Value(size_t);
    explicit                  Value(const Type&);
    template<class type>      Value(const type&,asData_t);
                              Value(const Type&,asTile_t);
                              Value(void*,size_t);
                              Value(void*,size_t,asView_t);
                              Value(const Value&);
                             ~Value();

public:                   // Operations
            Value&            operator= (const Value&);
    friend  bool              operator==(const Value&, const Value&);
    friend  std::ostream&     operator<<(std::ostream&,const Value&);

public:                   // Operations
            bool              isNull()             const {return _code >= 0;}
            bool              isTile()             const {return _code == MR_TILE;}
            bool              isDatum()            const {return _code == MR_DATUM;}
            bool              isView()             const {return _code == MR_VIEW;}
            bool              isDatumOrView()      const {return _code == MR_DATUM || _code == MR_VIEW;}
            bool              isSmall()            const {return isDatumOrView() && small(_size);}
            bool              isLarge()            const {return isDatumOrView() && large(_size);}
            bool              isString()           const;
            bool              getBool()            const {return get<bool>    ();}
            char              getChar()            const {return get<char>    ();}
            int8_t            getInt8()            const {return get<int8_t>  ();}
            int16_t           getInt16()           const {return get<int16_t> ();}
            int32_t           getInt32()           const {return get<int32_t> ();}
            int64_t           getInt64()           const {return get<int64_t> ();}
            uint8_t           getUint8()           const {return get<uint8_t> ();}
            uint16_t          getUint16()          const {return get<uint16_t>();}
            uint32_t          getUint32()          const {return get<uint32_t>();}
            uint64_t          getUint64()          const {return get<uint64_t>();}
            time_t            getDateTime()        const {return get<time_t>();}
    static_assert(sizeof(time_t) == 8, "DateTime is serialized as a 64 bit number");
            float             getFloat()           const {return get<float>   ();}
            double            getDouble()          const {return get<double>  ();}
            size_type         size()               const {return _size;}
            void*             data()               const;
            RLEPayload*       getTile()            const {assert(_code==MR_TILE);return _tile;}
      const char*             getString()          const;
            reason            getMissingReason()   const;

public:                   // Output support
            std::string       toString(TypeId const& typeId) const
                                {return s_defaultFormatter.format(typeId, *this);}
            std::string       toString(TypeId const& typeId, Formatter const& vf) const
                                {return vf.format(typeId, *this);}

            /**
             *  Helper class to stringify Value data according to an output format.
             */
            class Formatter
            {
            public:
                    /// Default precision for TID_DOUBLE values.
                    enum { DEFAULT_PRECISION = std::numeric_limits<double>::digits10 };

                    /// Constructor for default formatting.
                    Formatter();

                    /// Constructor for other formats.
                    Formatter(std::string const& format);

                    /// Given type and Value, return correctly formatted string.
                    std::string format(TypeId const& typeId, Value const& v) const;

                    /**
                     * Set precision for TID_DOUBLE values, returning previous precision.
                     * @param p precision to set, if < 0 restore default
                     * @return previous precision value
                     */
                    int setPrecision(int p);

            private:
                    int         _precision;
                    bool        _useDefaultNull;
                    bool        _tsv;
                    char        _quote;
                    std::string _format;
                    std::string _options;
                    std::string _nullRepr;
                    std::string _nanRepr;
                    void        quoteCstr(std::stringstream&, const char *) const;
            static  void        tsvChar(std::stringstream&, char);
            };
private:
            static  Formatter   s_defaultFormatter;

public:                   // Operations
    static  size_t            getFootprint(size_t);

    /**
     * Return true iff an integer value is an acceptable "missing reason" code.
     *
     * @description Although the code base uses various integral types
     * to store "missing reason", ultimately a valid code must conform
     * to the constraints imposed by our on-disk storage formats: it
     * must fit as a non-negative number within an @c int8_t .
     *
     * @note In many contexts a negative reason means "value is not
     * missing".  We explicitly do not permit them here because in
     * some contexts (for example, RLE segments) negative values are
     * never allowed.
     */
    template <typename T>
    static  bool              isValidMissingReason(T t)
    {
        if (std::numeric_limits<T>::is_signed) {
            return t >= 0 && t <= std::numeric_limits<int8_t>::max();
        } else {
            return t <= std::numeric_limits<int8_t>::max();
        }
    }

public:                   // Operations
            void              setNull    (reason = 0);
            void              setBool    (bool     v)    {set<bool>    (v);}
            void              setChar    (char     v)    {set<char>    (v);}
            void              setInt8    (int8_t   v)    {set<int8_t>  (v);}
            void              setInt16   (int16_t  v)    {set<int16_t> (v);}
            void              setInt32   (int32_t  v)    {set<int32_t> (v);}
            void              setInt64   (int64_t  v)    {set<int64_t> (v);}
            void              setUint8   (uint8_t  v)    {set<uint8_t> (v);}
            void              setUint16  (uint16_t v)    {set<uint16_t>(v);}
            void              setUint32  (uint32_t v)    {set<uint32_t>(v);}
            void              setUint64  (uint64_t v)    {set<uint64_t>(v);}
            void              setDateTime(uint64_t v)    {set<uint64_t>(v);}
            void              setFloat   (float    v)    {set<float>   (v);}
            void              setDouble  (double   v)    {set<double>  (v);}

            enum OldDataAction {
                IGNORE_DATA,
                KEEP_DATA
            };

            /// @brief Change the size of the data buffer.
            /// @param newSize  the new size of the buffer.
            /// @tparam oldDataAction  whether to ignore or keep the data previously stored in the buffer.
            /// @note In most cases, setSize() is called with oldDataAction=IGNORE_DATA.
            ///       When changing the API by adding this template parameter, we did not make IGNORE_DATA
            ///       the default behavior because there there may be legacy user code that relies on the
            ///       opposite behavior.
            template<OldDataAction> void* setSize(size_t newSize);

            void              setData    (const void*, size_t);
            void              setView    (const void*, size_t);
            void              setString  (const char*);
            void              setString  (const std::string&);
            void              clear      ();
            void              swap       (Value&);

public:                   // Operations
    template<class t>t const& get        ()        const;
    template<class t>t&       get        ();
    template<class t>void     set        (const t&);
    template<class t>void     reset      (const t&);
    template<class a>void     serialize  (a&,unsigned int);

public:                   // Operations
    template<class t>t const* getData    ()        const {return static_cast<t const*>(data());}
    template<class t>t*       getData    ()              {return static_cast<t*>      (data());}

private:                  // Implementation
            bool              consistent ()        const;// Assert consistency
            void*             resize     (size_t);       // Resize data buffer
            void              reset      ();             // Clear  data buffer

private:                  // Implementation
    static  bool              small      (size_t n)      {return n<=sizeof(_data);}
    static  bool              large      (size_t n)      {return n> sizeof(_data);}
    static  void*             calloc     (size_t);       // Wraps calloc()
    static  void*             malloc     (size_t);       // Wraps malloc()
    static  void              fail       (int);          // Throw exception

private:                  // Representation
            enum
            {
                MR_DATUM   = -1,                         // Represents a datum
                MR_TILE    = -2,                         // Represents a tile
                MR_VIEW    = -3,                         // Datum, but storage not owned
                MR_LAST    = -4                          // Must be last (for consistency checks)
            };

private:                  // Representation
            int32_t           _code;                     // >=0 for 'missing'
            size_type         _size;                     // Size of the buffer
    union { void*             _data;                     // The data buffer
            RLEPayload*       _tile; };                  // The tile pointer
};

/****************************************************************************/
} // namespace
/****************************************************************************/
#endif
/****************************************************************************/
