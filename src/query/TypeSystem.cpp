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

/****************************************************************************/

#include <query/TypeSystem.h>

#include <iomanip>
#include <cerrno>

#include <boost/tuple/tuple.hpp>

#include <log4cxx/logger.h>

#include <query/FunctionLibrary.h>
#include <util/Arena.h>

using namespace std;

/****************************************************************************/

namespace {

log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.typesystem"));

// No doubt there was once a reason for hiding $foo ?  Roman S. knew...
inline bool isHiddenTypeId(scidb::TypeId const& tid)
{
    return tid[0] == '$';
}

} // anon namespace

/****************************************************************************/
namespace scidb {
/****************************************************************************/

std::ostream& operator<<(std::ostream& os,const Type& t)
{
    return os << t.typeId();
}

/** PGB: Note that this will only generate the subset of the input list
 *     of types that are actually in the TypeLibrary.
 */
std::ostream& operator<<(std::ostream& os,const std::vector<TypeId>& r)
{
    for (size_t i=0,n=r.size(); i!=n; ++i)
    {
        if (i != 0)
        {
            os << ',';
        }

        os << ' ' << TypeLibrary::getType(r[i]);
    }

    return os;
}

std::ostream& operator<<(std::ostream& os,const std::vector<Type>& r)
{
    os << ' ';
    insertRange(os,r,", ");
    return os;
}

template<class K,class V,class C>
bool inFlatMap(PointerRange<const Keyed<K,V,C> > m,const K& k,V& v)
{
    const Keyed<K,V,C> *f,*l;                            // First+last matches

    boost::tie(f,l) = std::equal_range(m.begin(),m.end(),k);

    if (f != l)                                          // We found a match?
    {
        assert(l - f == 1);                              // ...match is unique
        v = f->value;
        return true;
    }

    return false;
}

TypeEnum typeId2TypeEnum(const TypeId& t,bool noThrow)
{
    static const Keyed<const char*,TypeEnum,less_strcmp> m[] =
    {
        {TID_BINARY    ,TE_BINARY    },
        {TID_BOOL      ,TE_BOOL      },
        {TID_CHAR      ,TE_CHAR      },
        {TID_DATETIME  ,TE_DATETIME  },
        {TID_DATETIMETZ,TE_DATETIMETZ},
        {TID_DOUBLE    ,TE_DOUBLE    },
        {TID_FLOAT     ,TE_FLOAT     },
        {TID_INDICATOR ,TE_INDICATOR },
        {TID_INT16     ,TE_INT16     },
        {TID_INT32     ,TE_INT32     },
        {TID_INT64     ,TE_INT64     },
        {TID_INT8      ,TE_INT8      },
        {TID_STRING    ,TE_STRING    },
        {TID_UINT16    ,TE_UINT16    },
        {TID_UINT32    ,TE_UINT32    },
        {TID_UINT64    ,TE_UINT64    },
        {TID_UINT8     ,TE_UINT8     },
        {TID_VOID      ,TE_VOID      },
    };

    TypeEnum e = TE_INVALID;

    if (inFlatMap(pointerRange(m),t.c_str(),e) || noThrow)
    {
        return e;
    }

    // Probably a user-defined type of some kind.  XXX We need to do a
    // better job of supporting those here!

    throw USER_EXCEPTION(SCIDB_SE_TYPE, SCIDB_LE_TYPE_NOT_REGISTERED) << t;
}

/**
 *  Return true if this supertype is base type for subtype.
 *  return true if subtype is direct or indirect subtype of supertype
 */
bool Type::isSubtype(TypeId const& subtype,TypeId const& supertype)
{
    return TypeLibrary::getType(subtype).isSubtypeOf(supertype);
}

std::ostream& operator<<(std::ostream& s,const Value& v)
{
    s << "scidb::Value(";

    switch (v.size())
    {
        case 1: s<<"0x"<<hex<<setfill('0')<<v.get<uint8_t> ()<< dec;break;
        case 2: s<<"0x"<<hex<<setfill('0')<<v.get<uint16_t>()<< dec;break;
        case 4: s<<"0x"<<hex<<setfill('0')<<v.get<uint32_t>()<< dec;break;
        case 8: s<<"0x"<<hex<<setfill('0')<<v.get<uint64_t>()<< dec;break;
        default:s<<"size="<< v.size()<<", data="<< v.data();        break;
    }

    if (v.isNull())
    {
        s << ", missingReason=" << v.getMissingReason();
    }

    return s << ')';
}

/**
 * TypeLibrary implementation
 */

TypeLibrary TypeLibrary::_instance;

TypeLibrary::TypeLibrary()
{
#if defined(SCIDB_CLIENT)
    registerBuiltInTypes();
#else
    // Initialization depends on the PluginManager.  In the client
    // PluginManager is stubbed out, but in the server we need it, and
    // it in turn needs the Configuration object.  But that isn't
    // available before main() runs.  We don't have it here when
    // initializing the file-scoped _instance.  Instead,
    // registerBuiltInTypes() is called explicitly in entry.cpp.
#endif
}

void TypeLibrary::registerBuiltInTypes()
{
    static struct builtin {const char* name;size_t bits;} const builtins[] =
    {
        {TID_INDICATOR,    1                     },
        {TID_CHAR,         8                     },
        {TID_INT8,         8                     },
        {TID_INT16,        16                    },
        {TID_INT32,        32                    },
        {TID_INT64,        64                    },
        {TID_UINT8,        8                     },
        {TID_UINT16,       16                    },
        {TID_UINT32,       32                    },
        {TID_UINT64,       64                    },
        {TID_FLOAT,        32                    },
        {TID_DOUBLE,       64                    },
        {TID_BOOL,         1                     },
        {TID_STRING,       0                     },
        {TID_DATETIME,     sizeof(time_t) * 8    },
        {TID_VOID,         0                     },
        {TID_BINARY,       0                     },
        {TID_DATETIMETZ,   2 * sizeof(time_t) * 8}
    };

    for (size_t i=0; i != SCIDB_SIZE(builtins); ++i)
    {
        const builtin& bti = builtins[i];

        Type t(bti.name, static_cast<uint32_t>(bti.bits));

        _instance._registerType(t);
        _instance._allDefaultValues[bti.name] = Value(t);

        // Access to these is not locked; fast(er) lookup for built-ins.
        _instance._builtinTypes[bti.name] = t;
        _instance._builtinDefaultValues[bti.name] = Value(t);
    }
}

bool TypeLibrary::_hasType(const TypeId& typeId) const
{
    if (_builtinTypes.find(typeId) != _builtinTypes.end())
    {
        return true;
    }

    ScopedMutexLock cs(_mutex, PTW_SML_TS);
    return _allTypes.find(typeId) != _allTypes.end();
}

const Type& TypeLibrary::_getType(const TypeId& typeId)
{
    auto const iter = _builtinTypes.find(typeId);
    if (iter != _builtinTypes.end())
    {
        return iter->second;
    }

    ScopedMutexLock cs(_mutex, PTW_SML_TS);
    return _getTypeUnlocked(typeId);
}

const Type& TypeLibrary::_getTypeUnlocked(const TypeId& typeId)
{
    auto iter = _allTypes.find(typeId);
    if (iter != _allTypes.end())
    {
        return iter->second;
    }

    size_t pos = typeId.find_first_of('_');
    if (pos != string::npos)
    {
        string genericTypeId = typeId.substr(0, pos + 1) + '*';
        iter = _allTypes.find(genericTypeId);
        if (iter != _allTypes.end())
        {
            Type limitedType(typeId, atoi(typeId.substr(pos + 1).c_str()) * 8,
                             iter->second.baseType());
            _typePlugins.addObject(typeId);
            return _allTypes[typeId] = limitedType;
        }
    }
    LOG4CXX_DEBUG(logger, "_getType('" << typeId << "') not found");
    throw SYSTEM_EXCEPTION(SCIDB_SE_TYPESYSTEM, SCIDB_LE_TYPE_NOT_REGISTERED)<< typeId;
}

std::vector<Type> TypeLibrary::getTypes(PointerRange<TypeId> ts)
{
    ScopedMutexLock cs(_instance._mutex, PTW_SML_TS);

    std::vector<Type> v;
    v.reserve(ts.size());
    for (auto const& tid : ts)
    {
        v.push_back(_instance._getTypeUnlocked(tid));
    }

    return v;
}

void TypeLibrary::_registerType(const Type& type)
{
    ScopedMutexLock cs(_mutex, PTW_SML_TS);
    auto const iter = _allTypes.find(type.typeId());
    if (iter == _allTypes.end()) {
        _allTypes[type.typeId()] = type;
        _typePlugins.addObject(type.typeId());
    } else {
        if (iter->second.bitSize() != type.bitSize() || iter->second.baseType() != type.baseType())  {
            throw SYSTEM_EXCEPTION(SCIDB_SE_TYPESYSTEM, SCIDB_LE_TYPE_ALREADY_REGISTERED) << type.typeId();
        }
    }
}

size_t TypeLibrary::_typesCount() const
{
    ScopedMutexLock cs(_mutex, PTW_SML_TS);
    size_t count = 0;
    for (auto const& entry : _allTypes) {
        if (!isHiddenTypeId(entry.first)) {
            ++count;
        }
    }
    return count;
}

std::vector<TypeId> TypeLibrary::_typeIds() const
{
    ScopedMutexLock cs(_mutex, PTW_SML_TS);
    std::vector<TypeId> v;
    v.reserve(_allTypes.size());

    for (auto const& entry : _allTypes) {
        if (!isHiddenTypeId(entry.first)) {
            v.push_back(entry.first);
        }
    }

    return v;
}

const Value& TypeLibrary::_getDefaultValue(const TypeId& typeId)
{
    auto iter = _builtinDefaultValues.find(typeId);
    if (iter != _builtinDefaultValues.end())
    {
        return iter->second;
    }

    ScopedMutexLock cs(_mutex, PTW_SML_TS);

    iter = _allDefaultValues.find(typeId);
    if (iter != _allDefaultValues.end())
    {
        return iter->second;
    }

    Value defaultValue(_getTypeUnlocked(typeId));

    FunctionDescription functionDesc;
    vector<FunctionPointer> converters;
    if (FunctionLibrary::getInstance()->findFunction(typeId, vector<TypeId>(), functionDesc, converters, false))
    {
        functionDesc.getFuncPtr()(0, &defaultValue, 0);
    }
    else
    {
        stringstream ss;
        ss << typeId << "()";
        throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_FUNCTION_NOT_FOUND) << ss.str();
    }

    // The default value of a user-defined type cannot be allocated from the load_library()'s
    // query arena, because it lives longer than the load_library() call.
    // So we temporarily remove the query arena from the thread-local storage.
    {
        arena::ScopedArenaTLS arenaTLS(nullptr);
        _allDefaultValues[typeId] = defaultValue;
    }

    return _allDefaultValues[typeId];
}


// No point in constructing one over and over.
Value::Formatter Value::s_defaultFormatter;

Value::Formatter::Formatter()
    : _precision(DEFAULT_PRECISION)
    , _useDefaultNull(true)
    , _tsv(false)
    , _quote('\'')
    , _format("dcsv")
    , _nullRepr("null")
    , _nanRepr("nan")
{ }

Value::Formatter::Formatter(string const& format)
    : _precision(DEFAULT_PRECISION)
    , _useDefaultNull(true)
    , _quote('\'')
    , _nullRepr("null")
    , _nanRepr("nan")
{
    // Break the format into a lowercase base format and a possibly
    // bUmPy cAsE option string.
    assert(!format.empty());
    string::size_type pos = format.find(':');
    if (pos == string::npos) {
        _format = format;
    } else {
        _format = format.substr(0, pos);
        _options = format.substr(pos + 1); // maintain upper/lower
    }
    transform(_format.begin(), _format.end(), _format.begin(), ::tolower);

    // Among other behaviors, TSV has its own default null representation.
    _tsv = (_format.find("tsv") != string::npos);
    if (_tsv) {
        _nullRepr = "\\N";
    }

    // Interpret options that force a particular null representation.
    pos = _options.find_first_of("EnN?");
    if (pos != string::npos) {
        _useDefaultNull = false;
        switch(_options[pos]) {
        case 'E':
            // Print null as empty string.
            _nullRepr = "";
            break;
        case 'n':
            // Print null as null (overrides TSV default).
            _nullRepr = "null";
            break;
        case 'N':
            // Print null as \N (Linear TSV).  Our TSV default.
            _nullRepr = "\\N";
            break;
        case '?':
            // Uniform printing of missing values.
            _nullRepr = "?0";
            break;
        }
    }

    // Interpret options that force a particular quote character.
    pos = _options.find_first_of("ds");
    if (pos != string::npos) {
        _quote = (_options[pos] == 'd' ? '"' : '\'');
    }
}


int Value::Formatter::setPrecision(int p)
{
    int prev = _precision;
    _precision = p < 0 ? DEFAULT_PRECISION : p;
    return prev;
}


/** Emit TSV representation of a character. */
void Value::Formatter::tsvChar(stringstream& ss, char ch)
{
    switch (ch) {
    case '\t':  ss << "\\t";    break;
    case '\n':  ss << "\\n";    break;
    case '\r':  ss << "\\r";    break;
    case '\\':  ss << "\\\\";   break;
    default:    ss << ch;      break;
    }
}

/**
 * Emit quoted CSV string.
 *
 * This isn't the correct way to quote a CSV string, but it's the way
 * we've historically been doing it... for CSV and for the "SciDB text"
 * family of formats.
 */
void Value::Formatter::quoteCstr(stringstream& ss, const char *s) const
{
    ss << _quote;
    while (char c = *s++) {
        if (c == _quote) {
            ss << '\\' << c;
        } else if (c == '\\') {
            ss << "\\\\";
        } else {
            ss << c;
        }
    }
    ss << _quote;
}

/**
 * Do format-based stringification of a value.
 *
 * @note This will only work efficiently for the built-in types. If you try to
 *       use this for a UDT it needs to do a lookup to try and find a UDF.
 */
string Value::Formatter::format(TypeId const& type, Value const& value) const
{
    static const string TSV_ESCAPED("\t\r\n\\");
    stringstream ss;

    /*
    ** Start with the most common ones, and do the least common ones
    ** last.
    */
    if ( value.isNull() ) {
        if (value.getMissingReason() == 0) {
            ss << _nullRepr;
        } else {
            // This needs to be cast to integer so that the character
            // value will not be output to ss.
            // e.g. ?34 not ?" (since ASCII 34 is the " character)
            ss << '?' << static_cast<int>(value.getMissingReason());
        }
    } else if ( TID_DOUBLE == type ) {
        double val = value.get<double>();
        if (std::isnan(val)) {
            ss << _nanRepr;
        }
        else {
            if (_precision > std::numeric_limits<double>::digits10) {
                ss.precision(std::numeric_limits<double>::digits10);
            } else {
                ss.precision(_precision);
            }
            ss << val;
        }
    } else if ( TID_INT64 == type ) {
        ss << value.get<int64_t>();
    } else if ( TID_INT32 == type ) {
        ss << value.get<int32_t>();
    } else if ( TID_STRING == type ) {
        char const* str = value.getString();
        if (str == NULL) {
            ss << _nullRepr;
        } else if (_tsv) {
            string raw(str);
            if (raw.find_first_of(TSV_ESCAPED) == string::npos) {
                ss << raw;
            } else {
                for (const char* cp = str; *cp; ++cp) {
                    tsvChar(ss, *cp);
                }
            }
        } else {
            // CSV and the SciDB text family of formats handle strings the same way.
            quoteCstr(ss, str);
        }
    } else if ( TID_CHAR == type ) {
        const char ch = value.get<char>();
        if (_tsv) {
            tsvChar(ss, ch);
        } else {
            ss << '\'';
            if (ch == '\0') {
                ss << "\\0";
            } else if (ch == '\n') {
                ss << "\\n";
            } else if (ch == '\r') {
                ss << "\\r";
            } else if (ch == '\t') {
                ss << "\\t";
            } else if (ch == '\f') {
                ss << "\\f";
            } else {
                if (ch == '\'' || ch == '\\') {
                    ss << '\\';
                }
                ss << ch;
            }
            ss << '\'';
        }
    } else if ( TID_FLOAT == type ) {
        float val = value.get<float>();
        if (std::isnan(val)) {
            ss << _nanRepr;
        }
        else {
            if (_precision > std::numeric_limits<float>::digits10) {
                ss.precision(std::numeric_limits<float>::digits10);
            } else {
                ss.precision(_precision);
            }
            ss << val;
        }
    } else if (( TID_BOOL == type ) || ( TID_INDICATOR == type )) {
        ss << boolalpha << value.get<bool>();
    } else if ( TID_DATETIME == type ) {

        char buf[STRFTIME_BUF_LEN];
        struct tm tm;
        time_t dt = value.getDateTime();

        gmtime_r(&dt, &tm);
        strftime(buf, sizeof(buf), DEFAULT_STRFTIME_FORMAT, &tm);
        if (_tsv) {
            ss << buf;
        } else {
            // No quote marks in a timestamp so no need for quoteCstr().
            ss << _quote << buf << _quote;
        }

    } else if ( TID_DATETIMETZ == type) {
        char buf[STRFTIME_BUF_LEN + 8];
        time_t *seconds = (time_t*) value.data();
        time_t *offset = seconds+1;

        struct tm tm;
        gmtime_r(seconds,&tm);
        size_t offs = strftime(buf, sizeof(buf), DEFAULT_STRFTIME_FORMAT, &tm);

        char sign = *offset > 0 ? '+' : '-';

        time_t aoffset = *offset > 0 ? *offset : (*offset) * -1;

        sprintf(buf+offs, " %c%02d:%02d",
                sign,
                (int32_t) aoffset/3600,
                (int32_t) (aoffset%3600)/60);
        if (_tsv) {
            ss << buf;
        } else {
            // No quote marks in a timestamp so no need for quoteCstr().
            ss << _quote << buf << _quote;
        }
    } else if ( TID_INT8 == type ) {
        ss << static_cast<int>(value.get<int8_t>());
    } else if ( TID_INT16 == type ) {
        ss << value.get<int16_t>();
    } else if ( TID_UINT8 == type ) {
        ss << static_cast<int>(value.get<uint8_t>());
    } else if ( TID_UINT16 == type ) {
        ss << value.get<uint16_t>();
    } else if ( TID_UINT32 == type ) {
        ss << value.get<uint32_t>();
    } else if ( TID_UINT64 == type ) {
        ss << value.get<uint64_t>();
    } else if ( TID_VOID == type ) {
        ss << "<void>";
    } else  {
        ss << "<" << type << ">";
    }

    return ss.str();
}

inline char mStringToMonth(const char* s)
{
    assert(s != 0);                                      // Validate arguments

    static const Keyed<const char*,char,less_strcasecmp> map[] =
    {
        {"apr", 4},
        {"aug", 8},
        {"dec",12},
        {"feb", 2},
        {"jan", 1},
        {"jul", 7},
        {"jun", 6},
        {"mar", 3},
        {"may", 5},
        {"nov",11},
        {"oct",10},
        {"sep", 9}
    };

    char n;

    if (inFlatMap(pointerRange(map),s,n))
    {
        return n;
    }

    throw USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION,SCIDB_LE_INVALID_MONTH_REPRESENTATION) << string(s);
}

/**
 * Parse a string that contains (hopefully) a DateTime constant into
 * the internal representation.
 * @param string containing DateTime value
 * @return standard time_t.
 */
time_t parseDateTime(std::string const& str)
{
    struct tm t;
    time_t now = time(NULL);
    if (str == "now") {
        return now;
    }
    gmtime_r(&now, &t);
    int n;
    int sec_frac;
    char const* s = str.c_str();
    t.tm_mon += 1;
    t.tm_hour = t.tm_min = t.tm_sec = 0;
    char mString[4]="";
    char amPmString[3]="";

    if (( sscanf(s, "%d-%3s-%d %d.%d.%d %2s%n", &t.tm_mday, &mString[0], &t.tm_year, &t.tm_hour, &t.tm_min, &t.tm_sec, &amPmString[0], &n) == 7 ||
          sscanf(s, "%d-%3s-%d %d.%d.%d%n",     &t.tm_mday, &mString[0], &t.tm_year, &t.tm_hour, &t.tm_min, &t.tm_sec, &n) == 6 ||
          sscanf(s, "%d-%3s-%d%n",              &t.tm_mday, &mString[0], &t.tm_year, &n) == 3 ||
          sscanf(s, "%d%3s%d:%d:%d:%d%n",       &t.tm_mday, &mString[0], &t.tm_year, &t.tm_hour, &t.tm_min, &t.tm_sec, &n) == 6 ) && n == (int) str.size())
    {
        t.tm_mon = mStringToMonth(mString);

        if(amPmString[0]=='P' && t.tm_hour < 12)
        {
            t.tm_hour += 12;
        }
    }
    else
    {
        if((sscanf(s, "%d/%d/%d %d:%d:%d%n",    &t.tm_mon, &t.tm_mday, &t.tm_year, &t.tm_hour, &t.tm_min, &t.tm_sec, &n) != 6 &&
            sscanf(s, "%d.%d.%d %d:%d:%d%n",    &t.tm_mday, &t.tm_mon, &t.tm_year, &t.tm_hour, &t.tm_min, &t.tm_sec, &n) != 6 &&
            sscanf(s, "%d-%d-%d %d:%d:%d.%d%n", &t.tm_year, &t.tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min, &t.tm_sec, &sec_frac, &n) != 7 &&
            sscanf(s, "%d-%d-%d %d.%d.%d.%d%n", &t.tm_year, &t.tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min, &t.tm_sec, &sec_frac, &n) != 7 &&
            sscanf(s, "%d-%d-%d %d.%d.%d%n",    &t.tm_year, &t.tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min, &t.tm_sec, &n) != 6 &&
            sscanf(s, "%d-%d-%d %d:%d:%d%n",    &t.tm_year, &t.tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min, &t.tm_sec, &n) != 6 &&
            sscanf(s, "%d/%d/%d %d:%d%n",       &t.tm_mon, &t.tm_mday, &t.tm_year, &t.tm_hour, &t.tm_min, &n) != 5 &&
            sscanf(s, "%d.%d.%d %d:%d%n",       &t.tm_mday, &t.tm_mon, &t.tm_year, &t.tm_hour, &t.tm_min, &n) != 5 &&
            sscanf(s, "%d-%d-%d %d:%d%n",       &t.tm_year, &t.tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min, &n) != 5 &&
            sscanf(s, "%d-%d-%d%n",             &t.tm_year, &t.tm_mon, &t.tm_mday, &n) != 3 &&
            sscanf(s, "%d/%d/%d%n",             &t.tm_mon, &t.tm_mday, &t.tm_year, &n) != 3 &&
            sscanf(s, "%d.%d.%d%n",             &t.tm_mday, &t.tm_mon, &t.tm_year, &n) != 3 &&
            sscanf(s, "%d:%d:%d%n",             &t.tm_hour, &t.tm_min, &t.tm_sec, &n) != 3 &&
            sscanf(s, "%d:%d%n",                &t.tm_hour, &t.tm_min, &n) != 2)
                    || n != (int)str.size())
            throw USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_FAILED_PARSE_STRING) << str << TID_DATETIME;
    }

    if (!(t.tm_mon >= 1 && t.tm_mon <= 12  && t.tm_mday >= 1 && t.tm_mday <= 31 && t.tm_hour >= 0
          && t.tm_hour <= 23 && t.tm_min >= 0 && t.tm_min <= 59 && t.tm_sec >= 0 && t.tm_sec <= 60))
        throw USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_INVALID_SPECIFIED_DATE);

    t.tm_mon -= 1;
    if (t.tm_year >= 1900) {
        t.tm_year -= 1900;
    } else if (t.tm_year < 100) {
        t.tm_year += 100;
    }
    return timegm(&t);
}

void parseDateTimeTz(std::string const& str, Value& result)
{
    if (str == "now")
    {
        pair<time_t,time_t> r;
        time_t now = time(NULL);
        struct tm localTm;
        localtime_r(&now, &localTm);
        r.second = timegm(&localTm) - now;
        r.first = now + r.second;
        result.setData(&r, 2*sizeof(time_t));
    }

    struct tm t;
    int offsetHours, offsetMinutes, secFrac, n;
    char mString[4]="";
    char amPmString[3]="";

    char const* s = str.c_str();
    t.tm_mon += 1;
    t.tm_hour = t.tm_min = t.tm_sec = 0;

    if ((sscanf(s, "%d-%3s-%d %d.%d.%d %2s %d:%d%n", &t.tm_mday, &mString[0], &t.tm_year, &t.tm_hour, &t.tm_min, &t.tm_sec, &amPmString[0], &offsetHours, &offsetMinutes, &n) == 9)
        && n == (int)str.size())
    {
        t.tm_mon = mStringToMonth(mString);
        if(amPmString[0]=='P')
        {
            t.tm_hour += 12;
        }
    }
    else
    {
        if((sscanf(s, "%d/%d/%d %d:%d:%d %d:%d%n", &t.tm_mon, &t.tm_mday, &t.tm_year, &t.tm_hour, &t.tm_min, &t.tm_sec, &offsetHours, &offsetMinutes, &n) != 8 &&
            sscanf(s, "%d.%d.%d %d:%d:%d %d:%d%n", &t.tm_mday, &t.tm_mon, &t.tm_year, &t.tm_hour, &t.tm_min, &t.tm_sec, &offsetHours, &offsetMinutes, &n) != 8 &&
            sscanf(s, "%d-%d-%d %d:%d:%d.%d %d:%d%n", &t.tm_year, &t.tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min, &t.tm_sec, &secFrac, &offsetHours, &offsetMinutes, &n) != 9 &&
            sscanf(s, "%d-%d-%d %d:%d:%d %d:%d%n", &t.tm_year, &t.tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min, &t.tm_sec, &offsetHours, &offsetMinutes, &n) != 8 &&
            sscanf(s, "%d-%d-%d %d.%d.%d.%d %d:%d%n", &t.tm_year, &t.tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min, &t.tm_sec, &secFrac, &offsetHours, &offsetMinutes, &n) != 9 &&
            sscanf(s, "%d-%d-%d %d.%d.%d %d:%d%n", &t.tm_year, &t.tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min, &t.tm_sec, &offsetHours, &offsetMinutes, &n) != 8 &&
            sscanf(s, "%d-%3s-%d %d.%d.%d %2s %d:%d%n", &t.tm_mday, &mString[0], &t.tm_year, &t.tm_hour, &t.tm_min, &t.tm_sec, &amPmString[0], &offsetHours, &offsetMinutes, &n) != 9)
              || n != (int)str.size())
            throw USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_FAILED_PARSE_STRING) << str << TID_DATETIMETZ;
    }

    if (offsetHours < 0 && offsetMinutes > 0)
    {
        offsetMinutes *= -1;
    }

    if (!(t.tm_mon >= 1 && t.tm_mon <= 12  &&
          t.tm_mday >= 1 && t.tm_mday <= 31 &&
          t.tm_hour >= 0 && t.tm_hour <= 23 &&
          t.tm_min >= 0 && t.tm_min <= 59 &&
          t.tm_sec >= 0 && t.tm_sec <= 60 &&
          offsetHours>=-13 && offsetHours<=13 &&
          offsetMinutes>=-59 && offsetMinutes<=59))
        throw USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_INVALID_SPECIFIED_DATE);

    t.tm_mon -= 1;
    if (t.tm_year >= 1900) {
        t.tm_year -= 1900;
    } else if (t.tm_year < 100) {
        t.tm_year += 100;
    }

    pair<time_t,time_t> r;
    r.first = timegm(&t);
    r.second = (offsetHours * 3600 + offsetMinutes * 60);
    result.setData(&r, 2*sizeof(time_t));
}

bool isBuiltinType(const TypeId& t)
{// use a flat map here
    return TID_DOUBLE == t
        || TID_INT64 == t
        || TID_INT32 == t
        || TID_CHAR == t
        || TID_STRING == t
        || TID_FLOAT == t
        || TID_INT8 == t
        || TID_INT16 == t
        || TID_UINT8 == t
        || TID_UINT16 == t
        || TID_UINT32 == t
        || TID_UINT64 == t
        || TID_INDICATOR == t
        || TID_BOOL == t
        || TID_DATETIME == t
        || TID_VOID == t
        || TID_DATETIMETZ == t
        || TID_BINARY == t;
}

TypeId propagateType(const TypeId& type)
{
    return TID_INT8 == type || TID_INT16 == type || TID_INT32 == type
    ? TID_INT64
    : TID_UINT8 == type || TID_UINT16 == type || TID_UINT32 == type
    ? TID_UINT64
    : TID_FLOAT == type ? TID_DOUBLE : type;
}

TypeId propagateTypeToReal(const TypeId& type)
{
    return TID_INT8 == type || TID_INT16 == type || TID_INT32 == type || TID_INT64 == type
        || TID_UINT8 == type || TID_UINT16 == type || TID_UINT32 == type || TID_UINT64 == type
        || TID_FLOAT == type ? TID_DOUBLE : type;
}

void StringToValue(const TypeId& type, const string& str, Value& value)
{
    if ( TID_DOUBLE == type ) {
        errno = 0;
        char *endptr = 0;
        double d = ::strtod(str.c_str(), &endptr);
        if (errno || !iswhitespace(endptr)) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_TYPE_CONVERSION_ERROR2)
                << str << "string" << "double" << (errno ? ::strerror(errno) : endptr);
        }
        value.setDouble(d);
    } else if ( TID_INT64 == type ) {
        int64_t val = StringToInteger<int64_t>(str.c_str(), TID_INT64);
        value.setInt64(val);
    } else if ( TID_INT32 == type ) {
        int32_t val = StringToInteger<int32_t>(str.c_str(), TID_INT32);
        value.setInt32(val);
    } else if (  TID_CHAR == type )  {
        value.setChar(str[0]);
    } else if ( TID_STRING == type ) {
        value.setString(str.c_str());
    } else if ( TID_FLOAT == type ) {
        errno = 0;
        char *endptr = 0;
        float f = ::strtof(str.c_str(), &endptr);
        if (errno || !iswhitespace(endptr)) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_TYPE_CONVERSION_ERROR2)
                << str << "string" << "float" << (errno ? ::strerror(errno) : endptr);
        }
        value.setFloat(f);
    } else if ( TID_INT8 == type ) {
        int8_t val = StringToInteger<int8_t>(str.c_str(), TID_INT8);
        value.setInt8(val);
    } else if (TID_INT16 == type) {
        int16_t val = StringToInteger<int16_t>(str.c_str(), TID_INT16);
        value.setInt16(val);
    } else if ( TID_UINT8 == type ) {
        uint8_t val = StringToInteger<uint8_t>(str.c_str(), TID_UINT8);
        value.setUint8(val);
    } else if ( TID_UINT16 == type ) {
        uint16_t val = StringToInteger<uint16_t>(str.c_str(), TID_UINT16);
        value.setUint16(val);
    } else if ( TID_UINT32 == type ) {
        uint32_t val = StringToInteger<uint32_t>(str.c_str(), TID_UINT32);
        value.setUint32(val);
    } else if ( TID_UINT64 == type ) {
        uint64_t val = StringToInteger<uint64_t>(str.c_str(), TID_UINT64);
        value.setUint64(val);
    } else if (( TID_INDICATOR == type ) || ( TID_BOOL == type )) {
        if (str == "true") {
            value.setBool(true);
        } else if (str == "false") {
            value.setBool(false);
        } else {
            throw SYSTEM_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_TYPE_CONVERSION_ERROR2)
                << str << "string" << "bool" << "Unrecognized token";
        }
    } else if ( TID_DATETIME == type ) {
        value.setDateTime(parseDateTime(str));
    } else if ( TID_DATETIMETZ == type) {
        parseDateTimeTz(str, value);
    } else if ( TID_VOID == type ) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_TYPE_CONVERSION_ERROR2)
            << str << "string" << type << "Type 'void' is not parseable";
    } else {
        std::stringstream ss;
        ss << type;
        throw SYSTEM_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_TYPE_CONVERSION_ERROR2)
            << str << "string" << type << "Unrecognized type";
    }
}

double ValueToDouble(const TypeId& type, const Value& value)
{
    std::stringstream ss;
    if ( TID_DOUBLE == type ) {
        return value.getDouble();
    } else if ( TID_INT64 == type ) {
        return static_cast<double>(value.getInt64());
    } else if ( TID_INT32 == type ) {
        return static_cast<double>(value.getInt32());
    } else if ( TID_CHAR == type ) {
        return static_cast<double>(value.getChar());
    } else if ( TID_STRING == type ) {
        double d;
        int n;
        char const* str = value.getString();
        if (sscanf(str, "%lf%n", &d, &n) != 1 || n != (int)strlen(str))
            throw USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_FAILED_PARSE_STRING) << str << "double";
        return d;
    } else if ( TID_FLOAT == type ) {
        return value.getFloat();
    } else if ( TID_INT8 == type ) {
        return value.getInt8();
    } else if ( TID_INT16 == type ) {
        return value.getInt16();
    } else if ( TID_UINT8 == type ) {
        return value.getUint8();
    } else if ( TID_UINT16 == type ) {
        return value.getUint16();
    } else if ( TID_UINT32 == type ) {
        return value.getUint32();
    } else if ( TID_UINT64 == type ) {
        return static_cast<double>(value.getUint64());
    } else if (( TID_INDICATOR == type ) || ( TID_BOOL == type )) {
        return value.getBool();
    } else if ( TID_DATETIME == type ) {
        return static_cast<double>(value.getDateTime());
    } else {
        throw SYSTEM_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_TYPE_CONVERSION_ERROR)
            << type << "double";
    }
}

void DoubleToValue(const TypeId& type, double d, Value& value)
{
      if (  TID_DOUBLE == type ) {
        value.setDouble(d);
      } else if ( TID_INT64 == type ) {
        value.setInt64((int64_t)d);
      } else if ( TID_UINT32 == type ) {
        value.setUint32((uint32_t)d);
      } else if ( TID_CHAR == type ) {
        value.setChar((char)d);
      } else if ( TID_FLOAT == type ) {
        value.setFloat((float)d);
      } else if ( TID_INT8 == type ) {
        value.setInt8((int8_t)d);
      } else if ( TID_INT16 == type ) {
        value.setInt32((int32_t)d);
      } else if ( TID_UINT8 == type ) {
        value.setUint8((uint8_t)d);
      } else if ( TID_UINT16 == type ) {
        value.setUint16((uint16_t)d);
      } else if ( TID_UINT64 == type ) {
        value.setUint64((uint64_t)d);
      } else if (( TID_INDICATOR == type ) || ( TID_BOOL == type )) {
        value.setBool(d != 0.0);
      } else if ( TID_STRING == type ) {
          std::stringstream ss;
          ss << d;
          value.setString(ss.str().c_str());
      } else if (  TID_DATETIME == type ) {
        return value.setDateTime((time_t)d);
    } else {
        throw SYSTEM_EXCEPTION(SCIDB_SE_TYPE_CONVERSION, SCIDB_LE_TYPE_CONVERSION_ERROR)
            << "double" << type;
    }
}

template<>  TypeId type2TypeId<char>()      {return TID_CHAR;  }
template<>  TypeId type2TypeId<int8_t>()    {return TID_INT8;  }
template<>  TypeId type2TypeId<int16_t>()   {return TID_INT16; }
template<>  TypeId type2TypeId<int32_t>()   {return TID_INT32; }
template<>  TypeId type2TypeId<int64_t>()   {return TID_INT64; }
template<>  TypeId type2TypeId<uint8_t>()   {return TID_UINT8; }
template<>  TypeId type2TypeId<uint16_t>()  {return TID_UINT16;}
template<>  TypeId type2TypeId<uint32_t>()  {return TID_UINT32;}
template<>  TypeId type2TypeId<uint64_t>()  {return TID_UINT64;}
template<>  TypeId type2TypeId<float>()     {return TID_FLOAT; }
template<>  TypeId type2TypeId<double>()    {return TID_DOUBLE;}

/****************************************************************************/

Value makeTileConstant(const TypeId& tid, const Value& v, size_t count)
{
    Type                t(TypeLibrary::getType(tid));
    Value               w(t,Value::asTile);
    RLEPayload*   const p = w.getTile();
    RLEPayload::Segment s(0,0,true,v.isNull());

    if (!s.null())
    {
        std::vector<char> varPart;

        p->appendValue(varPart,v,0);
        if (!varPart.empty()) {
            ASSERT_EXCEPTION(t.variableSize(), "Setting empty varpart?");
            p->setVarPart(varPart);
        } else {
            ASSERT_EXCEPTION(!t.variableSize(), "Setting varpart of fixed-size attribute tile?");
        }
    }

    p->addSegment(s);

    if (count) {
        p->flush(count);
    } else {
        // By calling flush with the max length we are making this a
        // special case tile that can be flagged by the length of the tile.
        //
        // Special case tiles are created when a binary op is used
        // Example:  apply(A, a + 3)
        // The 3 part will be a special case tile with the max length set.
        p->flush(CoordinateBounds::getMaxLength());
    }

    return w;
}

/**
 *  Throw a type system exception with code 'e'.
 */
void Value::fail(int e)
{
    throw SYSTEM_EXCEPTION(SCIDB_SE_TYPESYSTEM,e);       // Throw exception e
}

/**
 *  Return true if the object looks to be in good shape.  Centralizes a number
 *  of consistency checks that would otherwise clutter up the code, and, since
 *  only ever called from within assertions, can be eliminated entirely by the
 *  compiler from the release build.
 */
bool Value::consistent() const
{
    assert(_code > MR_LAST);                             // Check status code
    assert(_code <= std::numeric_limits<int8_t>::max()); // ...carefully
    // _code must be either MR_xxx (e.g. MR_DATUM) or in the range [0, 127]
    assert(implies(large(_size), _data!=0));             // Check data buffer
    assert(implies(_size==0,_data==0 || isTile()));      // Check buffer size
    assert(implies(isTile(),_tile!=0));                  // Check tile pointer
    assert(implies(isTile(),_size==0));                  // Check buffer size
    assert(implies(isView(),large(_size)));              // No small views

    return true;                                         // Appears to be good
}


/**
 * Shorthand for throwing USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION,SCIDB_LE_FAILED_PARSE_STRING)
 * @param s string being parsed
 * @param tid TypeId of desired destination type
 * @param what additional error information
 * @throws USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION,SCIDB_LE_FAILED_PARSE_STRING)
 * @returns never returns, always throws
 */
static void throwFailedParseEx(const char *s, const TypeId& tid, const char* what)
{
    stringstream ss;
    ss << tid << " (" << what << ')';
    throw USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION,
                         SCIDB_LE_FAILED_PARSE_STRING) << s << ss.str();
}

/**
 * @defgroup Templates to yield correct strtoimax(3)/strtoumax(3) return type.
 * @{
 */
template <bool isSigned>
struct maxint;

template <>
struct maxint<true> { typedef intmax_t type; };

template <>
struct maxint<false> { typedef uintmax_t type; };
/**@}*/

/**
 * Convert string to integral type T, backward compatibly with sscanf(3).
 *
 * @description We know we have an integer type here, so sscanf(3) is
 * overkill: we can call strtoimax(3)/strtoumax(3) with less overhead.
 * Also disallow octal input: the string is either base 10 or (with
 * leading 0x or 0X) base 16.
 *
 * @param s null-terminated string
 * @param tid TypeId used to generate exception messages
 * @return the T integer value parsed from the string @c s
 * @throws USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION,SCIDB_LE_FAILED_PARSE_STRING)
 *
 * @note The "non-digits" check is backward compatible with the old
 * sscanf(3) implementation.  "42hike" is not an integer.
 */
template <typename T>
T StringToInteger(const char *s, const TypeId& tid)
{
    ASSERT_EXCEPTION(s, "StringToInteger<> was given NULL string pointer");

    // FWIW "VMIN" is used by termio headers, so: V_MIN, V_MAX.
    typedef typename maxint< std::numeric_limits<T>::is_signed >::type MaxT;
    MaxT const V_MAX = std::numeric_limits<T>::max();
    MaxT const V_MIN = std::numeric_limits<T>::min();

    // For signed values, this is "right-sized" all-ones.
    MaxT const MASK = (V_MAX << 1) | 1;

    while (::isspace(*s))
        ++s;
    char next = *s ? *(s + 1) : '\0';
    int base = (*s == '0' && (next == 'x' || next == 'X')) ? 16 : 10;

    errno = 0;
    char *endptr = 0;
    MaxT v;
    if (std::numeric_limits<T>::is_signed) {
        v = ::strtoimax(s, &endptr, base);
    } else {
        v = ::strtoumax(s, &endptr, base);
    }
    if (errno) {
        throwFailedParseEx(s, tid, ::strerror(errno));
    }
    if (v > V_MAX) {
        if (!std::numeric_limits<T>::is_signed)
            throwFailedParseEx(s, tid, "unsigned overflow");
        if (base == 10 || (v & ~MASK))
            throwFailedParseEx(s, tid, "signed overflow");
        // Else it's a negative number entered as hex, and only *looks* > V_MAX.
        // Sign-extend it so that the cast will do the right thing.
        v |= ~MASK;
    }
    if (std::numeric_limits<T>::is_signed && v < V_MIN) {
        throwFailedParseEx(s, tid, "signed underflow");
    }
    // Allow trailing whitespace, but nothing else.  [csv2scidb compat]
    if (!iswhitespace(endptr)) {
        throwFailedParseEx(s, tid, "non-digits");
    }
    return static_cast<T>(v);
}

// Explicit instantiations for our favorite types.
template int8_t StringToInteger<int8_t>(const char *s, const TypeId& tid);
template uint8_t StringToInteger<uint8_t>(const char *s, const TypeId& tid);
template int16_t StringToInteger<int16_t>(const char *s, const TypeId& tid);
template uint16_t StringToInteger<uint16_t>(const char *s, const TypeId& tid);
template int32_t StringToInteger<int32_t>(const char *s, const TypeId& tid);
template uint32_t StringToInteger<uint32_t>(const char *s, const TypeId& tid);
template int64_t StringToInteger<int64_t>(const char *s, const TypeId& tid);
template uint64_t StringToInteger<uint64_t>(const char *s, const TypeId& tid);

/****************************************************************************/
}
/****************************************************************************/
