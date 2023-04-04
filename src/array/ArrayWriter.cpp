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

#define __EXTENSIONS__
#define _EXTENSIONS
#define _FILE_OFFSET_BITS 64
#if ! defined(HPUX11_NOT_ITANIUM) && ! defined(L64)
#  define _LARGEFILE64_SOURCE 1 // access to files greater than 2Gb in Solaris
#  define _LARGE_FILE_API     1 // access to files greater than 2Gb in AIX
#endif

#include <array/ArrayWriter.h>

#include <util/FileIO.h>

#include <stdio.h>
#include <ctype.h>
#include <inttypes.h>
#include <limits.h>
#include <float.h>
#include <string>
#include <errno.h>

#include <boost/archive/text_oarchive.hpp>
#include <log4cxx/logger.h>
#include <log4cxx/basicconfigurator.h>
#include <log4cxx/helpers/exception.h>

#include <array/ArrayDesc.h>
#include <array/ConstChunk.h>
#include <array/PinBuffer.h>
#include <array/TemplateParser.h>
#include <system/Exceptions.h>
#include <query/TypeSystem.h>
#include <query/FunctionDescription.h>
#include <query/FunctionLibrary.h>
#include <query/Query.h>
#include <system/SystemCatalog.h>
#include <system/Warnings.h>
#include <util/FileIO.h>

using namespace std;
using boost::archive::text_oarchive;

namespace scidb
{
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.smgr.io.ArrayWriter"));

    static inline void throwWriteError(int e)
    {
        throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR)
            << ::strerror(e) << e;
    }

#   define Fputc(_c, _fp)                       \
    do {                                        \
        int e = ::fputc_unlocked(_c, _fp);      \
        if (e == EOF) {                         \
            e = errno ? errno : EIO;            \
            throwWriteError(e);                 \
        }                                       \
    } while (0)

#   define Fputs(_s, _fp)                       \
    do {                                        \
        int e = ::fputs_unlocked(_s, _fp);      \
        if (e == EOF) {                         \
            e = errno ? errno : EIO;            \
            throwWriteError(e);                 \
        }                                       \
    } while (0)

#   define Fprintf(_fp,  _vargs...)             \
    do {                                        \
        int e = scidb::fprintf(_fp, _vargs);    \
        if (e < 0) {                            \
            e = errno ? errno : EIO;            \
            throwWriteError(e);                 \
        }                                       \
    } while (0)

    int ArrayWriter::_precision = ArrayWriter::DEFAULT_PRECISION;

    /*
     * The *apparent* purpose of this iterator wrapper is to never
     * skip over default values.  One could wish the original author
     * had left some documentation.
     *
     * - Currently used only by dcsv and text formats.
     * - Without it, we cannot load 'store' output as 'text'.
     * - Also, various test failures occur:
     *   + These fail with FILES_DIFFER:
     *     . checkin.other.unbounded_cast_002 (uses 'dense')
     *     . docscript.aggregate_op (uses 'dense')
     *   + These fail due to assertion failures in saveTextFormat()
     *     that cause scidbtestharness to exit, aborting the test run:
     *     . docscript.analytics_aggregates
     *     . p4/nova.03_filter
     *   + Crashed the coordinator:
     *     . p4/checkin.la_pearson2
     *
     * So I guess we need to keep it, despite the name's implied
     * suggestion that it could go away one day.
     */
    class CompatibilityIterator : public ConstChunkIterator
    {
        Coordinates currPos;
        std::shared_ptr<ConstChunkIterator> inputIterator;
        Coordinates const& firstPos;
        Coordinates const& lastPos;
        Coordinates const* nextPos;
        bool hasCurrent;
        Value defaultValue;
        int mode;
        bool isEmptyable;
        bool ignoreEmptyCells;

      public:
        CompatibilityIterator(std::shared_ptr<ConstChunkIterator> iterator, bool isSparse)
        : inputIterator(iterator),
          firstPos(iterator->getFirstPosition()),
          lastPos(iterator->getLastPosition()),
          defaultValue(iterator->getChunk().getAttributeDesc().getDefaultValue()),
          mode(iterator->getMode()),
          isEmptyable(iterator->getChunk().getArrayDesc().getEmptyBitmapAttribute() != NULL),
          ignoreEmptyCells(isSparse)
        {
            mode &= ~ConstChunkIterator::IGNORE_DEFAULT_VALUES;
            restart();
        }

        bool skipDefaultValue() {
            return false;
        }

        int getMode() const override {
            return inputIterator->getMode();
        }

        Value const& getItem() override {
            if (!hasCurrent)
                throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_NO_CURRENT_ELEMENT);
            return (nextPos == NULL || currPos != *nextPos) ? defaultValue : inputIterator->getItem();
        }

        bool isEmpty() const override {
            if (!hasCurrent)
                throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_NO_CURRENT_ELEMENT);
            return isEmptyable && (nextPos == NULL || currPos != *nextPos);
        }

        bool end() override {
            return !hasCurrent;
        }

        void operator ++() override {
            if (!hasCurrent)
                throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_NO_CURRENT_ELEMENT);

            do {
                if (ignoreEmptyCells) {
                    ++(*inputIterator);
                    if (inputIterator->end()) {
                        hasCurrent = false;
                        return;
                    }
                    nextPos = &inputIterator->getPosition();
                    currPos = *nextPos;
                } else {
                    if (nextPos != NULL && currPos == *nextPos) {
                        ++(*inputIterator);
                        nextPos = inputIterator->end() ? NULL : &inputIterator->getPosition();
                    }
                    size_t i = currPos.size()-1;
                    while (++currPos[i] > lastPos[i]) {
                        if (i == 0) {
                            hasCurrent = false;
                            return;
                        }
                        currPos[i] = firstPos[i];
                        i -= 1;
                    }
                }
            } while (skipDefaultValue());
        }

        Coordinates const& getPosition() override {
            return currPos;
        }

        bool setPosition(Coordinates const& pos) override {
            throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_NOT_IMPLEMENTED)
                << "CompatibilityIterator::setPosition";
        }

        void restart() override {
            inputIterator->restart();
            nextPos = inputIterator->end() ? NULL : &inputIterator->getPosition();
            hasCurrent = nextPos != NULL || !ignoreEmptyCells;
            currPos = ignoreEmptyCells && nextPos ? *nextPos : firstPos;
            if (hasCurrent && skipDefaultValue()) {
                ++(*this);
            }
        }

        ConstChunk const& getChunk() override {
            return inputIterator->getChunk();
        }
    };

    static void checkStreamError(FILE *f, char const* where)
    {
        int rc = ferror(f);
        if (rc) {
            throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR)
                << "Error flag set on FILE handle" << where;
        }
    }

    /**
     * Parameters and utility methods for "foo-separated values" formats.
     */
    class XsvParms {
    public:

        /** For historical reasons, use single-quote for CSV quoted strings. */
        static const char DEFAULT_CSV_QUOTE = '\'';

        /**
         * The default XsvParms object corresponds to 'csv'.
         *
         * The options string is derived from the SAVE operator's format parameter, whose syntax is
         *
         *     BASE_FORMAT [ : [ OPTIONS ] ]
         *
         * The XsvParms object provides different behaviors for printing of nulls depending on
         * the presence of various single characters in the options string.  The comments in the
         * switch statement describe the current possibilities.
         *
         * @param options string containing single character option designators
         *
         * @see wiki:Development/components/CsvTsvFormatOptions
         */
        XsvParms(const string& format)
            : _delim(',')
            , _pretty(false)
            , _wantCoords(false)
            , _compatMode(false)
            , _parallel(false)
            , _wantLabels(false)
            , _formatter(format)
        {
            string::size_type colon = format.find(':');
            if (colon != string::npos) {
                _wantLabels =
                    (format.substr(colon + 1).find('l') != string::npos);
            }
        }

        XsvParms& setDelim(char ch) {
            _delim = ch;
            return *this;
        }
        XsvParms& setPretty(bool b) {
            _pretty = b;
            return *this;
        }
        XsvParms& setCoords(bool b) {
            _wantCoords = b;
            return *this;
        }
        XsvParms& setCompat(bool b) {
            _compatMode = b;
            return *this;
        }
        XsvParms& setParallel(bool b) {
            _parallel = b;
            return *this;
        }
        XsvParms& setLabels(bool b) {
            _wantLabels = b;
            return *this;
        }
        XsvParms& setPrecision(int p) {
            (void) _formatter.setPrecision(p);
            return *this;
        }

        char delim() const { return _delim; }
        bool pretty() const { return _pretty; }
        bool wantCoords() const { return _wantCoords; }
        bool compatMode() const { return _compatMode; }
        bool parallelSave() const { return _parallel; }
        bool wantLabels() const { return _wantLabels; }
        Value::Formatter const& getFormatter() const { return _formatter; }

        /**
         * Encoding for TSV string fields.
         * @see http://dataprotocols.org/linear-tsv/
         */
        string encodeString(const char *s) const;

    private:
        char _delim;
        bool _pretty;
        bool _wantCoords;
        bool _compatMode;
        bool _parallel;
        bool _wantLabels;
        Value::Formatter _formatter;
    };

    const char XsvParms::DEFAULT_CSV_QUOTE;

    static void s_fprintValue(FILE *f,
                              const Value* v,
                              TypeId const& valueType,
                              FunctionPointer const converter,
                              Value::Formatter const& vf)
    {
        static const TypeId STRING_TYPE_ID(TID_STRING);

        Value strValue;
        TypeId const* tidp = &valueType;
        if (converter) {
            (*converter)(&v, &strValue, NULL);
            // Pretend we were working on the strValue string all along!
            v = &strValue;
            tidp = &STRING_TYPE_ID;
        }

        Fprintf(f, "%s", v->toString(*tidp, vf).c_str());
    }

    static void s_fprintCoordinate(FILE *f,
                                   Coordinate ordinalCoord)
    {
        Fprintf(f, "%" PRIi64, ordinalCoord);
    }

    static void s_fprintCoordinates(FILE *f,
                                    Coordinates const& coords)
    {
        Fputc('{', f);
        for (size_t i = 0; i < coords.size(); i++)
        {
            if (i != 0)
            {
                Fputc(',', f);
            }
            s_fprintCoordinate(f, coords[i]);
        }
        Fputc('}', f);
    }

    static void printLabels(FILE* f,
                            Dimensions const& dims,
                            Attributes const& attrs,
                            XsvParms const& parms)
    {
        // Dimensions first.
        if (parms.wantCoords()) {
            if (parms.pretty())
                Fputc('{', f);
            for (unsigned i = 0; i < dims.size(); ++i) {
                if (i) {
                    Fputc(parms.delim(), f);
                }
                Fprintf(f, "%s", dims[i].getBaseName().c_str());
            }
            if (parms.pretty()) {
                Fputs("} ", f);
            } else {
                Fputc(parms.delim(), f);
            }
        }

        // Then attributes.
        for (auto attr = attrs.begin(); attr != attrs.end(); ++attr) {
            if (attr != attrs.begin()) {
                Fputc(parms.delim(), f);
            }
            Fprintf(f, "%s", attr->getName().c_str());
        }
        Fputc('\n', f);
    }

    /**
     * @brief Single code path for "foo-separated values" formats.
     *
     * @description The handling of some text formats is remarkably
     * similar, and can be parameterized via an @c XsvParms object.
     * Currently supported formats are csv, csv+, tsv, tsv+, and dcsv.
     */
    static uint64_t saveXsvFormat(Array const& array,
                                  ArrayDesc const& desc,
                                  FILE* f,
                                  const XsvParms& parms,
                                  std::shared_ptr<Query> const& query)
    {
        // No attributes, no problem.
        Attributes const& attrs = desc.getAttributes(true);
        size_t numAttrs = attrs.size();
        if (numAttrs == 0) {
            checkStreamError(f, "No attributes");
            return 0;
        }

        // Gather various per-attribute items.
        vector<std::shared_ptr<ConstArrayIterator> > arrayIterators(numAttrs);
        vector<FunctionPointer>                 converters(numAttrs);
        vector<TypeId>                          types(numAttrs);

        for (const auto& attr : attrs) {
            arrayIterators[attr.getId()] = array.getConstIterator(attr);
            types[attr.getId()] = attr.getType();
            if (!isBuiltinType(types[attr.getId()])) {
                converters[attr.getId()] = FunctionLibrary::getInstance()->findConverter(
                    types[attr.getId()],
                    TID_STRING,
                    false);
            }
        }

        if (parms.wantLabels()) {
            if (parms.parallelSave()) {
                // Can't parallel-load labels, so don't save them in the first place.
#ifndef SCIDB_CLIENT
                query->postWarning(SCIDB_WARNING(SCIDB_LE_CANNOT_RELOAD_LABELS_IN_PARALLEL));
#endif
                LOG4CXX_WARN(logger, USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER,
                                                    SCIDB_LE_CANNOT_RELOAD_LABELS_IN_PARALLEL).what());
            } else {
                printLabels(f, desc.getDimensions(), attrs, parms);
            }
        }

        // Time to walk the chunks!
        uint64_t count = 0;
        vector<std::shared_ptr<ConstChunkIterator> > chunkIterators(numAttrs);
        const int CHUNK_MODE =
            ConstChunkIterator::IGNORE_OVERLAPS;
        const auto& fda = attrs.firstDataAttribute();
        while (!arrayIterators[fda.getId()]->end()) {

            // Set up chunk iterators, one per attribute.
            for (const auto& attr : attrs) {
                ConstChunk const& chunk = arrayIterators[attr.getId()]->getChunk();
                chunkIterators[attr.getId()] = chunk.getConstIterator(CHUNK_MODE);
            }

            // Print these chunks...
            while (!chunkIterators[attrs.firstDataAttribute().getId()]->end()) {

                // Coordinates, anyone?
                if (parms.wantCoords()) {
                    Coordinates const& pos =
                        chunkIterators[attrs.firstDataAttribute().getId()]->getPosition();
                    if (parms.pretty())
                        Fputc('{', f);
                    for (unsigned i = 0; i < pos.size(); ++i) {
                        if (i) {
                            Fputc(parms.delim(), f);
                        }
                        Fprintf(f, "%" PRIi64, pos[i]);
                    }
                    if (parms.pretty()) {
                        Fputs("} ", f);
                    } else {
                        Fputc(parms.delim(), f);
                    }
                }

                // Then come the attributes.  Bump their corresponding
                // chunk iterators as we go.
                for (const auto& attr : attrs) {
                    if (attr.getId() > fda.getId()) {
                        Fputc(parms.delim(), f);
                    }
                    s_fprintValue(f,
                                  &chunkIterators[attr.getId()]->getItem(),
                                  types[attr.getId()],
                                  converters[attr.getId()],
                                  parms.getFormatter());
                    ++(*chunkIterators[attr.getId()]);
                }

                // Another array cell for peace!
                count += 1;
                Fputc('\n', f);
            }

            // Bump the array iterators to get the next set of chunks.
            for (const auto& attr : attrs) {
                ++(*arrayIterators[attr.getId()]);
            }
        }

        checkStreamError(f, __FUNCTION__);
        return count;
    }

    /**
     * This code handles sparse, dense, store, and text formats.
     */
    static uint64_t saveTextFormat(Array const& array,
                                   ArrayDesc const& desc,
                                   FILE* f,
                                   std::string const& format)
    {
        uint64_t n = 0;
        int precision = ArrayWriter::getPrecision();
        Attributes const& attrs = desc.getAttributes(true);
        //If descriptor has empty flag we just ignore it and fill only iterators with actual data attributes
        size_t attrsCount = attrs.size();
        if (attrsCount == 0)
        {
            checkStreamError(f, __FUNCTION__);
            return n;
        }

        Dimensions const& dims = desc.getDimensions();
        const size_t nDimensions = dims.size();
        assert(nDimensions > 0);
        vector< std::shared_ptr<ConstArrayIterator> > arrayIterators(attrsCount); // 按照属性来迭代数组
        vector< std::shared_ptr<ConstChunkIterator> > chunkIterators(attrsCount); // 迭代chunk，大小也是用属性的大小来的，因为chunk至少有属性个
        vector< TypeId> types(attrsCount);
        vector< FunctionPointer> converters(attrsCount);
        Coordinates coord(nDimensions);
        int iterationMode = ConstChunkIterator::IGNORE_OVERLAPS;

        // Get array iterators for all attributes
        for (const auto& attr : attrs)
        {
            arrayIterators[attr.getId()] = array.getConstIterator(attr);
            types[attr.getId()] = attr.getType();
            if (!isBuiltinType(types[attr.getId()])) {
                converters[attr.getId()] =
                    FunctionLibrary::getInstance()->findConverter(types[attr.getId()], TID_STRING, false);
            }
        }

        bool sparseFormat = compareStringsIgnoreCase(format, "sparse") == 0;
        bool denseFormat = compareStringsIgnoreCase(format, "dense") == 0;
        bool storeFormat = compareStringsIgnoreCase(format, "store") == 0;
        bool autoFormat = compareStringsIgnoreCase(format, "text") == 0;

        bool startOfArray = true;
        if (storeFormat) {
            // We want what's really stored: *all* the floating point digits, overlap regions.
            if (precision < DBL_DIG) {
                precision = DBL_DIG;
            }
            iterationMode &= ~ConstChunkIterator::IGNORE_OVERLAPS;
        }

        Value::Formatter textFormatter;
        textFormatter.setPrecision(precision);

        // Set initial position
        Coordinates chunkPos(nDimensions);
        for (size_t i = 0; i < nDimensions; i++) {
            coord[i] = dims[i].getStartMin();
            chunkPos[i] = dims[i].getStartMin();
        }

        // Check if chunking is performed in more than one dimension
        bool multisplit = false;
        for (size_t i = 1; i < nDimensions; i++) {
            if (dims[i].getChunkInterval() < static_cast<int64_t>(dims[i].getLength())) {
                multisplit = true;
            }
        }

        coord[nDimensions-1] -= 1; // to simplify increment
        chunkPos[nDimensions-1] -= dims[nDimensions-1].getChunkInterval();
        {
            // Iterate over all chunks // 迭代所有chunk
            bool firstItem = true;
            while (!arrayIterators[attrs.firstDataAttribute().getId()]->end()) {
                // Get iterators for the current chunk
                bool isSparse = false;
                for (const auto& attr : attrs) {
                    ConstChunk const& chunk = arrayIterators[attr.getId()]->getChunk();
                    chunkIterators[attr.getId()] = chunk.getConstIterator(iterationMode);
                    if (attr.getId() == attrs.firstDataAttribute().getId()) {
                        isSparse = !denseFormat &&
                            (autoFormat && chunk.count()*100/chunk.getNumberOfElements(false) <= 10);
                    }
                    chunkIterators[attr.getId()] = std::shared_ptr<ConstChunkIterator>(
                         new CompatibilityIterator(chunkIterators[attr.getId()],
                                                   isSparse || sparseFormat));
                }
                int j = safe_static_cast<int>(nDimensions);
                while (--j >= 0 && (chunkPos[j] += dims[j].getChunkInterval()) > dims[j].getEndMax()) {
                    chunkPos[j] = dims[j].getStartMin();
                }
                bool gap =
                    !storeFormat && (sparseFormat ||
                                     arrayIterators[attrs.firstDataAttribute().getId()]->getPosition() != chunkPos);
                chunkPos = arrayIterators[attrs.firstDataAttribute().getId()]->getPosition();
                if (!sparseFormat || !chunkIterators[attrs.firstDataAttribute().getId()]->end()) {
                    if (!multisplit) {
                        Coordinates const& last =
                            chunkIterators[attrs.firstDataAttribute().getId()]->getLastPosition();
                        for (size_t i = 1; i < nDimensions; i++) {
                            if (last[i] < dims[i].getEndMax()) {
                                multisplit = true;
                            }
                        }
                    }
                    if (isSparse || storeFormat) {
                        if (!firstItem) {
                            firstItem = true;
                            for (size_t i = 0; i < nDimensions; i++) {
                                Fputc(']', f);
                            }
                            Fprintf(f, ";\n");
                            if (storeFormat) {
                                Fputc('{', f);
                                for (size_t i = 0; i < nDimensions; i++) {
                                    if (i != 0) {
                                        Fputc(',', f);
                                    }
                                    Fprintf(f, "%" PRIi64, chunkPos[i]);
                                }
                                Fputc('}', f);
                            }
                            for (size_t i = 0; i < nDimensions; i++) {
                                Fputc('[', f);
                            }
                        }
                    }
                    if (storeFormat) {
                        coord = chunkIterators[attrs.firstDataAttribute().getId()]->getChunk().getFirstPosition(true);
                        coord[nDimensions-1] -= 1; // to simplify increment
                    }
                    // Iterator over all chunk elements
                    while (!chunkIterators[attrs.firstDataAttribute().getId()]->end()) {
                        if (!isSparse) {
                            Coordinates const& pos =
                                chunkIterators[attrs.firstDataAttribute().getId()]->getPosition();
                            int nbr = 0;
                            for (size_t i = nDimensions-1;
                                 pos[i] != ++coord[i];
                                 i--) {
                                if (!firstItem) {
                                    Fputc(']', f);
                                    nbr += 1;
                                }
                                if (multisplit) {
                                    coord[i] = pos[i];
                                    if (i == 0) {
                                        break;
                                    }
                                } else {
                                    if (i == 0) {
                                        break;
                                    } else {
                                        coord[i] = dims[i].getStartMin();
                                        if (sparseFormat) {
                                            coord[i] = pos[i];
                                            if (i == 0) {
                                                break;
                                            }
                                        } else {
                                            // These assertions only hold when using the CompatibilityIterator.
                                            assert(coord[i] == pos[i]);
                                            assert(i != 0);
                                        }
                                    }
                                }
                            }
                            if (!firstItem) {
                                Fputc(nbr == (int)nDimensions ? ';' : ',', f);
                            }
                            if (gap) {
                                Fputc('{', f);
                                for (size_t i = 0; i < nDimensions; i++) {
                                    if (i != 0) {
                                        Fputc(',', f);
                                    }
                                    Fprintf(f, "%" PRIi64, pos[i]);
                                    coord[i] = pos[i];
                                }
                                Fputc('}', f);
                                gap = false;
                            }
                            if (startOfArray) {
                                if (storeFormat) {
                                    Fputc('{', f);
                                    for (size_t i = 0; i < nDimensions; i++) {
                                        if (i != 0) {
                                            Fputc(',', f);
                                        }
                                        Fprintf(f, "%" PRIi64, chunkPos[i]);
                                    }
                                    Fputc('}', f);
                                }
                                for (size_t i = 0; i < nDimensions; i++) {
                                    Fputc('[', f);
                                }
                                startOfArray = false;
                            }
                            while (--nbr >= 0) {
                                Fputc('[', f);
                            }
                            if (sparseFormat) {
                                Fputc('{', f);
                                Coordinates const& pos =
                                    chunkIterators[attrs.firstDataAttribute().getId()]->getPosition();
                                for (size_t i = 0; i < nDimensions; i++) {
                                    if (i != 0) {
                                        Fputc(',', f);
                                    }
                                    Fprintf(f, "%" PRIi64, pos[i]);
                                }
                                Fputc('}', f);
                            }
                        } else {
                            if (!firstItem) {
                                Fputc(',', f);
                            }
                            if (startOfArray) {
                                if (storeFormat) {
                                    Fputc('{', f);
                                    for (size_t i = 0; i < nDimensions; i++) {
                                        if (i != 0) {
                                            Fputc(',', f);
                                        }
                                        Fprintf(f, "%" PRIi64, chunkPos[i]);
                                    }
                                    Fputc('}', f);
                                }
                                for (size_t i = 0; i < nDimensions; i++) {
                                    Fputc('[', f);
                                }
                                startOfArray = false;
                            }
                            Fputc('{', f);
                            Coordinates const& pos =
                                chunkIterators[attrs.firstDataAttribute().getId()]->getPosition();
                            for (size_t i = 0; i < nDimensions; i++) {
                                if (i != 0) {
                                    Fputc(',', f);
                                }
                                Fprintf(f, "%" PRIi64, pos[i]);
                            }
                            Fputc('}', f);
                        }
                        Fputc('(', f);
                        if (!chunkIterators[attrs.firstDataAttribute().getId()]->isEmpty()) {
                            for (const auto& attr : attrs) {
                                if (attr.getId() != attrs.firstDataAttribute().getId()) {
                                    Fputc(',', f);
                                }
                                const Value* v = &chunkIterators[attr.getId()]->getItem();
                                s_fprintValue(f, v, types[attr.getId()], converters[attr.getId()], textFormatter);
                            }
                        }
                        n += 1;
                        firstItem = false;
                        Fputc(')', f);

                        for (const auto& attr : attrs) {
                            ++(*chunkIterators[attr.getId()]);
                        }
                    }
                }
                for (const auto& attr : attrs) {
                    ++(*arrayIterators[attr.getId()]);
                }
                if (multisplit) {
                    for (size_t i = 0; i < nDimensions; i++) {
                        coord[i] = dims[i].getEndMax() + 1;
                    }
                }
            }
            if (startOfArray) {
                for (size_t i = 0; i < nDimensions; i++) { // n个维度，每个维度都需要一个[符号
                    Fputc('[', f);
                }
                startOfArray = false;
            }
            for (size_t i = 0; i < nDimensions; i++) {
                Fputc(']', f);
            }
        }
        Fputc('\n', f);

        checkStreamError(f, __FUNCTION__);
        return n;
    }


    /**
     * This code handles the lsparse format.
     *
     * @note Duh, but what _is_ the lsparse format?  It appears to be
     *       indistinguishable from sparse.  Some say it may have had
     *       something to do with the abandoned non-integer dimension
     *       (NID) feature.  As of 16.9, lsparse has been removed from
     *       the documentation and its implementation will be removed
     *       in the next release.
     */
    static uint64_t saveLsparseFormat(Array const& array,
                                      ArrayDesc const& desc,
                                      FILE* f,
                                      std::string const& format)
    {
        size_t i;
        uint64_t n = 0;
        Value::Formatter textFormatter;
        textFormatter.setPrecision(ArrayWriter::getPrecision());

        Attributes const& attrs = desc.getAttributes(true);
        size_t nAttributes = attrs.size();

        if (nAttributes != 0)
        {
            Dimensions const& dims = desc.getDimensions();
            const size_t nDimensions = dims.size();
            assert(nDimensions > 0);
            vector< std::shared_ptr<ConstArrayIterator> > arrayIterators(nAttributes);
            vector< std::shared_ptr<ConstChunkIterator> > chunkIterators(nAttributes);
            vector< TypeId> attTypes(nAttributes);
            vector< FunctionPointer> attConverters(nAttributes);
            vector< FunctionPointer> dimConverters(nDimensions);
            vector<Value> origPos;

            int iterationMode = ConstChunkIterator::IGNORE_OVERLAPS;

            for (const auto& attr : attrs)
            {
                arrayIterators[attr.getId()] = array.getConstIterator(attr);
                attTypes[attr.getId()] = attr.getType();
            }

            Coordinates coord(nDimensions);
            bool startOfArray = true;

            // Set initial position
            Coordinates chunkPos(nDimensions);
            for (i = 0; i < nDimensions; i++)
            {
                coord[i] = dims[i].getStartMin();
                chunkPos[i] = dims[i].getStartMin();
            }

            // Check if chunking is performed in more than one dimension
            bool multisplit = false;
            for (i = 1; i < nDimensions; i++)
            {
                if (dims[i].getChunkInterval() < static_cast<int64_t>(dims[i].getLength()))
                {
                    multisplit = true;
                }
            }

            coord[nDimensions-1] -= 1; // to simplify increment
            chunkPos[nDimensions-1] -= dims[nDimensions-1].getChunkInterval();
            {
                // Iterate over all chunks
                bool firstItem = true;
                while (!arrayIterators[attrs.firstDataAttribute().getId()]->end())
                {
                    // Get iterators for the current chunk
                    for (const auto& attr : attrs)
                    {
                        ConstChunk const& chunk = arrayIterators[attr.getId()]->getChunk();
                        chunkIterators[attr.getId()] = chunk.getConstIterator(iterationMode);
                    }

                    int j = safe_static_cast<int>(nDimensions);
                    while (--j >= 0 && (chunkPos[j] += dims[j].getChunkInterval()) > dims[j].getEndMax())
                    {
                        chunkPos[j] = dims[j].getStartMin();
                    }
                    bool gap = true;
                    chunkPos = arrayIterators[attrs.firstDataAttribute().getId()]->getPosition();
                    if (!chunkIterators[attrs.firstDataAttribute().getId()]->end())
                    {
                        if (!multisplit)
                        {
                            Coordinates const& last =
                                chunkIterators[attrs.firstDataAttribute().getId()]->getLastPosition();
                            for (i = 1; i < nDimensions; i++)
                            {
                                if (last[i] < dims[i].getEndMax())
                                {
                                    multisplit = true;
                                }
                            }
                        }

                        // Iterator over all chunk elements
                        while (!chunkIterators[attrs.firstDataAttribute().getId()]->end())
                        {
                            {
                                Coordinates const& pos =
                                    chunkIterators[attrs.firstDataAttribute().getId()]->getPosition();
                                int nbr = 0;
                                for (i = nDimensions-1; pos[i] != ++coord[i]; i--)
                                {
                                    if (!firstItem)
                                    {
                                        Fputc(']', f);
                                        nbr += 1;
                                    }
                                    if (multisplit)
                                    {
                                        coord[i] = pos[i];
                                        if (i == 0)
                                        {
                                            break;
                                        }
                                    }
                                    else
                                    {
                                        if (i == 0)
                                        {
                                            break;
                                        }
                                        else
                                        {
                                            coord[i] = dims[i].getStartMin();
                                            coord[i] = pos[i];
                                            if (i == 0)
                                            {
                                                break;
                                            }
                                        }
                                    }
                                }
                                if (!firstItem)
                                {
                                    Fputc(nbr == (int)nDimensions ? ';' : ',', f);
                                }
                                if (gap)
                                {
                                    s_fprintCoordinates(f, pos);
                                    for (i = 0; i < nDimensions; i++)
                                    {
                                        coord[i]=pos[i];
                                    }
                                    gap = false;
                                }
                                if (startOfArray)
                                {
                                    for (i = 0; i < nDimensions; i++)
                                    {
                                        Fputc('[', f);
                                    }
                                    startOfArray = false;
                                }
                                while (--nbr >= 0)
                                {
                                    Fputc('[', f);
                                }
                                s_fprintCoordinates(f, pos);
                            }
                            Fputc('(', f);
                            if (!chunkIterators[attrs.firstDataAttribute().getId()]->isEmpty())
                            {
                                for (const auto& attr : attrs)
                                {
                                    if (attr.getId() != attrs.firstDataAttribute().getId())
                                    {
                                        Fputc(',', f);
                                    }

                                    s_fprintValue(f, &chunkIterators[attr.getId()]->getItem(),
                                                  attTypes[attr.getId()],
                                                  attConverters[attr.getId()],
                                                  textFormatter);
                                }
                            }
                            n += 1;
                            firstItem = false;
                            Fputc(')', f);
                            for (const auto& attr : attrs)
                            {
                                ++(*chunkIterators[attr.getId()]);
                            }
                        }
                    }
                    for (const auto& attr : attrs)
                    {
                        ++(*arrayIterators[attr.getId()]);
                    }
                    if (multisplit)
                    {
                        for (i = 0; i < nDimensions; i++)
                        {
                            coord[i] = dims[i].getEndMax() + 1;
                        }
                    }
                }
                if (startOfArray)
                {
                    for (i = 0; i < nDimensions; i++)
                    {
                        Fputc('[', f);
                    }
                    startOfArray = false;
                }
                for (i = 0; i < nDimensions; i++)
                {
                    Fputc(']', f);
                }
            }
            Fputc('\n', f);
        }
        checkStreamError(f, __FUNCTION__);
        return n;
    }

#ifndef SCIDB_CLIENT
    static uint64_t saveOpaque(Array const& array,
                               ArrayDesc const& desc,
                               FILE* f,
                               std::shared_ptr<Query> const& query)
    {
        const auto& attrs = desc.getAttributes();
        const auto nAttrs = attrs.size();
        vector< std::shared_ptr<ConstArrayIterator> > arrayIterators(nAttrs);
        uint64_t n;
        OpaqueChunkHeader hdr;

        hdr.version = SCIDB_OPAQUE_FORMAT_VERSION;
        hdr.signature = OpaqueChunkHeader::calculateSignature(desc);
        hdr.magic = OPAQUE_CHUNK_MAGIC;

        hdr.flags = OpaqueChunkHeader::ARRAY_METADATA;
        stringstream ss;
        text_oarchive oa(ss);
        oa & desc;
        string const s(ss.str());
        hdr.size = safe_static_cast<uint32_t>(s.size());
        // NOTE: fwrite_unlocked() is >= 20% faster than fwrite() for double-precision matrices
        if (scidb::fwrite_unlocked(&hdr, sizeof(hdr), 1, f) != 1
            || scidb::fwrite_unlocked(&s[0], 1, hdr.size, f) != hdr.size)
        {
            int err = errno ? errno : EIO;
            throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR)
                << ::strerror(err) << err;
        }

        for (const auto& attr : attrs) {
            arrayIterators[attr.getId()] = array.getConstIterator(attr);
        }
        for (n = 0; !arrayIterators[attrs.firstDataAttribute().getId()]->end(); n++) {
            for (const auto& attr : attrs) {
                ConstChunk const* chunk = &arrayIterators[attr.getId()]->getChunk();
                Coordinates const& pos = chunk->getFirstPosition(false);
                PinBuffer scope(*chunk);
                if (chunk->getSize() > (uint64_t)numeric_limits<uint32_t>::max())
                {
                    throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR)
                        << "Chunk larger than maximum size: " << chunk->getSize();
                }
                hdr.size = safe_static_cast<uint32_t>(chunk->getSize());
                hdr.attrId = attr.getId();
                hdr.compressionMethod = static_cast<int8_t>(chunk->getCompressionMethod());
                hdr.flags = 0;
                hdr.flags |= OpaqueChunkHeader::RLE_FORMAT;
                if (!chunk->getAttributeDesc().isEmptyIndicator()) {
                    // RLE chunks received from other nodes by SG contain empty bitmap.
                    // There is no need to save this bitmap in each chunk - so just cut it.
                    ConstRLEPayload payload(reinterpret_cast<const char*>(chunk->getConstData()));
                    assert(hdr.size >= payload.packedSize());
                    hdr.size = safe_static_cast<uint32_t>(payload.packedSize());
                }
                hdr.nDims = safe_static_cast<int8_t>(pos.size());
                if (scidb::fwrite_unlocked(&hdr, sizeof(hdr), 1, f) != 1
                    || scidb::fwrite_unlocked(&pos[0], sizeof(Coordinate), hdr.nDims, f) != hdr.nDims
                    || scidb::fwrite_unlocked(chunk->getConstData(), 1, hdr.size, f) != hdr.size)
                {
                    int err = errno ? errno : EIO;
                    throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR)
                        << ::strerror(err) << err;
                }
            }
            for (const auto& attr : attrs) {
                ++(*arrayIterators[attr.getId()]);
            }
        }

        // TODO: investigate: checkStreamError(fp, __FUNCTION__); not used here,
        //       but it is before other returns in this file.  explain or modify.
        return n;
    }

    /// Compute bytes of padding to insert for 'skip' columns.
    static inline size_t skip_bytes(ExchangeTemplate::Column const& c)
    {
        SCIDB_ASSERT(c.skip);
        return (c.fixedSize ? c.fixedSize : sizeof(uint32_t)) + c.nullable;
    }

    /**
     * Write binary data based on a template.
     * @see scidb::BinaryChunkLoader::loadChunk
     */
    static uint64_t saveUsingTemplate(Array const& array,
                                      ArrayDesc const& desc,
                                      FILE* f,
                                      string const& format,
                                      std::shared_ptr<Query> const& query)
    {
        ExchangeTemplate templ = TemplateParser::parse(desc, format, false);
        const auto& attrs = desc.getAttributes(true /*exclude empty bitmap*/);
        const size_t N_ATTRS = attrs.size();
        const size_t N_COLUMNS = templ.columns.size();
        SCIDB_ASSERT(N_COLUMNS >= N_ATTRS); // One col per attr, plus "skip" columns
        vector< std::shared_ptr<ConstArrayIterator> > arrayIterators(N_ATTRS);
        vector< std::shared_ptr<ConstChunkIterator> > chunkIterators(N_ATTRS);
        vector< Value > cnvValues(N_ATTRS);
        vector< char > padBuffer(sizeof(uint64_t) + 1, '\0'); // Big enuf for all nullable built-ins
        size_t nMissingReasonOverflows = 0;

        auto attrIter = attrs.begin();
        for (size_t c = 0; c < N_COLUMNS; ++c) {
            ExchangeTemplate::Column const& column = templ.columns[c];
            if (column.skip) {
                // Prepare to write (enough) padding.
                size_t pad = skip_bytes(column);
                if (pad > padBuffer.size()) {
                    padBuffer.resize(pad);
                }
            } else {
                // Prepare to write values.
                SCIDB_ASSERT(attrIter->getId() < N_ATTRS);
                arrayIterators[attrIter->getId()] = array.getConstIterator(*attrIter);
                if (column.converter) {
                    cnvValues[attrIter->getId()] = Value(column.externalType);
                }
                ++attrIter;            // next attribute
            }
        }

        // NOTE: fwrite_unlocked() is >= 20% faster than fwrite() for double-precision matrices
        uint64_t nCells = 0;    // aka number of tuples written
        for (size_t n = 0; !arrayIterators[attrs.firstDataAttribute().getId()]->end(); n++) {
            for (const auto& attr : attrs) {
                chunkIterators[attr.getId()] =
                    arrayIterators[attr.getId()]->getChunk().getConstIterator(
                        ConstChunkIterator::IGNORE_OVERLAPS);
            }
            while (!chunkIterators[attrs.firstDataAttribute().getId()]->end()) {
                ++nCells;
                auto attrIter = attrs.begin();
                for (size_t c = 0; c < N_COLUMNS; ++c) {
                    ExchangeTemplate::Column const& column = templ.columns[c];
                    if (column.skip) {
                        // On output, skip means write NUL bytes (ticket #4703).
                        size_t pad = skip_bytes(column);
                        SCIDB_ASSERT(padBuffer.size() >= pad);
                        if (scidb::fwrite_unlocked(&padBuffer[0], 1, pad, f) != pad) {
                            int err = errno ? errno : EIO;
                            throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR)
                                << ::strerror(err) << err;
                        }
                    }
                    else {
                        Value const* v = &chunkIterators[attrIter->getId()]->getItem();
                        if (column.nullable) {
                            // Binary format supports int8_t reasons only.
                            static_assert(std::is_same<Value::reason,int8_t>::value,
                                          "Value::reason type has changed, range checking may be needed");
                            int8_t missingReason = v->getMissingReason();
                            if (scidb::fwrite_unlocked(&missingReason, sizeof(missingReason), 1, f) != 1) {
                                int err = errno ? errno : EIO;
                                throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR)
                                    << ::strerror(err) << err;
                            }
                        }
                        if (v->isNull()) {
                            if (!column.nullable) {
                                throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_ASSIGNING_NULL_TO_NON_NULLABLE);
                            }
                            // for varying size type write 4-bytes counter
                            size_t size = column.fixedSize ? column.fixedSize : sizeof(uint32_t);
                            SCIDB_ASSERT(padBuffer.size() >= size);
                            if (scidb::fwrite_unlocked(&padBuffer[0], 1, size, f) != size) {
                                int err = errno ? errno : EIO;
                                throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR)
                                    << ::strerror(err) << err;
                            }
                        } else {
                            if (column.converter) {
                                column.converter(&v, &cnvValues[attrIter->getId()], NULL);
                                v = &cnvValues[attrIter->getId()];
                            }
                            if (v->size() > numeric_limits<uint32_t>::max()) {
                                throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_TRUNCATION)
                                    << v->size() << numeric_limits<uint32_t>::max();
                            }
                            uint32_t size = (uint32_t)v->size();
                            if (column.fixedSize == 0) { // varying size type
                                if (scidb::fwrite_unlocked(&size, sizeof(size), 1, f) != 1
                                    || scidb::fwrite_unlocked(v->data(), 1, size, f) != size)
                                {
                                    int err = errno ? errno : EIO;
                                    throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR)
                                        << ::strerror(err) << err;
                                }
                            } else {
                                if (size > column.fixedSize) {
                                    throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_TRUNCATION)
                                        << size << column.fixedSize;
                                }
                                if (scidb::fwrite_unlocked(v->data(), 1, size, f) != size)
                                {
                                    int err = errno ? errno : EIO;
                                    throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR)
                                        << ::strerror(err) << err;
                                }
                                if (size < column.fixedSize) {
                                    size_t padSize = column.fixedSize - size;
                                    assert(padSize <= padBuffer.size());
                                    if (scidb::fwrite_unlocked(&padBuffer[0], 1, padSize, f) != padSize)
                                    {
                                        int err = errno ? errno : EIO;
                                        throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR)
                                            << ::strerror(err) << err;
                                    }
                                }
                            }
                        }
                        ++(*chunkIterators[attrIter->getId()]);
                        ++attrIter;    // next attribute
                    }
                }
            }
            for (const auto& attr : attrs) {
                ++(*arrayIterators[attr.getId()]);
            }
        }
        if (nMissingReasonOverflows > 0) {
            query->postWarning(SCIDB_WARNING(SCIDB_LE_MISSING_REASON_OUT_OF_BOUNDS));
        }
        checkStreamError(f, __FUNCTION__);
        return nCells;
#       undef PAD
    }

#endif

    static inline int closeOrFlush(FILE* fp)
    {
        assert(fp != nullptr);
        if (fp == stdout || fp == stderr) {
            return ::fflush(fp);
        } else {
            return ::fclose(fp);
        }
    }

    uint64_t ArrayWriter::save(Array const& array, string const& file,
                               const std::shared_ptr<Query>& query,
                               string const& format, unsigned flags,
                               FILE* fp)
    {
        ArrayDesc const& desc = array.getArrayDesc();
        uint64_t n = 0;

        FILE* f;
        bool isBinary = compareStringsIgnoreCase(format, "opaque") == 0 || format[0] == '(';
        if (fp) {
            f = fp;
        } else if (file == "console" || file == "stdout") { // dz 这里就是输出到终端窗口中
            f = stdout;
        } else if (file == "stderr") {
            f = stderr;
        } else {
            bool append = flags & F_APPEND;
            f = fopen(file.c_str(), isBinary ? append ? "ab" : "wb" : append ? "a" : "w");
            if (NULL == f) {
                int error = errno;
                LOG4CXX_DEBUG(logger, "Attempted to open output file '" << file
                              << "' failed: " << ::strerror(error) << " (" << error);
                throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_CANT_OPEN_FILE)
                    << file << ::strerror(error) << error;
            }
            struct flock flc;
            flc.l_type = F_WRLCK;
            flc.l_whence = SEEK_SET;
            flc.l_start = 0;
            flc.l_len = 1;

            if(file.compare("/dev/null") != 0 ) {   // skip locking if /dev/null
                // ... because the fcntl will fail
                // in fact in parallel mode, all absolute paths will fail the fcntl
                // so the logical operator and/or open code should be fixed to reject
                // all absoluate paths in parallel mode *except* /dev/null
                // which is very important to support for performance testing and diagnosis
                int rc = fcntl(fileno(f), F_SETLK, &flc);
                if (rc == -1) {
                    throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_CANT_LOCK_FILE)
                        << file << ::strerror(errno) << errno;
                }
            }
        }

        // Switch out to "foo-separated values" if we can.
        std::shared_ptr<XsvParms> xParms;
        string::size_type colon = format.find(':');
        string baseFmt = format.substr(0, colon);
        if (compareStringsIgnoreCase(baseFmt, "csv") == 0) {
            xParms = make_shared<XsvParms>(format);
        } else if (compareStringsIgnoreCase(baseFmt, "csv+") == 0) {
            xParms = make_shared<XsvParms>(format);
            xParms->setCoords(!desc.isDataframe());
        } else if (compareStringsIgnoreCase(baseFmt, "dcsv") == 0) {
            xParms = make_shared<XsvParms>(format);
            xParms->setCoords(!desc.isDataframe())
                .setCompat(true).setLabels(true).setPretty(true);
        } else if (compareStringsIgnoreCase(baseFmt, "tsv") == 0) {
            xParms = make_shared<XsvParms>(format);
            xParms->setDelim('\t');
        } else if (compareStringsIgnoreCase(baseFmt, "tsv+") == 0) {
            xParms = make_shared<XsvParms>(format);
            xParms->setDelim('\t').setCoords(!desc.isDataframe());
        }

        try {
            if (xParms.get()) {
                xParms->setParallel(flags & F_PARALLEL);
                xParms->setPrecision(ArrayWriter::getPrecision());
                n = saveXsvFormat(array, desc, f, *xParms, query);
            }
            else if (compareStringsIgnoreCase(format, "lsparse") == 0) {
                n = saveLsparseFormat(array, desc, f, format);
            }
#ifndef SCIDB_CLIENT
            else if (compareStringsIgnoreCase(format, "opaque") == 0) {
                n = saveOpaque(array, desc, f, query);
            }
            else if (format[0] == '(') {
                n = saveUsingTemplate(array, desc, f, format, query);
            }
#endif
            else {
                n = saveTextFormat(array, desc, f, format);
            }
        }
        catch (Exception& e) {
            closeOrFlush(f);
            e.raise();
        }

        int rc = closeOrFlush(f);
        if (rc != 0) {
            int err = errno ? errno : EIO;
            assert(err!= EBADF);
            throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR)
                << ::strerror(err) << err;
        }
        return n;
    }

} // namespace
