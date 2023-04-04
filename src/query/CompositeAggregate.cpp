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
 * @file CompositeAggregate.cpp
 *
 * @description A CompositeAggregate is a vector of subaggregates.  It
 * can be used to accumulate input values into many aggregates at
 * once, and then treat them as a single aggregate for purposes of
 * merging state.  CompositeAggregates are intended to be used
 * internally when SciDB needs to gather several metrics at once about
 * attribute values, dimension coordinates, or other data.
 *
 * @author Mike Leibensperger <mjl@paradigm4.com>
 */

#include <query/Aggregate.h>

#include <boost/crc.hpp>
#include <sys/param.h>          // for roundup()

using namespace std;

namespace scidb {

CompositeAggregate::CompositeAggregate()
    : Aggregate("_compose", TypeLibrary::getType(TID_VOID), TypeLibrary::getType(TID_VOID))
    , _fingerprint(0)        // "freezes" subagg vector on first state init
    , _supportAsterisk(true) // true iff all subaggs support asterisk
    , _orderSensitive(false) // true iff any subagg is order sensitive
    , _ignoreNulls(true)     // true iff all subaggs ignore nulls
{}


CompositeAggregate& CompositeAggregate::add(AggregatePtr subAggregate)
{
    SCIDB_ASSERT(subAggregate.get()); // Validate argument
    SCIDB_ASSERT(0 == _fingerprint);  // Not "frozen" yet, can still add

    // Enforce inputType compatibility.
    if (!subAggregate->getInputType().isVoid()) {
        if (_inputType.isVoid()) {
            _inputType = subAggregate->getInputType();
        } else if (_inputType.typeId() != subAggregate->getInputType().typeId()) {
            stringstream ss;
            ss << getName() << "-of-" << _inputType.typeId();
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_AGGREGATE_DOESNT_SUPPORT_TYPE)
                << ss.str() << subAggregate->getInputType().typeId();
        }
    }

    _subAggs.push_back(subAggregate);

    // Maintain these predicates for the composite as a whole.
    _supportAsterisk &= subAggregate->supportAsterisk();
    _orderSensitive |= subAggregate->isOrderSensitive();
    _ignoreNulls &= subAggregate->ignoreNulls();

    return *this;
}


AggregatePtr CompositeAggregate::clone() const
{
    std::shared_ptr<CompositeAggregate> result = std::make_shared<CompositeAggregate>();
    for (AggregatePtr const& p : _subAggs) {
        result->add(p->clone());
    }
    return result;
}


AggregatePtr CompositeAggregate::clone(Type const& inputType) const
{
    std::shared_ptr<CompositeAggregate> result = std::make_shared<CompositeAggregate>();
    for (AggregatePtr const& p : _subAggs) {
        result->add(p->clone(inputType));
    }
    return result;
}


namespace {

    inline char* roundup_cptr(char* ptr, size_t n)
    {
        return reinterpret_cast<char*>(
            roundup(reinterpret_cast<uint64_t>(ptr), n));
    }

    inline ptrdiff_t subtract_cptr(const void* p1, const void* p2)
    {
        return reinterpret_cast<const char*>(p1) - reinterpret_cast<const char*>(p2);
    }

    /// @brief Class to manage CompositeAggregate state values.
    class State {
    public:
        State(Value& v);
        State(Value const& v) : State(const_cast<Value&>(v)) {}

        void init(vector<AggregatePtr> const& subAggs, uint32_t fingerprint);
        bool isInitialized() const; // Light-weight, no assert
        bool validate() const;      // Heavy-weight, asserts on invalid

        uint32_t getFingerprint() const
        {
            SCIDB_ASSERT(isInitialized());
            Header const* hdr = reinterpret_cast<Header const*>(_state.data());
            return hdr->fingerprint;
        }

        /// Access to substates.
        /// @{
        Value const& operator[](size_t idx) const;
        Value& operator[](size_t idx)
        {
            State const& self = const_cast<State const&>(*this);
            return const_cast<Value&>(self[idx]);
        }
        /// @}

    private:
        typedef uint32_t magic_t;

        const magic_t SUBSTATE_MAGIC = 0x42424242;
        const magic_t HDR_MAGIC = 0xAEAEAEAE;
        const magic_t END_MAGIC = 0xEE00EE00;

        struct Header {
            magic_t magic;
            uint32_t fingerprint;
            size_t count;
            Header* self;
        };

        bool wasMoved() const;
        void fixInternalPointers();

        Value& _state;
    };

    /**
     * @brief Construct a State from a Value, fixing internal pointers.
     *
     * @description If we are looking at a previously initialized
     * State, we may need to fix up the internal pointers.  Perhaps
     * they point to memory in a remote instance, or they were
     * copy-constructed by value or otherwise relocated inside the
     * local instance.  (That can happen during SG, by way of
     * MultiStreamArray::mergePartialStreams().)
     *
     * @note Construction from a const @c Value calls this non-const
     * constructor, but it's OK because the internal pointers we are
     * fixing here are logically mutable.
     */
    State::State(Value& v)
        : _state(v)
    {
        if (isInitialized() && wasMoved()) {
            fixInternalPointers();
        }
    }

    /**
     * @brief Initialize a State::_state for a CompositeAggregate.
     *
     * @description The initialized composite state contains the
     * in-order initialized states of the subaggregates.  To achieve
     * this without turning the composite state into a messy DAG of
     * memory allocations, we use the "view" style of scidb::Value to
     * make all subaggregates' states point into the larger allocated
     * data buffer of the overall composite _state.  That way the
     * substates don't own their own memory; memory is owned only by
     * the composite _state.
     *
     * @p States can move around in memory and between instances when
     * they are SG'ed, and this will invalidate the internal pointers
     * in the views.  To deal with this, the State::Header stores a
     * pointer to itself.  If it ever stops pointing to itself, it
     * must have been moved, and the pointers must be fixed.  This is
     * done using a list of ptrdiff_t offsets stored in the State just
     * beyond the Header.  These act as memory location independent
     * pointers.
     *
     * @p The internal format of a composite State's data buffer is:
     * @code
     *    +---------------------------------+
     * .->| HDR_MAGIC                       | \
     * |  | fingerprint                     |  \ __ Header
     * |  | count                           |  /
     * `--|-self                            | /
     *    +---------------------------------+
     *    | ptrdiff_t                       | \ __ Location-independent
     *    |    :   (hdr.count of 'em)       | /    "pointers"
     *    +---------------------------------+
     *    | Embedded substate scidb::Value  | \ __ Substates
     *    |    :   (hdr.count of 'em)       | /
     *    +---------------------------------+
     *    | SUBSTATE_MAGIC                  | \
     *    | Data for 1st isLarge() substate |  \
     *    +---------------------------------+   |   Data pointed at by large
     *    | SUBSTATE_MAGIC                  |   |-- isView() substates
     *    | Data for 2nd isLarge() substate |   |
     *    +---------------------------------+   |
     *    |   etc.                          |  /
     *    |    :   (N <= hdr.count of 'em)  | /
     *    +---------------------------------+
     *    | END_MAGIC                       |
     *    +---------------------------------+
     * @endcode
     *
     * @p We make liberal use of magic numbers to detect memory
     * corruption.
     */
    void State::init(vector<AggregatePtr> const& subAggs, uint32_t fingerprint)
    {
        SCIDB_ASSERT(!subAggs.empty());

        // First have the subaggregates initialize some ordinary Values.
        vector<Value> subStates(subAggs.size());
        for (size_t i = 0; i < subAggs.size(); ++i) {
            subAggs[i]->initializeState(subStates[i]);
            SCIDB_ASSERT(!subStates[i].isTile());
        }

        // Now we need to compute how much data buffer we need for the
        // entire composite state.
        size_t data_len = sizeof(Header)
            + (subAggs.size() * sizeof(ptrdiff_t))
            + (subAggs.size() * sizeof(Value))
            + sizeof(magic_t);  // END_MAGIC
        for (auto const& ss : subStates) {
            if (ss.isLarge()) {
                data_len += roundup(ss.size(), sizeof(magic_t)) + sizeof(magic_t);
            }
        }

        // Allocate the composite state data buffer.
        char* p = static_cast<char*>(_state.setSize<Value::IGNORE_DATA>(data_len));
        ::memset(p, 0, data_len);
        char* end = &p[data_len] - sizeof(magic_t);
        *(magic_t*)end = END_MAGIC;

        // Build header.
        Header* hdr = reinterpret_cast<Header*>(p);
        hdr->magic = HDR_MAGIC;
        hdr->fingerprint = fingerprint;
        hdr->count = subAggs.size();
        hdr->self = hdr;

        // Bit-wise copy the small substates, make views for the large ones.
        // Ownership of subStates data buffers stays with subStates entries.
        // Don't touch the ptrdiff_t area yet, the view data() pointers aren't right.
        ptrdiff_t* pdiff = reinterpret_cast<ptrdiff_t*>(hdr + 1);
        Value* vp = reinterpret_cast<Value*>(pdiff + hdr->count);
        for (auto const& ss : subStates) {
            if (ss.isLarge()) {
                vp->setView(ss.data(), ss.size());
            } else {
                ::memcpy(vp, &ss, sizeof(Value));
            }
            ++vp;
        }

        // Now copy the large substate data buffers, prefixing magic numbers
        // and fixing up embedded 'view' Values as we go.
        // Record data's ptrdiff_t from _state.data() for location independence.
        p = reinterpret_cast<char*>(vp);
        vp = reinterpret_cast<Value*>(pdiff + hdr->count);
        for (size_t i = 0; i < hdr->count; ++i, ++vp, ++pdiff) {
            if (vp->isLarge()) {
                *(magic_t*)p = SUBSTATE_MAGIC;
                p += sizeof(magic_t);
                ::memcpy(p, vp->data(), vp->size()); // copy view data
                vp->setView(p, vp->size()); // re-point view at copy
                p += vp->size();
                p = roundup_cptr(p, sizeof(magic_t));
                *pdiff = subtract_cptr(vp->data(), _state.data());
            } else {
                *pdiff = 0;
            }
        }

        // Test post-conditions.
        SCIDB_ASSERT(*(magic_t*)p == END_MAGIC);
        SCIDB_ASSERT(validate());
    }

    // Lightweight, non-asserting validate().
    bool State::isInitialized() const
    {
        Header const* hdr = reinterpret_cast<Header const*>(_state.data());
        return _state.isLarge() && hdr->magic == HDR_MAGIC;
    }

    // Verify this State was built correctly via a call to init().
    bool State::validate() const
    {
        SCIDB_ASSERT(isInitialized());
        Header const* hdr = reinterpret_cast<Header const*>(_state.data());
        SCIDB_ASSERT(hdr->magic == HDR_MAGIC);
        SCIDB_ASSERT(hdr->count > 0);

        ptrdiff_t const* pdiff = reinterpret_cast<ptrdiff_t const*>(hdr + 1);
        Value const* vp = reinterpret_cast<Value const*>(pdiff + hdr->count);
        const char* bufp = reinterpret_cast<const char*>(vp + hdr->count);
        for (size_t i = 0; i < hdr->count; ++i, ++vp, ++pdiff) {
            SCIDB_ASSERT(!vp->isTile());
            if (vp->isLarge()) {
                SCIDB_ASSERT(vp->isView());
                SCIDB_ASSERT(*(magic_t*)bufp == SUBSTATE_MAGIC);
                SCIDB_ASSERT(subtract_cptr(vp->data(), bufp) == sizeof(magic_t));
                bufp += sizeof(magic_t);
                SCIDB_ASSERT(*pdiff == subtract_cptr(bufp, _state.data()));
                bufp += roundup(vp->size(), sizeof(magic_t));
            } else {
                // Subaggregate-initialized state may be a null.
                SCIDB_ASSERT(vp->isDatum() || vp->isNull());
                SCIDB_ASSERT(*pdiff == 0);
            }
        }
        SCIDB_ASSERT(*(magic_t*)bufp == END_MAGIC);
        SCIDB_ASSERT(_state.data() == (bufp + sizeof(magic_t) - _state.size()));
        return true;
    }

    /**
     * @brief Return embedded substate.
     */
    Value const& State::operator[](size_t idx) const
    {
        if (isDebug()) {
            SCIDB_ASSERT(validate());
        } else {
            SCIDB_ASSERT(isInitialized());
        }

        Header const* hdr = reinterpret_cast<Header const*>(_state.data());
        ptrdiff_t const* pdiff = reinterpret_cast<ptrdiff_t const*>(hdr + 1);
        Value const* vp = reinterpret_cast<Value const*>(pdiff + hdr->count);
        vp += idx;
        return *vp;
    }

    bool State::wasMoved() const
    {
        SCIDB_ASSERT(isInitialized());
        Header const* hdr = reinterpret_cast<Header const*>(_state.data());
        return hdr != hdr->self; // I am not feeling quite myself today...
    }

    /**
     * @brief Fix internal pointers that point to remote memory.
     *
     * @description If the overall state originated on a foreign
     * instance, we have some work to do before returning the desired
     * substate.  Any isLarge() 'view' substates' data pointers are
     * pointers to memory in the foreign instance.  Use the ptrdiff_t
     * values to fix them, pointing them back into the right places
     * within this State object's data buffer.
     */
    void State::fixInternalPointers()
    {
        Header* hdr = reinterpret_cast<Header*>(_state.data());
        ptrdiff_t* pdiff = reinterpret_cast<ptrdiff_t*>(hdr + 1);
        Value* vp = reinterpret_cast<Value*>(pdiff + hdr->count);

        for (size_t i = 0; i < hdr->count; ++i, ++pdiff, ++vp) {
            if (vp->isLarge()) {
                char* p = reinterpret_cast<char*>(_state.data()) + *pdiff;
                vp->setView(p, vp->size());
                if (isDebug()) {
                    p -= sizeof(magic_t);
                    SCIDB_ASSERT(*(magic_t*)p == SUBSTATE_MAGIC);
                }
            }
        }

        // Mark ourself fixed.
        hdr->self = hdr;
    }

} // anonymous namespace


void CompositeAggregate::initializeState(Value& state)
{
    // First state initialization?  Time to "freeze" the subaggregate
    // vector by fingerprinting it.
    if (0 == _fingerprint) {
        computeFingerprint();
    }
    SCIDB_ASSERT(_fingerprint != 0);

    State s(state);
    s.init(_subAggs, _fingerprint);
}


/// Accumulates srcValue for all subaggregates.
void CompositeAggregate::accumulateIfNeeded(Value& dstState, Value const& srcValue)
{
    if (!isStateInitialized(dstState)) {
        initializeState(dstState);
        SCIDB_ASSERT(isStateInitialized(dstState));
    }

    State state(dstState);
    Value* vp = &state[0];
    for (auto& agg : _subAggs) {
        agg->accumulateIfNeeded(*vp++, srcValue);
    }

    SCIDB_ASSERT(state.validate());
}


bool CompositeAggregate::isAccumulatable(Value const& srcValue) const
{
    // True if any subaggregate can accumulate the value.
    for (auto const& agg : _subAggs) {
        if (agg->isAccumulatable(srcValue)) {
            return true;
        }
    }
    return false;
}


void CompositeAggregate::accumulate(Value& dstState, Value const& input)
{
    // Have to use accumulateIfNeeded() here rather than accumulate()
    // because the latter is protected.  Bummer.

    Value* vp = &State(dstState)[0];
    for (auto const& agg : _subAggs) {
        agg->accumulateIfNeeded(*vp++, input);
    }
}


bool CompositeAggregate::isMergeable(Value const& srcState) const
{
    State state(srcState);
    if (!state.isInitialized()) {
        return false;
    }
    if (isDebug()) {
        SCIDB_ASSERT(state.validate());
    }

    Value const* vp = &state[0];
    for (auto const& agg : _subAggs) {
        if (!agg->isMergeable(*vp++)) {
            return false;
        }
    }
    return true;
}


void CompositeAggregate::merge(Value& dstState, Value const& srcState)
{
    // Have to use mergeIfNeeded() here rather than merge() because the
    // latter is protected.  Bummer.

    Value* dst = &State(dstState)[0];
    Value const* src = &State(srcState)[0];
    for (auto const& agg : _subAggs) {
        agg->mergeIfNeeded(*dst++, *src++);
    }
}


void CompositeAggregate::finalResult(Value& dstValue, Value const& srcState, size_t aggIndex)
{
    SCIDB_ASSERT(!_subAggs.empty());
    SCIDB_ASSERT(aggIndex < _subAggs.size());
    Value const* vp = &State(srcState)[aggIndex];
    _subAggs[aggIndex]->finalResult(dstValue, *vp);
}


void CompositeAggregate::finalResults(vector<Value>& dstVals, Value const& srcState)
{
    SCIDB_ASSERT(!_subAggs.empty());
    dstVals.clear();
    dstVals.resize(_subAggs.size());
    Value const* vp = &State(srcState)[0];
    for (size_t i = 0; i < _subAggs.size(); ++i, ++vp) {
        _subAggs[i]->finalResult(dstVals[i], *vp);
    }
}


/**
 *  Once we start initializing state values, we'll want to ensure that
 *  all subsequent operations use state that was initialized in
 *  exactly the same way.  By computing a CRC fingerprint over the
 *  names and input types of the subaggregates, we can check for state
 *  compatibility by comparing the embedded fingerprint.
 */
void CompositeAggregate::computeFingerprint()
{
    SCIDB_ASSERT(!_subAggs.empty());
    SCIDB_ASSERT(_fingerprint == 0);

    boost::crc_32_type crc;
    for (auto& agg : _subAggs) {
        string const& name = agg->getName();
        string const& inputType = agg->getInputType().name();

        // Fingerprint names and input types (and include trailing NULs).
        crc.process_bytes(name.c_str(), name.size()+1);
        crc.process_bytes(inputType.c_str(), inputType.size()+1);
    }

    _fingerprint = crc.checksum();
    SCIDB_ASSERT(_fingerprint != 0);
}

} // namespace
