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

#include <system/Constants.h>                            // For KiB, MiB, etc.
#include <util/arena/LimitedArena.h>                     // For LimitedArena
#include <boost/array.hpp>                               // For boost::array
#include <bitset>                                        // For std::bitset
#include "ArenaDetails.h"                                // For implementation
#include "ArenaHeader.h"                                 // For Header

/****************************************************************************/
namespace scidb { namespace arena { namespace {
/****************************************************************************/

template<class,class>                                    // For each header
class Link;                                              // Adds list links
class Page;                                              // Large allocation
class Live;                                              // Small allocation
class Dead;                                              // Adds binning info

/****************************************************************************/

/**
 *  @brief      Adds intrusive linked list handling to class 'base'.
 *
 *  @details    Class Link<t,b>  adds a pair of pointers to the base class 'b'
 *              that allow it to maintain an intrusively linked list that runs
 *              through each of its instances.
 *
 *              It also implements a number of other member functions that are
 *              common to both the Page and Dead header classes, including:
 *
 *              - size()       returns the overall size of the allocation that
 *              is described by this header.
 *
 *              - length()     returns the size of  the payload portion of the
 *              allocation that is described by this header.
 *
 *              - payload()    returns a pointer to the payload portion of the
 *              allocation that is described by this header.
 *
 *              The template assumes that its base 'b' can be initialized with
 *              a size and that this lives in a member named '_size,  and also
 *              that the base provides a function named overhead() that yields
 *              the number of words occupied by the header itself.
 *
 *  @author     jbell@paradigm4.com.
 */
template<class type,class base>
class Link : public base, noncopyable
{
 protected:                // Construction
                              Link(size_t size)
                               : base(size),
                                _prev(0),
                                _next(0)                 {}

 public:                   // Header Operations
            size_t            size()               const {return base::_size;}
            size_t            length()             const {return size()-base::overhead();}
            bool              consistent()         const;// Verify consistency
            void*             payload();                 // Return the payload

 public:                   // List Operations
            bool              empty()              const {return _prev==0 && _next==0;}
            void              unlink()                   {_prev = _next = 0;}

 public:                   // List Operations
    static  void              push(type*&,type*);        // Insert onto list
    static  void              drop(type*&,type*);        // Remove from list
    static  type*             pop (type*&);              // Pop head of list

 private:                  // Representation
            type*             _prev;                     // Next item on list
            type*             _next;                     // Prev item on list
};

/**
 *  @brief      Implements the base class for class Page, defined below.
 *
 *  @author     jbell@paradigm4.com.
 */
class Page_
{
 public:                   // Construction
                              Page_(size_t size)         // Page size in words
                               : _size(size)             {assert(_size==size);}

 public:                   // Operations
    static  size_t            overhead()                 {return asWords(sizeof(Link<Page_,Page_>));}

 protected:                // Representation
            size_t      const _size;                     // Page size in words
};

/**
 *  @brief      Represents a large allocation from which many small blocks are
 *              then sub-allocated.
 *
 *  @details    Class Page provides the header for a large hunk of memory from
 *              which many smaller blocks are then sub-allocated. It includes:
 *
 *              - the overall size of the allocation, expressed in words.
 *
 *              - pointers to the previous and next pages in the arena's list
 *              of currently active pages.
 *
 *  @author     jbell@paradigm4.com.
 */
class Page : public Link<Page,Page_>
{
 public:                   // Operations
                              Page(size_t size)          // Page size in words
                               : Link<Page,Page_>(size)  {}
};

/**
 *  @brief      Represents a live allocation made within some page.
 *
 *  @details    Class Live provides the header for a live block of memory that
 *              resides within a page, and that is currently being used by the
 *              clients of the arena. It includes:
 *
 *              - a bit to indicate whether the block is live or dead; used to
 *              determine whether a dead neighbouring block can be merged with
 *              this block or not.
 *
 *              - a bit to indicate whether this block sits at the very end of
 *              the page or else is followed by a 'successor' block.
 *
 *              - the offset in words of the block's 'predecessor', that block
 *              within the same page thats sits immediately before this block,
 *              or zero if this block is at the very front of the page.
 *
 *              - the total size in words of the block: this includes the size
 *              of both the header and also its payload.
 *
 *              Together these fields allow the arena to work out in which bin
 *              to place a block when killing it off, whether the block can be
 *              merged with its neighbours, and if so, whether the entire page
 *              in which it sits can be freed or not.
 *
 *  @author     jbell@paradigm4.com.
 */
class Live
{
 public:                   // Construction
                              Live(size_t size)          // Live size in words
                               : _live(0),               // Not yet resurrected
                                 _succ(0),               // No successor block
                                 _pred(0),               // No predecessor block
                                 _size(size & _BITS_MASK) {assert(_size==size);}

 public:                   // Operations
            bool              live()               const {return _live;}
            Dead*             dead();                    // Check the downcast
            Dead*             kill();                    // Mark dead and cast
            void              predecessor(Dead*);        // Update predecessor
    static  size_t            overhead()                 {return asWords(sizeof(Live));}
    static  size_t            smallest()                 {return asWords(sizeof(Link<Live,Live>));}

 protected:                // Representation
    static  size_t      const _BITS = std::numeric_limits<size_t>::digits/2-1;
    static  size_t      const _BITS_MASK = size_t(~0) >> (std::numeric_limits<size_t>::digits - _BITS);
            size_t            _live : 1;                 // Currently in use?
            size_t            _succ : 1;                 // Successor exists?
            size_t            _pred : _BITS;             // Predecessor offset
            size_t            _size : _BITS;             // Full size in words
};

/**
 *  @brief      Represents a dead allocation made within some page that is now
 *              binned and awaiting reuse.
 *
 *  @details    Class Dead extends class Live,from which it derives, by adding
 *              pointers to the previous and  next similarly sized dead blocks
 *              that sit awaiting reuse in the same bin. The bin is, in effect
 *              just a pointer to the first dead block on this list. It adds:
 *
 *              - successor()     returns the block immediately following this
 *              one in the same page, or null if this block sits at the end of
 *              the page.
 *
 *              - predecessor()   returns the block immediately preceding this
 *              one in the same page,  or zero if this block sits at the start
 *              of the page.
 *
 *              - split(s)        truncates the block to 's' words and returns
 *              the offcut if successful, or null if there's insufficient room
 *              within the block to create such an offcut.
 *
 *              - merge(s)        extends this block into the storage occupied
 *              by its immediate successor 's'.
 *
 *              - resurrect()     marks the block as now being in use and then
 *              returns its payload.
 *
 *              - reusable()      returns true if the block does not occupy an
 *              entire page, and thus is a suitable candidate for binning.
 *
 *  @author     jbell@paradigm4.com.
 */
class Dead : public Link<Dead,Live>
{
 public:                   // Construction
                              Dead(size_t size)
                               : Link<Dead,Live>(size)   {}

 public:                   // Operations
            bool              reusable()           const {return _succ||_pred;}
            Live*             successor();               // Next block in page
            Live*             predecessor();             // Prev block in page
            Dead*             split(size_t);             // Split from surplus
            void              merge(Dead*);              // Merge w/ successor
            void*             resurrect()                {_live = true;return payload();}
};

/****************************************************************************/

/**
 *  Adjust the pointer 's' by adding (or subtracting) 'words' multiples of the
 *  alignment word size and then casting to the type of object that we believe
 *  actually resides there.
 */
template<class target>
target* delta_cast(void* s,long words)
{
    assert(aligned(s));                                  // Validate alignment

    target* t = reinterpret_cast<target*>(reinterpret_cast<alignment_t*>(s) + words);

    assert(aligned(t));                                  // Validate alignment

    return t;                                            // Return the target
}

/**
 *  Adjust the given payload pointer by rewinding it back past the header, so
 *  as to retreive a pointer to the original allocation header.
 */
template<class type>
type* retrieve(void* payload)
{
    assert(aligned(payload));                            // Validate arguments

    return delta_cast<type>(payload,-type::overhead());  // Rewind past header
}

/**
 *  Return true if the object looks to be in good shape.  Centralizes a number
 *  of consistency checks that would otherwise clutter up the code, and, since
 *  only ever called from within assertions, can be eliminated entirely by the
 *  compiler from the release build.
 */
template<class t,class b>
bool Link<t,b>::consistent() const
{
    assert(aligned(this));                               // Validate alignment
    assert(asWords(sizeof(b)) <= size());                // Account for header

    return true;                                         // Appears to be good
}

/**
 *  Return a pointer to the region of storage that lies immediately beyond the
 *  end of this header object.
 */
template<class t,class b>
void* Link<t,b>::payload()
{
    return delta_cast<t>(this,b::overhead());            // Step past overhead
}

/**
 *  Place 'link' onto the front of the given doubly-linked list.
 */
template<class t,class b>
void Link<t,b>::push(t*& list,t* link)
{
    assert(link!=0 && link->empty());                    // Validate arguments

    if (list != 0)                                       // Is list non empty?
    {
        list->_prev = link;                              // ...aim at new head
        link->_next = list;                              // ...aim at old head
    }

    list = link;                                         // Place link on list
}

/**
 *  Remove 'link' from the given doubly-linked list.
 */
template<class t,class b>
void Link<t,b>::drop(t*& list,t* link)
{
    assert(list!=0 && link!=0);                          // Validate arguments

    if (t* n = link->_next)                              // Followed by a link?
    {
        n->_prev = link->_prev;                          // ...disconnect it
    }

    if (t* p = link->_prev)                              // Preceded by a link?
    {
        p->_next = link->_next;                          // ...disconnect it
    }
    else                                                 // No preceding link
    {
        assert(list == link);                            // ...it's the first

        list = link->_next;                              // ...so step past it
    }

    link->unlink();                                      // Reset the pointers
}

/**
 *  Remove and return the initial link from the given doubly-linked list.
 */
template<class t,class b>
t* Link<t,b>::pop(t*& list)
{
    assert(list != 0);                                   // Validate arguments

    t* head = list;                                      // Save the list head

    if (list = list->_next, list!=0)                     // Followed by a link?
    {
        list->_prev = 0;                                 // ...disconnect it
    }

    head->unlink();                                      // Reset the pointers

    return head;                                         // Return the old head
}

/**
 *  Test the '_live' bit before safely downcasting to a Dead block header.
 */
Dead* Live::dead()
{
    return _live ? 0 : static_cast<Dead*>(this);         // Check the downcast
}

/**
 *  Mark the block as being no longer in use and cast it to back to the larger
 *  Dead block header that we know is really there; this is why we clamped the
 *  size of the original allocation  in doMalloc() to be at least sizeof(Dead)
 *  bytes long, but also represents a small source of waste: this arena can't,
 *  in fact, allocate a block smaller than sizeof(Dead) bytes.
 */
Dead* Live::kill()
{
    assert(_live);                                       // Validate our state

    _live = false;                                       // Mark block as dead

    Dead* d = static_cast<Dead*>(this);                  // Dowcast to 'Dead*'

    d->unlink();                                         // Reset bin pointers

    assert(d->consistent());                             // Check consistency
    return d;                                            // Return the header
}

/**
 *  Update the field '_pred' to point at the given preceding dead block, which
 *  has just been split into two.
 */
void Live::predecessor(Dead* dead)
{
    assert(this  == dead->successor());                  // Dead precedes this
    assert(_pred >= dead->size());                       // Dead has been split

    this->_pred = dead->size() & _BITS_MASK;             // Update its offset
}

/**
 *  Return the block that immediately follows this one in the page, or null if
 *  this block sits at the very end of the page.
 */
Live* Dead::successor()
{
    if (_succ)                                           // Have a successor?
    {
        return delta_cast<Live>(this,_size);             // ...then return it
    }

    return 0;                                            // Has no successor
}

/**
 *  Return the block that this one immediately follows in the page, or null if
 *  this block sits at the very beginning of the page.
 */
Live* Dead::predecessor()
{
    if (_pred)                                           // Has a predecessor?
    {
        return delta_cast<Live>(this,-_pred);            // ...then return it
    }

    return 0;                                            // Has no predecessor
}

/**
 *  Split the block into two: the original, which is truncated to hold exactly
 *  'size' words, and an offcut - its 'successor' - that resides at the end of
 *  the original block and occupies all of the remaining space.
 *
 *  Return the offcut if there is sufficient space available to create one, or
 *  null if there is not.
 */
Dead* Dead::split(size_t size)
{
    assert(this->dead() && size>=Dead::smallest());      // Validate arguments

 /* Would truncating this block to carry a payload of exactly 'size' words
    leave sufficient room for at least the header of a subsequent block?...*/

    if (_size >= size + Dead::smallest())                // Room for an offcut?
    {
        void* v = delta_cast<void>(this,size);           // ...get the new end
        Dead* b = new(v) Dead(_size - size);             // ...carve an offcut

        b->_succ = _succ;                                // ...adopt successor
        b->_pred = size & _BITS_MASK;                    // ...we are its pred

        _size = size & _BITS_MASK;                       // ...shrink original
        _succ = true;                                    // ...has a successor

        if (Live* s = b->successor())                    // Has b a successor?
        {
            s->predecessor(b);                           // ...update its pred
        }

        assert(this->consistent() && b->consistent());   // ...check all is ok
        return b;                                        // ...return offcut b
    }

    return 0;                                            // No room for offcut
}

/**
 *  Grow this block into the space currently occupied by the block immediately
 *  following us within the same page.
 */
void Dead::merge(Dead* block)
{
    assert(this->dead() && block->dead());               // Must both be dead
    assert(block == this->successor());                  // Must be successor

    _size = (_size + block->_size) & _BITS_MASK;         // Swallow the  block
    _succ = block->_succ;                                // Only if he has one

    if (Live* s = block->successor())                    // Has successor too?
    {
        static_cast<Dead*>(s)->_pred = _size;            // ...set predecessor
    }

    assert(this->consistent() && block->consistent());   // Check both look ok
}

/****************************************************************************/
}
/****************************************************************************/

/**
 *  @brief      Adapts Doug Lea's memory allocator to implement an %arena that
 *              supports both recycling and resetting.
 *
 *  @details    Class LeaArena implements a hybrid %arena that allocates large
 *              pages of memory from its parent %arena from which it then sub-
 *              allocates the actual requests for memory, similarly to the way
 *              in which class ScopedArena works, but also accepts requests to
 *              eagerly recycle allocations,  which it handles by placing them
 *              on one of several intrusively linked lists known as 'bins'. In
 *              a sense the class implements a sort 'heap within a heap'.
 *
 *              The design is loosely based upon that of Doug Lea's well known
 *              allocator implementation. In particular, our binning strategy-
 *              the number and sizes of the bins - is adpated from his design,
 *              although we've not yet implemented all of the other heuristics
 *              found in his design, such as using trees to maintain the large
 *              bins in sorted order, nor the use of a 'designated victim' to
 *              optimize the locality of consecutive allocations.
 *
 *  @see        http://g.oswego.edu/dl/html/malloc.html for an overview of the
 *              design from which this implementation derives.
 *
 *  @author     jbell@paradigm4.com.
 */
class LeaArena : public LimitedArena
{
 public:                   // Construction
                              LeaArena(const Options&);
    virtual                  ~LeaArena();

 public:                   // Attributes
    virtual features_t        features()           const;
    virtual void              insert(std::ostream&)const;

 public:                   // Operations
    virtual void              reset();

 public:                   // Implementation
    virtual void*             doMalloc(size_t);
    virtual void              doFree  (void*,size_t);

 private:                  // Implementation
            Dead*             makePage(size_t);
            void              freePage(Page*);
            Dead*             reuse   (size_t&);
            void              merge   (Dead*&);
            void              unbin   (Dead*);
            void              rebin   (Dead*);
    static  size_t            bin     (const Dead*);
    static  size_t            bin     (size_t);

 private:                  // Implementation
            bool              consistent()        const;

 private:                  // Representation
     boost::array<Dead*,128> _bins;                      // The bin array
       std::bitset<     128> _used;                      // The bin usage map
     static size_t     const _size[128];                 // The bin sizes
            size_t     const _pgsz;                      // The page size
            Page*            _page;                      // The page list
};

/**
 *  Construct a resetting %arena that allocates storage o.pagesize() bytes at
 *  a time from the %arena o.%parent().
 */
    LeaArena::LeaArena(const Options& o)
            : LimitedArena(o),
              _pgsz(asWords(o.pagesize())),
              _page(0)
{
    _bins.fill(0);                                       // Clear out the bins
}

/**
 *  Reset the %arena, thus returning any remaining pages to our parent %arena.
 */
    LeaArena::~LeaArena()
{
    this->reset();                                       // Free all our pages
}

/**
 *  Return a bitfield indicating the set of features this %arena supports.
 */
features_t LeaArena::features() const
{
    return finalizing | resetting | recycling;           // Supported features
}

/**
 *  Overrides @a Arena::insert() to emit a few interesting members of our own.
 */
void LeaArena::insert(std::ostream& o) const
{
    LimitedArena::insert(o);                             // First insert base

    o <<",pagesize=" << words_t(_pgsz);                  // Emit the page size
}

/**
 *  Reset the %arena to its originally constructed state,destroying any extant
 *  objects, recycling their underlying storage for use in future allocations,
 *  and resetting the allocation statistics to their default values.
 */
void LeaArena::reset()
{
    _bins.fill(0);                                       // Clear out the bins
    _used.reset();                                       // Sync the usage map

    while (_page != 0)                                   // While pages remain
    {
        freePage(Page::pop(_page));                      // ...free first page
    }

    LimitedArena::reset();                               // Reset statistics

    assert(consistent());                                // Check consistency
}

/**
 *  Allocate 'size' bytes of raw storage.
 *
 *  'size' may not be zero.
 *
 *  The result is correctly aligned to hold one or more 'alignment_t's.
 *
 *  The resulting allocation must eventually be returned to the same %arena by
 *  calling doFree(), and with the same value for 'size'.
 */
void* LeaArena::doMalloc(size_t size)
{
    assert(size != 0);                                   // Validate arguments

    size = asWords(size);                                // Convert into words
    size+= Live::overhead();                             // Account for header

    Dead* b = reuse(size);                               // Pop first suitable

    if (b == 0)                                          // No suitable block?
    {
        b = makePage(size);                              // ...make a huge one
    }

    if (Dead* r = b->split(size))                        // Larger than needed?
    {
        rebin(r);                                        // ...bin the offcut
    }

    assert(size<=b->size());                             // Request satisfied
    assert(consistent());                                // Check consistency

    return b->resurrect();                               // Mark block as live
}

/**
 *  Free the memory that was allocated earlier from the same %arena by calling
 *  malloc() and attempt to recycle it for future reuse to reclaim up to 'size'
 *  bytes of raw storage.  No promise is made as to *when* this memory will be
 *  made available again however: the %arena may, for example, prefer to defer
 *  recycling until a subsequent call to reset() is made.
 */
void LeaArena::doFree(void* payload,size_t)
{
    assert(aligned(payload));                            // Validate arguments

    Dead* b = retrieve<Live>(payload)->kill();           // Rewind past header

    merge(b);                                            // Merge w neighbours

    if (b->reusable())                                   // Has live neighbor?
    {
        rebin(b);                                        // ...rebin the block
    }
    else                                                 // Fills a whole page
    {
        Page* p = retrieve<Page>(b);                     // ...fetch the page

        Page::drop(_page,p);                             // ...drop from list

        freePage(p);                                     // ...release memory
    }

    assert(consistent());                                // Check consistency
}

/**
 *  Allocate a new large page from the parent arena and return its contents as
 *  a single dead block that is allocated within it.
 */
Dead* LeaArena::makePage(size_t size)
{
    assert(size >= Dead::smallest());                    // Validate argument

 /* Round up the page size to the nearest multiple of the requested size.
    This strategy tends to favour subsequent allocations of the same size,
    but may need a little tuning to avoid excessive waste... */

    if (size_t r = _pgsz % size)                         // Gives a remainder?
    {
        size += _pgsz - r;                               // ...round _pgsz up
    }
    else                                                 // Divides pages size
    if (size < _pgsz)                                    // But is it smaller?
    {
        size  = _pgsz;                                   // ...use _pgsz as is
    }

    size_t n = Page::overhead() + size;                  // Add page overhead
    void*  v = LimitedArena::doMalloc(asBytes(n));       // Allocate the page
    Page*  p = new(v) Page(n);                           // Create the header

    Page::push(_page,p);                                 // Push onto the list

 /* Construct a block within 'p' that occupies the entire payload area...*/

    return new(p->payload()) Dead(size);                 // Return page block
}

/**
 *  Return the allocation described the given page to our parent arena.
 */
void LeaArena::freePage(Page* page)
{
    assert(aligned(page));                               // Validate argument

    LimitedArena::doFree(page,asBytes(page->size()));    // Return to parent
}

/**
 *  Check the bins for the first available block that is big enough to satisfy
 *  a requested allocation size of 'size' words and, if found, pop it from the
 *  bin and update 'size' to reflect the actual size of the block we intend to
 *  allocate.
 *
 *  Throughout the code we have been keeping a bit vector of the bins that are
 *  in use for just this purpose: a scan of the map now quickly tells us which
 *  is the next largest bin to pop.
 */
Dead* LeaArena::reuse(size_t& size)
{
    size_t i = bin(size);                                // Find the bin index

    if (i >= _bins.size())                               // Too big for a bin?
    {
        return 0;                                        // ...make a new page
    }

    size = _size[i];                                     // Actual size wanted

    i = _used._Find_next(i-1);                           // Next non empty bin

    if (i >= _bins.size())                               // Failed to find one?
    {
        return 0;                                        // ...make a new page
    }

    Dead* d = Dead::pop(_bins[i]);                       // Pop the first dead

    _used[i] = _bins[i];                                 // Sync the usage map

    assert(d->size() >= size);                           // Confirm big enough
    return d;                                            // So we can reuse it
}

/**
 *  Attempt to merge the given block with those immediate neighbours allocated
 *  within the same page.
 */
void LeaArena::merge(Dead*& block)
{
    assert(aligned(block) && block->dead());             // Validate argument

    if (Live* s = block->successor())                    // Has a successor?
    {
        if (Dead* d = s->dead())                         // ...and it's dead?
        {
            unbin(d);                                    // ....pull from bin
            block->merge(d);                             // ....fuse together
        }
    }

    if (Live* p = block->predecessor())                  // Has a predecessor?
    {
        if (Dead* d = p->dead())                         // ...and it's dead?
        {
            unbin(d);                                    // ....pull from bin
            d->merge(block);                             // ....fuse together
            block = d;                                   // ....the new block
        }
    }
}

/**
 *  Remove the given block from whichever bin it is currently sitting in.
 */
void LeaArena::unbin(Dead* block)
{
    assert(aligned(block) && block->dead());             // Validate argument

    size_t i = bin(block);                               // Calculate its bin

    Dead::drop(_bins[i],block);                          // Drop from the bin

    _used[i] = _bins[i];                                 // Sync the usage map
}

/**
 *  Return the given block to whichever bin it belongs.
 */
void LeaArena::rebin(Dead* block)
{
    assert(aligned(block) && block->dead());             // Validate argument
    assert(block->reusable());                           // Do not bin a page

    size_t i = bin(block);                               // Calculate its bin

    Dead::push(_bins[i],block);                          // Toss into the bin

    _used[i] = true;                                     // Sync the usage map
}

/**
 *  Return the index of the bin in which the dead block 'd' should be placed.
 *
 *  That is, return the greatest 'i' such that d->size() >= _size[i].
 */
size_t LeaArena::bin(const Dead* d)
{
    return std::upper_bound(_size,_size+SCIDB_SIZE(_size),d->size())-_size-1;
}

/**
 *  Return the index of the first bin that is guarenteed to hold blocks all of
 *  which are at least 'size' words or larger.
 *
 *  That is, return the least 'i' such that size <= _size[i].
 */
size_t LeaArena::bin(size_t size)
{
    return std::lower_bound(_size,_size+SCIDB_SIZE(_size),size) - _size;
}

/**
 *  Return true if the object looks to be in good shape.  Centralizes a number
 *  of consistency checks that would otherwise clutter up the code, and, since
 *  only ever called from within assertions, can be eliminated entirely by the
 *  compiler from the release build.
 */
bool LeaArena::consistent() const
{
    assert(LimitedArena::consistent());                  // Check base is good

    for (size_t i=0,e=_bins.size(); i!=e; ++i)           // For every bin ...
    {
        assert(iff(_used[i],_bins[i]!=0));               // ...map is in sync

     /* Check that if the i'th bin has a block in it, then this block is at
        least as big as the bin size and that it is properly binned... */

        if (Dead* d = _bins[i])                          // ...bin has a block?
        {
            assert(d->size() >= _size[i]);               // ....is big enough
            assert(bin(d)    == i);                      // ....in proper bin
            static_cast<void>(d);                        // ....release build
        }
    }

    return true;                                         // Appears to be good
}

/**
 *  The table of bin sizes.
 *
 *  Half our bins hold blocks whose sizes match the bin size exactly. The rest
 *  of the bins handle dead blocks whose sizes fall within a range,  the upper
 *  bound of each range being spaced roughly logarithmically. There is nothing
 *  sacred about these values, however, and one can always tune the entries by
 *  using performance data taken from a run of the system in order to optimize
 *  the distribution of blocks amongst the bins.
 */
const size_t LeaArena::_size[128] =
{
#define bin(bin,     bytes)  (asWords(static_cast<size_t>(bytes)) + Live::overhead())
        bin(  0,        16),
        bin(  1,        24),
        bin(  2,        32),
        bin(  3,        40),
        bin(  4,        48),
        bin(  5,        56),
        bin(  6,        64),
        bin(  7,        72),
        bin(  8,        80),
        bin(  9,        88),
        bin( 10,        96),
        bin( 11,       104),
        bin( 12,       112),
        bin( 13,       120),
        bin( 14,       128),
        bin( 15,       136),
        bin( 16,       144),
        bin( 17,       152),
        bin( 18,       160),
        bin( 19,       168),
        bin( 20,       176),
        bin( 21,       184),
        bin( 22,       192),
        bin( 23,       200),
        bin( 24,       208),
        bin( 25,       216),
        bin( 26,       224),
        bin( 27,       232),
        bin( 28,       240),
        bin( 29,       248),
        bin( 30,       256),
        bin( 31,       264),
        bin( 32,       272),
        bin( 33,       280),
        bin( 34,       288),
        bin( 35,       296),
        bin( 36,       304),
        bin( 27,       312),
        bin( 38,       320),
        bin( 39,       328),
        bin( 40,       336),
        bin( 41,       344),
        bin( 42,       352),
        bin( 43,       360),
        bin( 44,       368),
        bin( 45,       376),
        bin( 46,       384),
        bin( 47,       392),
        bin( 48,       400),
        bin( 49,       408),
        bin( 50,       416),
        bin( 51,       424),
        bin( 52,       432),
        bin( 53,       440),
        bin( 54,       448),
        bin( 55,       456),
        bin( 56,       464),
        bin( 57,       472),
        bin( 58,       480),
        bin( 59,       488),
        bin( 60,       496),
        bin( 61,       504),
        bin( 62,       512),
        bin( 63,       520),
        bin( 64,       576),
        bin( 65,       640),
        bin( 66,       704),
        bin( 67,       768),
        bin( 68,       832),
        bin( 69,       896),
        bin( 70,       960),
        bin( 71,      1024),
        bin( 72,      1088),
        bin( 73,      1152),
        bin( 74,      1216),
        bin( 75,      1280),
        bin( 76,      1344),
        bin( 77,      1408),
        bin( 78,      1472),
        bin( 79,      1536),
        bin( 80,      1600),
        bin( 81,      1664),
        bin( 82,      1728),
        bin( 83,      1792),
        bin( 84,      1856),
        bin( 85,      1920),
        bin( 96,      1984),
        bin( 87,   2.0*KiB),
        bin( 88,   2.2*KiB),
        bin( 89,   2.5*KiB),
        bin( 90,   3.0*KiB),
        bin( 91,   3.5*KiB),
        bin( 92,   4.0*KiB),
        bin( 93,   4.5*KiB),
        bin( 94,   5.0*KiB),
        bin( 95,   5.5*KiB),
        bin( 96,   6.0*KiB),
        bin( 97,   6.5*KiB),
        bin( 98,   7.0*KiB),
        bin( 99,   7.5*KiB),
        bin(100,   8.0*KiB),
        bin(101,   8.5*KiB),
        bin(102,   9.0*KiB),
        bin(103,   9.5*KiB),
        bin(104,  10.0*KiB),
        bin(105,  10.5*KiB),
        bin(106,  12.0*KiB),
        bin(107,  16.0*KiB),
        bin(108,  20.0*KiB),
        bin(109,  24.0*KiB),
        bin(110,  28.0*KiB),
        bin(111,  32.0*KiB),
        bin(112,  36.0*KiB),
        bin(113,  40.0*KiB),
        bin(114,  64.0*KiB),
        bin(115,  96.0*KiB),
        bin(116, 128.0*KiB),
        bin(117, 160.0*KiB),
        bin(118, 256.0*KiB),
        bin(119, 512.0*KiB),
        bin(120,   1.0*MiB),
        bin(121,   2.0*MiB),
        bin(122,   4.0*MiB),
        bin(123,   8.0*MiB),
        bin(124,  16.0*MiB),
        bin(125,  32.0*MiB),
        bin(126,  64.0*MiB),
        bin(127, 128.0*MiB),
#undef  bin
};

/**
 *  Create and return a resetting %arena that constrains the %arena o.%parent()
 *  to allocating at most o.limit() bytes of memory before throwing an arena::
 *  Exhausted exception,  that allocates memory in pages of o.pagesize() bytes
 *  at a time, and that also supports eager recycling of memory alocations.
 */
ArenaPtr newLeaArena(const Options& o)
{
    return std::make_shared<LeaArena>(o);              // Allocate new arena
}

/****************************************************************************/
}}
/****************************************************************************/
