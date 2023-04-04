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

/*
 * This file adds class FlexiblePoint to namespace boost::geometry::model.
 *
 * @author Donghui Zhang
 * @date 2017-2-3
 */

#ifndef FLEXIBLE_POINT_H_
#define FLEXIBLE_POINT_H_

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point.hpp>

namespace boost { namespace geometry
{

// Silence warning C4127: conditional expression is constant
#if defined(_MSC_VER)
#pragma warning(push)
#pragma warning(disable : 4127)
#endif


namespace model
{

/**
 * @brief A variation of boost::geometry::model::point that supports flexible get/set.
 *
 * In boost::geometry::model::point, get() and set() are functions requiring numDims
 * as a template parameter.
 * For our purposes we want to do the following with point p but it is not possible:
 *   \code
 *   for (size_t i=0; i < numDims; ++i) {
 *       auto c = p1.get<i>();
 *       p2.set<i>(c);
 *   }
 *   \endcode
 * With the FlexiblePoint class you can do it:
 *   \code
 *   for (size_t i=0; i < numDims; ++i) {
 *       auto c = p1.get(i);
 *       p2.set(i, c);
 *   }
 *   \endcode
 */
template
<
    typename CoordinateType,
    std::size_t DimensionCount,
    typename CoordinateSystem
>
class FlexiblePoint
{
public:

    /// @brief Default constructor, no initialization
    inline FlexiblePoint()
    {}

    /// @brief Constructor to set one, two or three values
    inline FlexiblePoint(CoordinateType const& v0, CoordinateType const& v1 = 0, CoordinateType const& v2 = 0)
    {
        if (DimensionCount >= 1) m_values[0] = v0;
        if (DimensionCount >= 2) m_values[1] = v1;
        if (DimensionCount >= 3) m_values[2] = v2;
    }

    /// @brief Get a coordinate
    /// @tparam K coordinate to get
    /// @return the coordinate
    template <std::size_t K>
    inline CoordinateType const& get() const
    {
        BOOST_STATIC_ASSERT(K < DimensionCount);
        return m_values[K];
    }

    /// @brief Set a coordinate
    /// @tparam K coordinate to set
    /// @param value value to set
    template <std::size_t K>
    inline void set(CoordinateType const& value)
    {
        BOOST_STATIC_ASSERT(K < DimensionCount);
        m_values[K] = value;
    }

    /// @brief Get a coordinate
    /// @param K coordinate to get
    /// @return the coordinate
    inline CoordinateType const& get(std::size_t K) const
    {
        BOOST_ASSERT(K < DimensionCount);
        return m_values[K];
    }

    /// @brief Set a coordinate
    /// @param K coordinate to set
    /// @param value value to set
    inline void set(std::size_t K, CoordinateType const& value)
    {
        BOOST_ASSERT(K < DimensionCount);
        m_values[K] = value;
    }

private:

    CoordinateType m_values[DimensionCount];
};


} // namespace model

// Adapt the FlexiblePoint to the concept
#ifndef DOXYGEN_NO_TRAITS_SPECIALIZATIONS
namespace traits
{
template
<
    typename CoordinateType,
    std::size_t DimensionCount,
    typename CoordinateSystem
>
struct tag<model::FlexiblePoint<CoordinateType, DimensionCount, CoordinateSystem> >
{
    typedef point_tag type;
};

template
<
    typename CoordinateType,
    std::size_t DimensionCount,
    typename CoordinateSystem
>
struct coordinate_type<model::FlexiblePoint<CoordinateType, DimensionCount, CoordinateSystem> >
{
    typedef CoordinateType type;
};

template
<
    typename CoordinateType,
    std::size_t DimensionCount,
    typename CoordinateSystem
>
struct coordinate_system<model::FlexiblePoint<CoordinateType, DimensionCount, CoordinateSystem> >
{
    typedef CoordinateSystem type;
};

template
<
    typename CoordinateType,
    std::size_t DimensionCount,
    typename CoordinateSystem
>
struct dimension<model::FlexiblePoint<CoordinateType, DimensionCount, CoordinateSystem> >
    : boost::mpl::int_<DimensionCount>
{};

template
<
    typename CoordinateType,
    std::size_t DimensionCount,
    typename CoordinateSystem,
    std::size_t Dimension
>
struct access<model::FlexiblePoint<CoordinateType, DimensionCount, CoordinateSystem>, Dimension>
{
    static inline CoordinateType get(
        model::FlexiblePoint<CoordinateType, DimensionCount, CoordinateSystem> const& p)
    {
        return p.template get<Dimension>();
    }

    static inline void set(
        model::FlexiblePoint<CoordinateType, DimensionCount, CoordinateSystem>& p,
        CoordinateType const& value)
    {
        p.template set<Dimension>(value);
    }
};

} // namespace traits
#endif // DOXYGEN_NO_TRAITS_SPECIALIZATIONS

#if defined(_MSC_VER)
#pragma warning(pop)
#endif

}} // namespace boost::geometry

#endif  // FLEXIBLE_POINT_H_
