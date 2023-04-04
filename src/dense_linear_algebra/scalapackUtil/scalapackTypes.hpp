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
#ifndef SCALAPACK_TYPES__HPP
#define SCALAPACK_TYPES__HPP

///
/// these types are used by:
/// * scalapackFromCpp.hpp, which defines an interface to real ScaLAPACK functions
/// * scalapackEmulation.hpp, which defins a number of work-alikes prefixed with "scidb_"
/// used to manipulate ScaLAPACK array descriptors, and reference local ScaLAPACK memory
/// for converting it to and from arrays.
///
///

#include <system/Constants.h>
#include <util/Utility.h>

#include <cstdint>
#include <iostream>
#include <type_traits>

//
//      This file is a home-brew alternative to using e.g. a BLAST-style interface
//      (which IIRC requires writing wrappers, and is entirely C oriented)
//      to ScaLAPACK, which officially has Fortran interface
//
//      This header makes it possible to pass integer types to the Fortran code without having
//      to take their address ... only arrays are passed by address in this style of the API, which
//      is more natural to C++ programmers.  This avoids frequent awkward convolutions like:
//
//      int_t one = 1;          // awkward way to pass
//      pdgemm_(..., &one, ...) // the integer "1"
//
//      Instead, there is a declaration that would say that the first argument is a reference
//      to the correctly sized integer.  This integer size varies, in some cases, with whether the
//      ScaLAPACK/LAPACK/BLAS was compiled with Fortran INTEGER being 32 or 64 bits, both are
//      possible, depending on the library (MKL for example, provides both forms).
//

//      The following typedefs for int_t exist to match the exact size of the integer types
//      that INTEGER was compiled to be in the Fortran ScaLAPACK, LAPACK, and BLAS that is used.
//      That is because although some variables in the API are specifically sized as INTEGER*32, etc,
//      most integers default to Fortran INTEGER type, and its size depends on whether the Fortran code
//      was compiled in the Intel "lp64" model (where C "int" and Fortran "INTEGER" are 32-bit),
//      or the Intel "ilp64" model (where those types are 64-bit).
//      Right now, the platform BLAS that is installed by default, which SciDB uses, is 32-bit integer.
//      So that is why the typedefs are based on int32_t.
//
//      At some point in the future, in order to handle some very large matrices where the "WORK" array overflows 32-bits on
//      problems that are reasonable for a cluster, we will upgrade to 64 bits, and start compiling and providing an alternative
//      default ScaLAPACK, LAPACK, and BLAS, and then we will be glad there is only one place, this file, where these types
//      are declared.
//
//      We may also start using int64 internally everywhere for these, and only raise an exception right at the point where
//      we need to call the "Slave" API, as this will let us handle overflow cases at runtime, only for the cases of a 32 bit
//      API where they will actually happen, enabling the lower-level slave code to switch between 32- and 64-bit INTEGER libraries
//      without recompilation.  This has its value as well, for certain cases we won't detail -- it is only
//      necessary at this time to realize this is another possible type of solution which utility should be evaluated
//      at the time of consideration of which libraries need to be placed into use, and whether the code should be able to
//      reference both classes of INTEGER libraries without re-compilation.
//

namespace mpi {
    typedef int32_t int_t ; // change type to match how MPI is compiled
}

// for development of scidb to use a 64-bit scalapack, lapack, and blas:
// (i.e. during development of that feature, not for production)
// 1. change the following typedef to int64_t
// 2. change the linking of mpi_slave_{direct,scidb} in src/mpi/slaving/CMakeLists.txt
//    per the example there
// 3. change the compilation of fortran code in src/dense_linear_algebra
//    to add the -fdefault-integer-8 flag in CMakeLists.txt, per the example there
// NOTE: during the period of transition, after modifying any Scalapack functionality,
//       do a full set of checkin tests using both int32_t and int64_t for ScaLAPACK_Int
typedef int32_t ScaLAPACK_Int;  // must be something a C interface can use


// namespace slpp:
//      slpp corresponds to Scalapack Plus Plus meaning an API for scalapack from C++
namespace slpp {

    typedef ScaLAPACK_Int int_t ; // /int32_t or int64_t depending on the ScaLAPACK + lapack + blas used

    // the only standardized aggregate type used in our "blacs" interface so far.
    class context_t {
    public:
        int_t nprow = -1;
        int_t npcol = -1;
        int_t myrow = -1;
        int_t mycol = -1;

        context_t() : nprow(-1), npcol(-1), myrow(-1), mycol(-1) {}      // senseless values

        bool isParticipating() const { return (0 <= myrow && myrow < nprow &&
                                               0 <= mycol && mycol < npcol); }
    };

    // Convenience method to cast larger bit-sized integral values
    // to slpp::int_t. 
    template <typename T>
    inline int_t int_cast(T t)
    {
        if (std::is_same<T, int_t>::value) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
// the diagnostic is to disable the warning that occurs when this path
// is never taken because t and the return type are different
            return t ;
#pragma GCC diagnostic pop
        } else {
            // won't be called when the type is the same
            // but can't call the one with the static assert in
            // that case
            return scidb::safe_static_cast_impl<int_t>(t);
        }
    }

    // the only standardized aggregate type used in our "slpp" interface so far.
    // the capitals here help identify variables that are exactly the same as their
    // Fortran counterparts -- so capitals suggest FORTRAN variables used from C/C++ code.
    class desc_t {
    public:
        int_t   DTYPE ;
        int_t   CTXT ;
        int_t   M, N ;
        int_t   MB, NB ;
        int_t   RSRC, CSRC ;
        int_t   LLD ;     // 9 fields total
        friend  std::ostream& operator<<(std::ostream& os, const desc_t& a);
    };


    inline std::ostream& operator<<(std::ostream& os, const desc_t& a)
    {
        // indent of one space helps set it off from the array info it is typically
        //   nested inside of, when printed
        // an indent argument would be better, but breaks the use of operator<<()
        // a better solution is surely possible.
        os << " DTYPE:" << a.DTYPE << " CTXT:" << a.CTXT << std::endl;
        os << " M:" << a.M << " N:" << a.N << std::endl;
        os << " MB:" << a.MB << " NB:" << a.NB << std::endl;
        os << " RSRC:" << a.RSRC << " CSRC:" << a.CSRC << std::endl;
        os << " LLD:" << a.LLD ;
        return os;
    }

    //
    // SCALAPACK_{MIN,EFFICIENT,MAX}_BLOCK_SIZE:
    //
    // limits of the range of ScaLAPACK MB,NB supported
    // at this time, this also affects the range of sizes of SciDB chunk size accepted by ScaLAPACK
    // operators, which do not want to do repartitions that are currenly prohibitively expensive.

    //
    // rational for the settings below:
    //
    // All three values were 32 through release 13.6  So we want to include 32 in the new range until a period
    // of time after optional deprecation of small sizes.
    //
    // In breif testing conducted on a 6-core Sandy-Bridge-E processor [AVX vector unit] @ 3.2 GHz nominal:
    // using MKL BLAS (double precision), via our gemm() operator:
    //
    // On a test-case-matrix of : MB,NB from 32 to 256 (step 32) BY
    //                            array sizes 5 kibi, 10 kibi, 14 kibi, and 20 kibi [square]
    //
    // the highest, or next-to-highest rates were produced by MB,NB = 192
    // This in agreement with statements by others that the optimal size for MKL will be between 100 and 200.
    //
    // However, this is still too small to overlap with a good chunk size for SciDB, which is a minimum of 1000
    // Additional testing shows that the penalty for sizes above 192  is  <15% up to 35 kibi (square), and has
    // not yet been tested beyond that point due to memory limitations (which can be raised by some forthcoming
    // changes to the Scidb gemm() operator).
    //
    // NOTE: perhaps this could be moved to its own header file, but so far, the places that use the enum
    //       need type types in this file, because MB,NB are used in slpp::desc_t, defined above.
    //
    enum dummy {
        SCALAPACK_MIN_BLOCK_SIZE        = 32,
        SCALAPACK_EFFICIENT_BLOCK_SIZE  = 192,
        SCALAPACK_MAX_BLOCK_SIZE        = 1*scidb::KiB
    };
}



#endif // SCALAPACK_TYPES__HPP
