/* -*- C++ -*-
 *
 *  Permissions.inc
 *
 *  ATTENTION: DO NOT INCLUDE THIS FILE DIRECTLY!
 *
 *  Instead you should include some file that includes this file, like
 *  Rights.h.
 *
 *  WHEN EDITING THIS FILE:
 *  1. Remember, "cannot" is one word.
 *  2. Going forward, bit numbers are important for binary compatibility of
 *      client APIs, so please do not renumber.
 *
 *  See http://www.drdobbs.com/the-new-c-x-macros/184401387 .
 */
/* Name         Bit     Code        Description */
X(DB_ADMIN,     0,      'a',        "Database administrator")
X(DB_OPS,       1,      'o',        "Database operator")
X(NS_ADMIN,     2,      'n',        "Namespace administrator (not implemented)")
X(NS_CREATE,    3,      'c',        "Create arrays in namespace")
X(NS_LIST,      4,      'l',        "List arrays in namespace")
X(NS_READ,      5,      'r',        "Read arrays in namespace")
X(NS_UPDATE,    6,      'u',        "Update arrays in namespace")
X(NS_DELETE,    7,      'd',        "Delete arrays in namespace")
X(NS_EXP1,      8,      '1',        "Experimental permission 1")
X(BITS_MAX,     9,      '.',        "MUST BE LAST")
