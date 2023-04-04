/* -*- C++ -*-
 *
 *  AuthMethods.inc
 *
 *  ATTENTION: DO NOT INCLUDE THIS FILE DIRECTLY!
 *
 *  Instead you should include some file that includes this file, like
 *  Auth.h.
 *
 *  WHEN EDITING THIS FILE:
 *  1. Remember, there is no space in "cannot".
 *  2. Going forward, code numbers are important for binary compatibility of
 *      client APIs, so please do not renumber.
 *  3. Keep methods numerically sorted by code.
 *
 *  See http://www.drdobbs.com/the-new-c-x-macros/184401387 .
 */
/* Name         Code    Tag         Description */
X(AUTH_NONE,    0,      "none",     "Undefined authentication method")
X(AUTH_TRUST,   1,      "trust",    "Trust all clients")
X(AUTH_RAW,     2,      "raw",      "Password hash")  // -a la- release 16.9
X(AUTH_I2I,     3,      "i2i",      "Instance to instance (cluster-internal)")
X(AUTH_MPI,     4,      "mpi",      "MPI slave (cluster-internal)")
X(AUTH_PAM,     5,      "pam",      "Pluggable Authentication Module")
