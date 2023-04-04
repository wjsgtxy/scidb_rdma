/* -*- C++ -*-
 *
 *  Placeholder.inc
 *
 *  ATTENTION: DO NOT INCLUDE THIS FILE DIRECTLY!
 *
 *  Instead you should include some file that includes this file, like
 *  OperatorParam.h.
 *
 *  WHEN EDITING THIS FILE:
 *  1. Don't change bit numbers, that will break plugins.
 *  2. Leave no bit numbering gaps, else bit-to-name table lookups
 *     fail.  If a line becomes obsolete, rename it UNUSED_<bit#>.
 *
 *  See http://www.drdobbs.com/the-new-c-x-macros/184401387 .
 */
/* Name             Bit    HelpTag              Description */
X(INPUT,            0,     "<input>",           "array name or array operator")
X(ARRAY_NAME,       1,     "<array name>",      "array name")
X(ATTRIBUTE_NAME,   2,     "<attribute name>",  "attribute name")
X(DIMENSION_NAME,   3,     "<dimension name>",  "dimension name")
X(CONSTANT,         4,     "<constant>",        "constant")
X(EXPRESSION,       5,     "<expression>",      "expression")
X(VARIES,           6,     "...",               "...")
X(SCHEMA,           7,     "<schema>",          "schema")
X(AGGREGATE_CALL,   8,     "<aggregate call>",  "aggregate call")
X(NS_NAME,          9,     "<namespace name>",  "namespace name")
X(END_OF_VARIES,    10,    "?EOV?",             "end of arguments")
X(OUTPUT_FLAG,      11,    "?OFLAG?",           "NOT a real placeholder type")
X(UNDEF,            12,    "?UNDEF?",           "Needed for default ctor")
X(PUSH,             13,    "'('",               "start of parameter sublist")
X(POP,              14,    "')'",               "end of parameter sublist")
X(DISTRIBUTION,     15,    "<distribution>",    "array storage distribution")
X(OBJECT_NAME,      16,    "<name>",            "generic name")
