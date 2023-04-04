/* -*- C++ -*-
 *
 *  LongErrors.inc
 *
 *  ATTENTION: DO NOT INCLUDE THIS FILE DIRECTLY!
 *
 *  Instead you should include some file that includes this file, like
 *  ErrorCodes.h.
 *
 *  WHEN EDITING THIS FILE:
 *  1. Remember, there is no space in "cannot".
 *  2. Going forward, code numbers are important for binary compatibility of
 *      client APIs, so please do not renumber.
 *  3. Keep errors numerically sorted by code.
 *  4. Prefer strerror(errno) over raw errno values in error output.
 *
 *  See http://www.drdobbs.com/the-new-c-x-macros/184401387 .
 */
/*
* BEGIN_COPYRIGHT
*
* Copyright (C) 2014-2019 SciDB, Inc.
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

//Name                                        Code    Message text
X(SCIDB_LE_NO_ERROR,                          0,      "Success")
X(SCIDB_LE_UNKNOWN_ERROR,                    -1,      "Unknown error: %1%")
X(SCIDB_LE_ERRNS_ALREADY_REGISTERED,          1,      "Error namespace '%1%' is already registered")
X(SCIDB_LE_BAD_KEYWORD_REGEX,                 2,      "Operator '%1%' keyword '%2%' has illegal syntax: %3%")
X(SCIDB_LE_WRONG_OPERATOR_KWARGS_REGEX,       3,      "Operator '%1%' keyword '%2%' expected more arguments,"
                                                      " pattern \"%3%\"")
X(SCIDB_LE_AGGREGATE_NOT_FOUND,               4,      "Aggregate name '%1%' is not registered in the system")
X(SCIDB_LE_AGGREGATE_DOESNT_SUPPORT_ASTERISK, 5,      "Aggregate '%1%' does not support asterisk")
X(SCIDB_LE_AGGREGATE_DOESNT_SUPPORT_TYPE,     6,      "Aggregate '%1%' does not support input type '%2%'")
X(SCIDB_LE_NO_MEMORY_FOR_VALUE,               8,      "Not enough memory for new value")
X(SCIDB_LE_THREAD_EVENT_ERROR,                9,      "Event %1% operation failed: %2% (%3%)")
X(SCIDB_LE_WRONG_OPERATOR_ARGUMENTS_REGEX,    10,     "Operator '%1%' expected more arguments, pattern \"%2%\"")
X(SCIDB_LE_CANT_LOAD_MODULE,                  11,     "Cannot load module '%1%', dlopen returned '%2%'")
X(SCIDB_LE_WRONG_MODULE_VERSION,              12,     "Plugin version does not match the version of SciDB."
                                                      " The plugin %1% has version %2%.%3%.%4%.%5% but SciDB"
                                                      " version is %6%")
X(SCIDB_LE_CANT_FIND_SYMBOL,                  13,     "Cannot find symbol '%1%', dlsym returned '%2%'")
X(SCIDB_LE_INPUTS_MUST_BE_BEFORE_PARAMS,      14,     "Error in operator '%1%'. All inputs must appear before"
                                                      " other parameters")
X(SCIDB_LE_VAR_MUST_BE_AFTER_PARAMS,          15,     "Error in operator '%1%'. Optional parameters must appear last")
X(SCIDB_LE_CANT_STAT_FILE,                    16,     "Cannot get file status for '%1%': %2% (%3%)")
X(SCIDB_LE_PWRITE_ERROR,                      17,     "pwrite failed to write %1% byte(s) to position"
                                                      " %2% in file %3% with error '%4%' (%5%)")
X(SCIDB_LE_PREAD_ERROR,                       18,     "pread failed to read %1% byte(s) from position"
                                                      " %2% in file %3% with return code %4% and error '%5%'"
                                                      " (%6%)")
X(SCIDB_LE_OPERATOR_NOT_FOUND,                19,     "Operator '%1%' not found")
X(SCIDB_LE_ARRAY_DOESNT_EXIST,                20,     "Array '%1%' does not exist")
X(SCIDB_LE_ARRAYID_DOESNT_EXIST,              21,     "Array with id '%1%' does not exist")
X(SCIDB_LE_CANT_OPEN_FILE,                    22,     "Cannot open file '%1%': %2% (%3%)")
X(SCIDB_LE_INSTANCE_DOESNT_EXIST,             23,     "Instance with id %1% does not exist")
X(SCIDB_LE_ATTRIBUTE_DOESNT_EXIST,            24,     "Attribute with id %1% does not exist in array '%2%'")
X(SCIDB_LE_DUPLICATE_ATTRIBUTE_NAME,          25,     "Cannot create attribute '%1%'; name collides with"
                                                      " existing object")
X(SCIDB_LE_ILLEGAL_OPERATION,                 26,     "Illegal operation: %1%")
X(SCIDB_LE_SEMAPHORE_ERROR,                   27,     "Semaphore %1% operation failed: %2% (%3%)")
X(SCIDB_LE_CANT_REWIND_FILE,                  28,     "Cannot rewind file '%1%' (%2%)")
X(SCIDB_LE_OPERATOR_FAILED,                   29,     "Operator '%1%' failed: %2%")
X(SCIDB_LE_MISSING_CONFIG_OPTION,             30,     "Required configuration option is missing"
                                                      " or its value is empty: %1%")
X(SCIDB_LE_NOT_IMPLEMENTED,                   31,     "Feature '%1%' not yet implemented")
X(SCIDB_LE_NO_CURRENT_ELEMENT,                32,     "No current element")
X(SCIDB_LE_OPERATION_FAILED,                  33,     "Operation '%1%' failed")
X(SCIDB_LE_UNKNOWN_CONFIG_OPTION,             34,     "Unknown configuration option '%1%'")
X(SCIDB_LE_ERROR_NEAR_CONFIG_OPTION,          35,     "Error '%1%' near configuration option '%2%'")
X(SCIDB_LE_ERROR_IN_CONFIGURATION_FILE,       36,     "Error '%1%' in configuration file")
X(SCIDB_LE_FILE_NOT_FOUND,                    37,     "File '%1%' not found")
X(SCIDB_LE_ARRAY_VERSION_DOESNT_EXIST,        38,     "Specified version of array '%1%' does not exist")
X(SCIDB_LE_CANT_SEND_RECEIVE,                 39,     "Cannot %1% network message: %2%.  An instance may be offline.")
X(SCIDB_LE_CONNECTION_ERROR,                  40,     "Error #%1% when connecting to %2%:%3%")
X(SCIDB_LE_BAD_CONFIG_VALUE,                  41,     "Bad value '%1%' for configuration option %2%: %3%")
X(SCIDB_LE_FUNCTION_NOT_FOUND,                42,     "Function '%1%' not found")
X(SCIDB_LE_TYPE_CONVERSION_ERROR,             43,     "Cannot convert type '%1%' into '%2%'")
X(SCIDB_LE_TYPE_CONVERSION_ERROR2,            44,     "Cannot convert '%1%' of type '%2%' into '%3%' (%4%)")
X(SCIDB_LE_REF_NOT_FOUND,                     45,     "Reference '%1%' not found")
X(SCIDB_LE_ERROR_IN_UDF,                      46,     "Error '%1%' in user-defined function '%2%'")
X(SCIDB_LE_UNKNOWN_ERROR_IN_UDF,              47,     "Unknown error in user-defined function '%1%'")
X(SCIDB_LE_TYPE_NOT_REGISTERED,               48,     "Type '%1%' is not registered")
X(SCIDB_LE_TYPE_ALREADY_REGISTERED,           49,     "Type '%1%' is already registered")
X(SCIDB_LE_DIMENSION_NOT_EXIST,               50,     "Dimension '%1%' not found among %2% array dimensions [%3%]")
X(SCIDB_LE_ATTRIBUTE_NOT_EXIST,               51,     "Attribute '%1%' does not exist")
X(SCIDB_LE_CANT_FIND_CONVERTER,               52,     "Cannot find converter from type '%1%' to '%2%'")
X(SCIDB_LE_CANT_FIND_IMPLICIT_CONVERTER,      53,     "Cannot find implicit converter from type '%1%' to '%2%'")
X(SCIDB_LE_PARAMETER_TYPE_ERROR,              55,     "Operator expected expression of type '%1%' but passed"
                                                      " nonconvertible type '%2%'")
X(SCIDB_LE_DISTRIBUTION_SPECIFICATION_ERROR,  56,     "Multi-child operators with specific distribution"
                                                      " requirements not supported")
X(SCIDB_LE_DISTRIBUTION_SPECIFICATION_ERROR2, 57,     "Operators with more than two input arrays may only"
                                                      " require distribution ANY or COLLOCATED")
X(SCIDB_LE_LOGICAL_OP_DOESNT_EXIST,           58,     "Logical operator '%1%' does not exist")
X(SCIDB_LE_PHYSICAL_OP_DOESNT_EXIST,          59,     "Physical operator '%1%' for logical operator '%2%'"
                                                      " does not exist")
X(SCIDB_LE_LOGICAL_OP_ALREADY_REGISTERED,     60,     "Logical operator '%1%' already registered")
X(SCIDB_LE_PHYSICAL_OP_ALREADY_REGISTERED,    61,     "Physical operator '%1%' for logical operator '%2%'"
                                                      " already registered")
X(SCIDB_LE_QUERY_CANCELLED,                   62,     "Query %1% was cancelled")
X(SCIDB_LE_NO_QUORUM,                         63,     "Not enough online instances to execute query: %1%")
X(SCIDB_LE_QUERY_NOT_FOUND,                   64,     "Query %1% not found")
X(SCIDB_LE_QUERY_NOT_FOUND2,                  65,     "Query already deallocated/cancelled")
X(SCIDB_LE_QUERY_WAS_EXECUTED,                66,     "Query was executed previously")
X(SCIDB_LE_MEMORY_ALLOCATION_ERROR,           67,     "Error '%1%' during memory allocation")
X(SCIDB_LE_REDISTRIBUTE_ERROR,                68,     "Attempt to redistribute on undefined partitioning schema")
X(SCIDB_LE_QUERY_PARSING_ERROR,               69,     "Query parser failed with error '%1%'")
X(SCIDB_LE_SUBQUERIES_NOT_SUPPORTED,          70,     "Subqueries inside expressions not supported")
X(SCIDB_LE_WRONG_ASTERISK_USAGE,              71,     "Asterisk can be used only in aggregates")
X(SCIDB_LE_ARRAY_NAME_REQUIRED,               72,     "CREATE ARRAY statement requires array name")
X(SCIDB_LE_ARRAY_ALREADY_EXISTS,              73,     "Array '%1%' already exists")
X(SCIDB_LE_DUPLICATE_DIMENSION_NAME,          74,     "Cannot create dimension '%1%'; name collides with"
                                                      " existing object")
X(SCIDB_LE_REFERENCE_NOT_ALLOWED_IN_DEFAULT,  75,     "References not allowed in DEFAULT clause")
X(SCIDB_LE_NULL_IN_NON_NULLABLE,              76,     "Default value for non-nullable attribute '%1%' cannot be null")
X(SCIDB_LE_COMPRESSOR_DOESNT_EXIST,           77,     "Compressor with name '%1%' does not exist")
X(SCIDB_LE_UNEXPECTED_OPERATOR_ARGUMENT,      78,     "Operator '%1%' does not take an argument but was passed '%2%'")
X(SCIDB_LE_WRONG_OPERATOR_ARGUMENTS_COUNT,    79,     "Operator '%1%' expected %2% argument(s) but was passed %3%")
X(SCIDB_LE_WRONG_OPERATOR_ARGUMENTS_COUNT2,   80,     "Operator '%1%' expected more arguments")
X(SCIDB_LE_WRONG_OPERATOR_ARGUMENTS_COUNT3,   81,     "Operator '%1%' expected %2% argument(s)")
X(SCIDB_LE_WRONG_OPERATOR_ARGUMENT,           82,     "Expected '%1%' in position %2% of operator '%3%' but"
                                                      " found '%4%'")
X(SCIDB_LE_NESTED_INPUT_ARRAY,                83,     "Cannot pass input arrays inside nested parameter lists")
X(SCIDB_LE_NO_MAPPING_ARRAY,                  84,     "There is no mapping array for dimensions '%1%' in array '%2%'")
X(SCIDB_LE_AMBIGUOUS_OPERATOR_PARAMETER,      85,     "Parameter of operator is ambiguous")
X(SCIDB_LE_WRONG_OPERATOR_ARGUMENT2,          86,     "Parameter must be %1%")
X(SCIDB_LE_AMBIGUOUS_ATTRIBUTE,               87,     "Attribute '%1%' is ambiguous")
X(SCIDB_LE_AMBIGUOUS_DIMENSION,               88,     "Dimension '%1%' is ambiguous")
X(SCIDB_LE_UNEXPECTED_OPERATOR_IN_EXPRESSION, 89,     "Array operators cannot be used inside scalar expressions")
X(SCIDB_LE_WRONG_AGGREGATE_ARGUMENTS_COUNT,   90,     "An aggregate call must have exactly one argument")
X(SCIDB_LE_WRONG_AGGREGATE_ARGUMENT,          91,     "An aggregate call must contain a single attribute"
                                                      " reference, SELECT statement or *")
X(SCIDB_LE_REFERENCE_EXPECTED,                92,     "Attribute or dimension reference expected")
X(SCIDB_LE_SORTING_QUIRK_WRONG_USAGE,         93,     "Parser error: incorrect usage of SORT qualifier")
X(SCIDB_LE_ARGUMENT_NESTING,                  94,     "Parameter list nesting is too deep")
X(SCIDB_LE_NAME_MUST_BE_QUALIFIED,            95,     "Name '%1%' must be qualified in this context")
X(SCIDB_LE_NAME_CANT_BE_QUALIFIED,            96,     "Name '%1%.%2%' cannot be qualified in this context")
X(SCIDB_LE_AGGREGATE_EXPECTED,                97,     "SELECT list items must contain a single aggregate if FROM"
                                                      " is omitted")
X(SCIDB_LE_WRONG_AGGREGATE_ARGUMENT2,         98,     "An aggregate call must contain a single attribute"
                                                      " reference or SELECT statement")
X(SCIDB_LE_SINGLE_ATTRIBUTE_IN_INPUT_EXPECTED, 99,    "Input must have one attribute for aggregating in this form")
X(SCIDB_LE_UNKNOWN_ATTRIBUTE_OR_DIMENSION,    100,    "Attribute or dimension reference '%1%' does not exist")
X(SCIDB_LE_AMBIGUOUS_ATTRIBUTE_OR_DIMENSION,  101,    "Attribute or dimension reference '%1%' is ambiguous")
X(SCIDB_LE_INPUT_EXPECTED,                    102,    "Array name, array operator or SELECT statement expected")
X(SCIDB_LE_CAN_NOT_STORE,                     103,    "Cannot STORE or REDIMENSION_STORE output array into '%1%'."
                                                      " Schemas do not conform")
X(SCIDB_LE_AGGREGATE_CANT_BE_NESTED,          104,    "Aggregate calls cannot be nested")
X(SCIDB_LE_UNEXPECTED_SELECT_INSIDE_AGGREGATE,105,    "SELECT inside aggregate not implemented")
X(SCIDB_LE_ITEM_MUST_BE_INSIDE_AGGREGATE,     106,    "SELECT item must be inside aggregate function")
X(SCIDB_LE_ITEM_MUST_BE_INSIDE_AGGREGATE2,    107,    "SELECT item must be constant or be inside aggregate function")
X(SCIDB_LE_WRONG_REGRID_REDIMENSION_SIZES_COUNT,108,  "REGRID and WINDOW clause must contain sizes for each dimension")
X(SCIDB_LE_INJECTED_ERROR,                    109,    "Injected error")
X(SCIDB_LE_ERRNS_CAN_NOT_BE_UNREGISTERED,     110,    "Error namespace '%1%' cannot be unregistered")
X(SCIDB_LE_STORAGE_CLOSE_FAILED,              111,    "Storage closed with errors")
X(SCIDB_LE_UNKNOWN_MESSAGE_TYPE,              112,    "Unknown/unexpected message type '%1%'")
X(SCIDB_LE_IQUERY_PARSER_ERROR,               113,    "Iquery parser error: '%1%'")
X(SCIDB_LE_STORAGE_ALREADY_REGISTERED,        114,    "Instance #%1% already registered in system catalog")
X(SCIDB_LE_STORAGE_NOT_REGISTERED,            115,    "Storage is not registered in system catalog")
X(SCIDB_LE_CANT_ACCEPT_CONNECTION,            116,    "Error #%1% (%2%) when accepting connection")
X(SCIDB_LE_UNKNOWN_MESSAGE_TYPE2,             117,    "Invalid message type for ID")
X(SCIDB_LE_INVALID_SHEDULER_WORK_ITEM,        118,    "Invalid work item for Scheduler")
X(SCIDB_LE_INVALID_SHEDULER_PERIOD,           119,    "Invalid period for Scheduler")
X(SCIDB_LE_CONNECTION_ERROR2,                 120,    "Connection error while sending: %1% (%2%)")
X(SCIDB_LE_CANT_OPEN_PATH,                    121,    "Cannot open path '%1%'")
X(SCIDB_LE_DATAFRAMES_NOT_SUPPORTED,          122,    "Operator '%1%' does not support dataframes")
X(SCIDB_LE_BAD_DATAFRAME_USAGE,               123,    "Bad dataframe usage in operator '%1%': %2%")
X(SCIDB_LE_WRONG_ATTRIBUTE_TYPE,              124,    "Attribute '%1%' has incorrect data type (source: %2%,"
                                                      " destination: %3%)")
X(SCIDB_LE_WRONG_ATTRIBUTE_FLAGS,             125,    "Attribute '%1%' has incorrect properties")
X(SCIDB_LE_WRONG_SOURCE_ATTRIBUTE_TYPE,       126,    "Source attribute '%1%' must be of integral type (but not UINT64)")
X(SCIDB_LE_WRONG_DESTINATION_ATTRIBUTE_TYPE,  128,    "Destination attribute '%1%' must be of type '%2%'")
X(SCIDB_LE_WRONG_DESTINATION_ATTRIBUTE_FLAGS, 129,    "Destination attribute '%1%' must not have special properties")
X(SCIDB_LE_CHUNK_SIZE_LIMIT_EXCEEDED,         130,    "Chunk size %1% exceeds chunk-size-limit %2%")
X(SCIDB_LE_BUFFERMGR_SLOTS_EXCEEDED,          131,    "buffermgr-slots-max exceeded")
X(SCIDB_LE_DIMENSIONS_DONT_MATCH,             132,    "Dimensions '%1%' and '%2%' do not match")
X(SCIDB_LE_UNEXPECTED_DESTINATION_DIMENSION,  133,    "No data provided for destination dimension '%1%'")
X(SCIDB_LE_UNEXPECTED_DESTINATION_ATTRIBUTE,  134,    "No data provided for destination attribute '%1%'")
X(SCIDB_LE_AGGREGATE_RESULT_CANT_BE_TRANSFORMED_TO_DIMENSION, 136,
                                                      "Aggregate result '%1%' cannot be transformed into a dimension")
X(SCIDB_LE_CHUNK_WRONG_ITERATION_MODE,        137,    "Operator does not support the requested chunk iteration mode")
X(SCIDB_LE_SUBSTITUTE_ERROR1,                 138,    "The substitute array '%1%' must have only one dimension")
X(SCIDB_LE_SUBSTITUTE_ERROR2,                 139,    "The substitute array '%1%' must have only one attribute")
X(SCIDB_LE_SUBSTITUTE_ERROR4,                 141,    "The dimension must start at 0")
X(SCIDB_LE_SUBSTITUTE_ERROR5,                 142,    "Input attribute '%1%' does not match the data type of '%2%'")
X(SCIDB_LE_DIMENSION_COUNT_MISMATCH,          143,    "Dimension counts for operator '%1%' do not match:"
                                                      "  [%2%] != [%3%]")
X(SCIDB_LE_START_INDEX_MISMATCH,              144,    "Array dimensions have different start indices: %1%")
X(SCIDB_LE_LIST_ERROR1,                       146,    "Bad 'list' operator parameter, run \"help('list')\""
                                                      " to see legal values")
X(SCIDB_LE_CANT_CREATE_DIRECTORY,             148,    "Cannot create directory '%1%': %2%")
X(SCIDB_LE_EXPLAIN_ERROR1,                    149,    "Operator '_explain_physical' accepts 1 or 2 parameters only")
X(SCIDB_LE_EXPLAIN_ERROR2,                    150,    "Second parameter of '_explain_logical' operator must be"
                                                      " 'aql' or 'afl'")
X(SCIDB_LE_OPERATION_NOT_FOUND,               151,    "Operation '%1%' not defined for type '%2%'")
X(SCIDB_LE_NO_MAPPING_FOR_COORDINATE,         152,    "No mapping for such coordinate value")
X(SCIDB_LE_UNDEFINED_DISTRIBUTION_CANT_HAVE_MAPPER, 153, "An undefined distribution cannot have a mapper")
X(SCIDB_LE_SPECIFIC_DISTRIBUTION_REQUIRED,    154,    "The chunk distribution requirement 'SpecifyAnyOrder' does"
                                                      " not match what is provided")
X(SCIDB_LE_CANT_GET_SYSTEM_TIME,              155,    "Cannot get system time")
X(SCIDB_LE_TIMER_RETURNED_UNEXPECTED_ERROR,   156,    "Timer (expires_from_now) returned unexpected error '%1%'")
X(SCIDB_LE_TIMER_RETURNED_UNEXPECTED_ERROR2,  157,    "Timer handler returned unexpected error '%1%'")
X(SCIDB_LE_CANT_LINK_DATA_TO_ZERO_BUFFER,     158,    "Cannot link data to zero length buffer")
X(SCIDB_LE_CANT_MODIFY_VALUE_WITH_LINKED_DATA,159,    "Cannot modify Value object with linked data")
X(SCIDB_LE_CANT_SET_VALUE_VECTOR_TO_DEFAULT,  160,    "Cannot set value vector to default")
X(SCIDB_LE_CANT_ADD_NULL_FACTORY,             161,    "Cannot add NULL factory to Aggregate Library")
X(SCIDB_LE_DUPLICATE_AGGREGATE_FACTORY,       162,    "Cannot add duplicate factory to Aggregate Library")
X(SCIDB_LE_CANT_INCREMENT_LOCK,               163,    "Cannot increment lock counter for '%1%' because query"
                                                      " was cancelled")
/* Prefer ...DIMENSION_COUNT_MISMATCH to ...DIMENSIONS_MISMATCH if possible. */
X(SCIDB_LE_DIMENSIONS_MISMATCH,               164,    "Dimension mismatch")
X(SCIDB_LE_CHUNK_ALREADY_EXISTS,              165,    "An existing chunk at position '%1%' cannot be updated")
X(SCIDB_LE_CANT_MERGE_CHUNKS_WITH_VARYING_SIZE, 166,  "Merge of chunks with varying size attribute type is"
                                                      " not supported")
X(SCIDB_LE_CANT_UPDATE_READ_ONLY_CHUNK,       167,    "Chunk is read-only and cannot be updated")
X(SCIDB_LE_TYPE_MISMATCH_BETWEEN_AGGREGATE_AND_CHUNK, 168, "Type mismatch between aggregate and chunk")
X(SCIDB_LE_AGGREGATE_STATE_MUST_BE_NULLABLE,  169,    "Aggregate NULL flag must be TRUE")
X(SCIDB_LE_EXTRACT_EXPECTED_FIXED_SIZE_ATTRIBUTE,170, "Extract attribute should be fixed size")
X(SCIDB_LE_EXTRACT_UNEXPECTED_BOOLEAN_ATTRIBUTE, 171, "Extract attribute cannot have boolean type")
/* Prefer ...DIMENSION_COUNT_MISMATCH to ...WRONG_NUMBER_OF_DIMENSIONS if possible. */
X(SCIDB_LE_WRONG_NUMBER_OF_DIMENSIONS,        172,    "Wrong number of dimensions")
X(SCIDB_LE_UNALIGNED_COORDINATES,             173,    "Coordinates should be aligned on chunk boundary: %1%")
X(SCIDB_LE_CANT_ALLOCATE_MEMORY,              174,    "Failed to allocate memory")
X(SCIDB_LE_NO_CURRENT_CHUNK,                  175,    "No current chunk")
X(SCIDB_LE_CHUNK_OUT_OF_BOUNDARIES,           176,    "Chunk at %1% is outside of array boundaries: %2%")
X(SCIDB_LE_ACCESS_TO_EMPTY_CELL,              177,    "Access to empty cell")
X(SCIDB_LE_ASSIGNING_NULL_TO_NON_NULLABLE,    178,    "Assigning NULL to non-nullable attribute")
X(SCIDB_LE_MEM_ARRAY_THRESHOLD_EXCEEDED,      179,    "mem-array-threshold exceeded")
X(SCIDB_LE_TILE_NOT_ALIGNED,                  180,    "Tile position is not correctly aligned")
X(SCIDB_LE_TILE_MODE_EXPECTED_STRIDE_MAJOR_ORDER,181, "Tile can be written only in stride major order")
X(SCIDB_LE_INVALID_OPERATION_FOR_SEQUENTIAL_MODE,182, "Invalid operation for sequential mode")
X(SCIDB_LE_CANT_UPDATE_BITMAP_IN_TILE_MODE,   183,    "Cannot update bitmap in tile mode")
X(SCIDB_LE_NO_ASSOCIATED_BITMAP_CHUNK,        184,    "No associated bitmap chunk")
X(SCIDB_LE_NO_CURRENT_BITMAP_CHUNK,           185,    "No current bitmap chunk")
X(SCIDB_LE_CANT_FETCH_CHUNK_BODY,             186,    "Failed to fetch chunk body")
X(SCIDB_LE_MULTIDIMENSIONAL_ARRAY_NOT_ALLOWED, 188,   "Multidimensional array is not allowed")
X(SCIDB_LE_OP_SAMPLE_ERROR1,                  191,    "Seed should be a positive integer")
X(SCIDB_LE_OP_SAMPLE_ERROR2,                  192,    "Probability should be in interval (0..1]")
X(SCIDB_LE_INVALID_INSTANCE_ID,               193,    "Invalid instance ID: '%1%'")
X(SCIDB_LE_ARRAYS_NOT_CONFORMANT,             194,    "Arrays are not conformant: %1%") /* What a cop out! */
X(SCIDB_LE_OP_REDIMENSION_ERROR1,             199,    "Destination array for REDIMENSION must have EMPTY flag TRUE")
X(SCIDB_LE_NO_MEMORY_TO_ALLOCATE_MATRIX,      200,    "Not enough memory to allocate matrix")
X(SCIDB_LE_NO_MEMORY_TO_ALLOCATE_VECTOR,      201,    "Not enough memory to allocate vector")
X(SCIDB_LE_CANT_UPDATE_CHUNK,                 202,    "Cannot update disk chunk")
X(SCIDB_LE_INVALID_REDUNDANCY,                203,    "Redundancy %1% should be smaller than the number of servers (in array residency) %2% and no more than %3%")
X(SCIDB_LE_INSTANCE_OFFLINE,                  204,    "Instance #%1% is offline or invalid")
X(SCIDB_LE_DIVISION_BY_ZERO,                  205,    "Division by zero")
X(SCIDB_LE_FAILED_PARSE_STRING,               206,    "Failed to parse string '%1%' as %2%")
X(SCIDB_LE_DIMENSION_EXPECTED,                207,    "Dimension has to be specified")
X(SCIDB_LE_REDISTRIBUTE_AGGREGATE_ERROR1,     209,    "Empty tag attribute should be last and cannot be aggregated")
X(SCIDB_LE_REDISTRIBUTE_ERROR1,               210,    "Empty tag attribute should be last")
X(SCIDB_LE_CANT_MERGE_READONLY_CHUNK,         211,    "Cannot merge read-only chunk")
X(SCIDB_LE_INVALID_SPECIFIED_DATE,            214,    "Specified date is not valid")
X(SCIDB_LE_CANT_CONVERT_NULL,                 215,    "Cannot convert NULL value")
X(SCIDB_LE_LIVENESS_CHANGED,                  216,    "Instance liveness has changed from %1% to %2%")
X(SCIDB_LE_OP_REGRID_ERROR1,                  217,    "Regrid interval should be positive")
X(SCIDB_LE_OP_WINDOW_ERROR2,                  219,    "Window doesn't fit in overlap area")
X(SCIDB_LE_OP_BUILD_ERROR1,                   220,    "Build expression should not access attributes")
X(SCIDB_LE_OP_BUILD_ERROR2,                   221,    "Constructed array should have one attribute")
X(SCIDB_LE_OP_BUILD_ERROR3,                   222,    "Array with open boundary cannot be built")
X(SCIDB_LE_OP_BUILD_SPARSE_ERROR1,            223,    "Build_sparse expression should not access attributes")
X(SCIDB_LE_OP_BUILD_SPARSE_ERROR2,            224,    "Build_sparse predicate should not access attributes")
X(SCIDB_LE_OP_BUILD_SPARSE_ERROR3,            225,    "Constructed array should have one attribute")
X(SCIDB_LE_OP_BUILD_SPARSE_ERROR4,            226,    "Cannot BUILD array with open boundary")
X(SCIDB_LE_ATTR_COUNT_MISMATCH,               227,    "Mismatched number of attributes: %1% != %2%")
X(SCIDB_LE_ATTR_TYPE_MISMATCH,                228,    "Attribute type mismatch for %1% and %2% in schemas "
                                                      " %3% and %4%")
X(SCIDB_LE_OP_CAST_FLAG_MISMATCH,             229,    "Attribute '%1%' flag mismatch (you cannot cast away "
                                                      "nullability)")
X(SCIDB_LE_OP_CAST_SCHEMA_NOT_USED,           230,    "Schema argument '%1%' was not used, which seems wrong")
X(SCIDB_LE_OP_CAST_BAD_DIMENSION_LENGTH,      231,    "Dimension '%1%' length doesn't match")
X(SCIDB_LE_OP_CONCAT_ERROR1,                  235,    "Arrays with open boundary cannot be concatenated")
X(SCIDB_LE_OP_CROSSJOIN_ERROR1,               236,    "Dimension should be specified only once in JOIN ON list")
X(SCIDB_LE_OP_DELDIM_ERROR1,                  237,    "Cannot delete the only dimension of array")
X(SCIDB_LE_OP_DELDIM_ERROR2,                  238,    "Only dimension with length 1 can be removed")
X(SCIDB_LE_OP_REDIMENSION_ERROR2,             239,    "The result of an aggregate cannot be transformed into"
                                                      " a dimension")
X(SCIDB_LE_OP_REDIMENSION_STORE_ERROR1,       240,    "Resulting array has no dimensions")
X(SCIDB_LE_OP_REDIMENSION_STORE_ERROR2,       241,    "Destination array for REDIMENSION should have EMPTY flag TRUE")
X(SCIDB_LE_OP_REDIMENSION_STORE_ERROR5,       244,    "Attribute '%1%' cannot be transformed into a dimension")
X(SCIDB_LE_OP_INPUT_ERROR1,                   245,    "Out-of-sequence chunk in input file")
X(SCIDB_LE_OP_INPUT_ERROR2,                   246,    "Input parser expected '%1%'")
X(SCIDB_LE_OP_INPUT_ERROR3,                   247,    "Input parser expected coordinate")
X(SCIDB_LE_OP_INPUT_ERROR4,                   248,    "Invalid chunk position")
X(SCIDB_LE_DUPLICATE_CHUNK_ADDR,              249,    "Duplicate or out-of-order chunk address '%1%'")
X(SCIDB_LE_OP_INPUT_ERROR6,                   250,    "Too much nesting")
X(SCIDB_LE_OP_INPUT_OOB,                      251,    "Load from %1% file '%2%' at coordinate %3% is"
                                                      " out of chunk bounds: %4%")
X(SCIDB_LE_OP_INPUT_ERROR8,                   252,    "Input parser expected literal")
X(SCIDB_LE_OP_INPUT_ERROR9,                   253,    "No values for empty cells should be defined: only ()"
                                                      " is accepted")
X(SCIDB_LE_OP_INPUT_ERROR10,                  254,    "This version of SciDB does not support this input file")
X(SCIDB_LE_OP_INPUT_ERROR11,                  255,    "Chunk is out of window boundary")
X(SCIDB_LE_OP_INPUT_NEXT_CHUNK_OOB,           256,    "Data exceeds array bounds (stepped out of bounds computing"
                                                      " next chunk location)")
X(SCIDB_LE_OP_INPUT_ERROR13,                  257,    "Unterminated string literal")
X(SCIDB_LE_OP_INPUT_ERROR14,                  258,    "Bad literal")
X(SCIDB_LE_OP_INPUT_TOO_MANY_FIELDS,          259,    "Too many fields in input at position %1%, line %2%,"
                                                      " column %3% (truncating input records to fit)")
X(SCIDB_LE_OP_INPUT_TOO_FEW_FIELDS,           260,    "Too few input record fields at position %1%, line %2%,"
                                                      " column %3%")
X(SCIDB_LE_OP_INPUT_PLOAD_BOUNDED,            261,    "Parallel load requires an unbounded first dimension")
X(SCIDB_LE_OP_INPUT_NO_OVERLAP,               262,    "Data in format '%1%' cannot be read directly using a"
                                                      " schema with dimension overlaps.  Use repart() to add"
                                                      " overlap regions to the input() result")
X(SCIDB_LE_CSV_UNBALANCED_QUOTE,              263,    "Unbalanced quote in CSV input at or near record %1%")
X(SCIDB_LE_CSV_PARSE_ERROR,                   264,    "CSV parsing error at position %1%, record %2%, column %3%: %4%")
X(SCIDB_LE_TSV_PARSE_ERROR,                   265,    "TSV parsing error, raw delimiter must not be escaped")
X(SCIDB_LE_NO_CURRENT_POSITION,               266,    "No current position")
X(SCIDB_LE_OP_MULTIPLY_ERROR2,                267,    "Matrix must have 1 attribute")
X(SCIDB_LE_OP_MULTIPLY_ERROR3,                268,    "Both input arrays to multiply must be 2-dimensional")
X(SCIDB_LE_OP_MULTIPLY_ERROR4,                269,    "Cannot multiply unbounded matrices")
X(SCIDB_LE_OP_MULTIPLY_ERROR5,                270,    "Matrix dimensions must match")
X(SCIDB_LE_OP_MULTIPLY_ERROR6,                271,    "Matrix chunk sizes in the joint dimension must match")
X(SCIDB_LE_OP_MULTIPLY_ERROR7,                272,    "Matrices to be multiplied should have the same attribute type")
X(SCIDB_LE_OP_MULTIPLY_ERROR8,                273,    "Matrices with nullable attributes cannot be multiplied")
X(SCIDB_LE_OP_NORMALIZE_ERROR1,               275,    "Vector must contain 1 attribute")
X(SCIDB_LE_OP_NORMALIZE_ERROR2,               276,    "Array must be 1-D vector")
X(SCIDB_LE_OP_REPART_ERROR3,                  279,    "Mismatched array base")
X(SCIDB_LE_OP_REPART_ERROR4,                  280,    "Mismatched array length")
X(SCIDB_LE_OP_REPART_ERROR5,                  281,    "Array should have fixed lower boundary")
X(SCIDB_LE_OP_RESHAPE_ERROR2,                 283,    "Chunk should have no overlap")
X(SCIDB_LE_OP_RESHAPE_ERROR3,                 284,    "Incompatible array dimensions")
X(SCIDB_LE_OP_REVERSE_ERROR1,                 285,    "Only fixed-size array can be reversed")
X(SCIDB_LE_OP_SLICE_ERROR1,                   286,    "Input array has too few dimensions for slice")
X(SCIDB_LE_OP_SLICE_ERROR2,                   287,    "Slice coordinate is out of bounds")
X(SCIDB_LE_OP_SORT_ERROR2,                    289,    "Column index is out of range")
X(SCIDB_LE_OP_SORT_ERROR3,                    290,    "Cannot jump more than one chunk forward")
X(SCIDB_LE_OP_SORT_ERROR4,                    291,    "Chunk out of window")
X(SCIDB_LE_OP_THIN_ERROR1,                    292,    "Step must be divider of chunk size")
X(SCIDB_LE_OP_THIN_ERROR2,                    293,    "Starting position is out of bounds")
X(SCIDB_LE_OP_THIN_ERROR3,                    294,    "Starting offset must be smaller than step")
X(SCIDB_LE_OP_THIN_ERROR4,                    295,    "Step must be smaller than chunk size")
X(SCIDB_LE_OP_XGRID_ERROR1,                   298,    "Cannot use xgrid on an unbounded array")
X(SCIDB_LE_OP_XGRID_ERROR2,                   299,    "Xgrid arguments must be positive")
X(SCIDB_LE_DIMENSION_START_CANT_BE_UNBOUNDED, 300,    "Dimension start cannot be unbounded")
X(SCIDB_LE_HIGH_SHOULDNT_BE_LESS_LOW,         301,    "The upper boundary of a dimension must be greater than or"
                                                      " equal to the lower boundary")
X(SCIDB_LE_OVERLAP_CANT_BE_LARGER_CHUNK,      302,    "Chunk overlap cannot be larger than chunk size")
X(SCIDB_LE_PIN_RETRIES_EXCEEDED,              303,    "Cannot refresh buffer handle due to many lost races")
X(SCIDB_LE_PIN_UNPIN_DISBALANCE,              304,    "Unbalanced pin/unpin calls, or race condition during close")
X(SCIDB_LE_COMPRESS_METHOD_NOT_DEFINED,       305,    "Compression method is not defined for the chunk")
X(SCIDB_LE_STORAGE_DESCRIPTOR_INVALID_FORMAT, 307,    "Invalid format of storage description file")
X(SCIDB_LE_CHUNK_NOT_FOUND,                   308,    "Chunk not found")
X(SCIDB_LE_NO_FREE_SPACE,                     309,    "No more free space")
X(SCIDB_LE_CHUNK_SIZE_TOO_LARGE,              310,    "Chunk size %1% is larger than segment size %2%")
X(SCIDB_LE_CANT_REALLOCATE_MEMORY,            311,    "Failed to reallocate memory")
X(SCIDB_LE_INVALID_LIVENESS,                  312,    "Invalid liveness")
X(SCIDB_LE_LIVENESS_MISMATCH,                 313,    "Liveness mismatch")
X(SCIDB_LE_MESSAGE_MISSED_QUERY_ID,           314,    "Message has no query ID")
X(SCIDB_LE_MISMATCHED_COORDINATES_IN_PHYSICAL_BOUNDARIES, 316,"Mismatched coordinates in physical boundaries: %1%")
X(SCIDB_LE_MISMATCHED_BOUNDARIES,             318,    "Mismatched boundaries")
X(SCIDB_LE_CANT_CREATE_BOUNDARIES_FROM_INFINITE_ARRAY, 319, "Cannot to create boundaries from infinite array")
X(SCIDB_LE_LIVENESS_EMPTY,                    320,    "Liveness unexpectedly contains no data")
X(SCIDB_LE_DUPLICATE_QUERY_ID,                321,    "Query with same ID '%1%' already exists in the system")
X(SCIDB_LE_CANT_GENERATE_UTC_TIME,            323,    "Unable to generate UTC time")
X(SCIDB_LE_CANT_CREATE_SG_WITH_UNDEFINED_DISTRIBUTION, 324, "Cannot create an SG with undefined distribution")
X(SCIDB_LE_CANT_LOCK_DATABASE,                326,    "Failed to get exclusive access to the database")
X(SCIDB_LE_DATABASE_HEADER_CORRUPTED,         327,    "Database header has been corrupted")
X(SCIDB_LE_CANT_DECOMPRESS_CHUNK,             328,    "Failed to decompress chunk")
X(SCIDB_LE_CHUNK_NOT_PINNED,                  329,    "Accessed chunk should be pinned")
X(SCIDB_LE_INVALID_SYSTEM_ERROR_CODE,         330,    "Internal error, cannot register SciDB error namespace"
                                                      " with error code '%1%': system-defined error codes must"
                                                      " be less than %2%")
X(SCIDB_LE_INVALID_OPERATOR_ARGUMENT,         331,    "Operator '%1%' invalid argument: '%2%'")
X(SCIDB_LE_INVALID_MONTH_REPRESENTATION,      332,    "Invalid month representation: %1%")
X(SCIDB_LE_INVALID_USER_ERROR_CODE,           333,    "Cannot register error namespace '%1%' with error code"
                                                      " '%2%': user-defined error codes must be greater than %3%."
                                                      " Use constant SCIDB_USER_ERROR_CODE_START to define your"
                                                      " first error code.")
X(SCIDB_LE_ATTRIBUTES_MISMATCH,               334,    "Attributes mismatch")
X(SCIDB_LE_INVALID_FUNCTION_ARGUMENT,         335,    "Invalid function argument: %1%")
X(SCIDB_LE_LOGICAL_CHUNK_SIZE_TOO_LARGE,      336,    "Chunk size should not exceed 2^63")
X(SCIDB_LE_FILE_NOT_FOUND_ON_INSTANCES,       337,    "File '%1%' not found on instance(s) %2%")
X(SCIDB_LE_FILE_IMPORT_FAILED,                338,    "Import from file '%1%' (instance %2%) to array '%3%' failed"
                                                      " at line %4%, column %5%, offset %6%, value='%7%': %8%")
X(SCIDB_LE_ACCESS_TO_RAW_CHUNK,               339,    "Access to raw chunk of array '%3%'")
X(SCIDB_LE_INVALID_COMMIT_STATE,              340,    "'%2%' request for query (%1%) is invalid in the current state")
X(SCIDB_LE_CANT_COMPRESS_CHUNK,               341,    "Failed to compress buffer")
X(SCIDB_LE_WRONG_ASTERISK_USAGE2,             342,    "Asterisk for all array versions access can be used"
                                                      " only in SELECT")
X(SCIDB_LE_LE_CANT_ACCESS_INDEX_FOR_ALLVERSIONS,343,  "Only index of specified array version can be accessed")
X(SCIDB_LE_CANT_ACCESS_ARRAY_VERSION,         344,    "Array version inaccessible in this context")
X(SCIDB_LE_CANT_ACCESS_INDEX_ARRAY,           345,    "Index array inaccessible in this context")
X(SCIDB_LE_OP_THIN_ERROR5,                    346,    "Step must be positive number")
X(SCIDB_LE_CANT_UNLOAD_MODULE,                347,    "Cannot unload module '%1%' because it was not loaded")
X(SCIDB_LE_OP_REDIMENSION_STORE_ERROR6,       348,    "Aggregate '%1%' doesn't match with any attribute")
X(SCIDB_LE_SUBSTITUTE_FAILED,                 349,    "Failed to substitute missing reason '%1%'")
X(SCIDB_LE_TRUNCATION,                        351,    "Varying size type with length %1% cannot be converted to"
                                                      " fixed size type with limit %2%")
X(SCIDB_LE_LOOKUP_BAD_PARAM,                  352,    "Number of attributes in pattern array of LOOKUP should"
                                                      " match number of dimensions in source array")
X(SCIDB_LE_INVALID_STORAGE_HEADER,            353,    "This version of SciDB cannot read this storage file"
                                                      " (header magic mismatch)")
X(SCIDB_LE_MISMATCHED_STORAGE_FORMAT_VERSION, 354,    "This version of SciDB cannot read this storage file"
                                                      " (file min %1%, file max %2%, system %3%)")
X(SCIDB_LE_OP_CROSSJOIN_ERROR2,               355,    "Joined dimensions should be specified for both arrays")
X(SCIDB_LE_OP_NORMALIZE_ERROR3,               356,    "Attribute must be of double type")
X(SCIDB_LE_EXPLICIT_EMPTY_FLAG_NOT_ALLOWED,   357,    "Explicit empty flag attribute not allowed")
X(SCIDB_LE_INCONSISTENT_ARRAY_DESC,           359,    "Array descriptor is inconsistent")
X(SCIDB_LE_OP_REDIMENSION_SYNTH_INTERVAL,     360,    "Logical chunk size too big to allow synthentic interval")
X(SCIDB_LE_OP_REDIMENSION_STORE_ERROR7,       361,    "Too many duplicates for REDIMENSION_STORE with synthetic"
                                                      " dimension: increase chunk size")
X(SCIDB_LE_QUERY_ALREADY_COMMITED,            362,    "Operation not possible because the query (%1%) has already"
                                                      " been committed")
X(SCIDB_LE_NETWORK_QUEUE_FULL,                364,    "Network queue is full")
X(SCIDB_LE_INVALID_MESSAGE_FORMAT,            365,    "Invalid message format for type %1%")
X(SCIDB_LE_INVALID_ARRAY_LITERAL,             366,    "Build required constant string when array literal flag is true")
X(SCIDB_LE_INVALID_REDIMENSION_POSITION,      367,    "Cannot redimension coordinates %1%; the position is not"
                                                      " valid in the destination array")
X(SCIDB_LE_SYSCALL_ERROR,                     368,    "Invocation of %1% failed with return code=%2%, errno=%3% (%4%),"
                                                      " arg(s)=%5%")
X(SCIDB_LE_NON_FQ_PATH_ERROR,                 369,    "Path %1% is not fully qualified")
X(SCIDB_LE_UNKNOWN_CTX,                       370,    "Unknown/unexpected context type '%1%'")
X(SCIDB_LE_CANT_LOCK_FILE,                    371,    "Failed to get exclusive access to the file %1%: %2% (%3%)")
X(SCIDB_LE_TEMPLATE_PARSE_ERROR,              372,    "Failed to parse template at position %1%")
X(SCIDB_LE_TEMPLATE_FIXED_SIZE_TYPE,          373,    "Length should not be explicitly specified for fixed size"
                                                      " type %1%")
X(SCIDB_LE_FILE_WRITE_ERROR,                  374,    "Failed to write file: %1% (%2%)")
X(SCIDB_LE_FILE_READ_ERROR,                   375,    "Failed to read file: %1%")
X(SCIDB_LE_UNSUPPORTED_FORMAT,                376,    "Unsupported format: '%1%'")
X(SCIDB_LE_MULTIPLE_DIMENSION_SPECIFICATION,  377,    "Same dimension specified multiple times")
X(SCIDB_LE_NOT_ENOUGH_DIMENSIONS_IN_SPECIFICATION, 378, "Not all dimensions were specified")
X(SCIDB_LE_PARTITION_NAME_NOT_UNIQUE,         379,    "Partition name must be unique")
X(SCIDB_LE_PARTITION_NAME_NOT_SPECIFIED,      380,    "Partition name not specified")
X(SCIDB_LE_UNKNOWN_PARTITION_NAME,            381,    "Unknown partition name")
X(SCIDB_LE_OP_WINDOW_ERROR3,                  382,    "PRECEDING and FOLLOWING must be non-negative values")
X(SCIDB_LE_WRONG_OVER_USAGE,                  383,    "OVER keyword can only be applied to aggregates")
X(SCIDB_LE_MISSING_REASON_OUT_OF_BOUNDS,      384,    "Missing reason cannot be imported in binary file")
X(SCIDB_LE_DDL_CANT_BE_NESTED,                385,    "DDL operators cannot be nested")
X(SCIDB_LE_NO_OPERATOR_RESULT,                386,    "Nested operator did not return any result to parent")
X(SCIDB_LE_CATALOG_NEWER_THAN_SCIDB,          387,    "Cannot connect SciDB with metadata version %1% to catalog"
                                                      " with metadata version %2%")
X(SCIDB_LE_DDL_SHOULDNT_HAVE_INPUTS,          389,    "DDL operators cannot accept input arrays")
X(SCIDB_LE_UNSUPPORTED_INPUT_ARRAY,           390,    "Operator %1% does not support the given virtual input"
                                                      " array. Consider splitting the query and using store()"
                                                      " to create a temporary result")
X(SCIDB_LE_OP_WINDOW_ERROR4,                  391,    "Window size should be greater than 1")
X(SCIDB_LE_OP_INPUT_ERROR15,                  392,    "Expected boolean constant for empty indicator")
X(SCIDB_LE_OP_INPUT_ERROR16,                  393,    "Number of errors exceeds threshold")
X(SCIDB_LE_CHUNK_SIZE_MUST_BE_POSITIVE,       394,    "Chunk size must be positive")
X(SCIDB_LE_OPERATION_FAILED_WITH_ERRNO,       395,    "Operation %1% failed: %2% (%3%)")
X(SCIDB_LE_OP_INPUT_ERROR17,                  396,    "Provided max_errors value must be non-negative")
X(SCIDB_LE_RESERVED_NAME,                     397,    "The name '%1%' is reserved for system use")

// RESOURCE_BUSY is not caused by the user but should be retriable.
// Example:  Temporarily ran out of file descriptors, or suspect a deadlock, or ...
X(SCIDB_LE_RESOURCE_BUSY,                     398,    "Not enough resources: %1%. Try again.")
X(SCIDB_LE_NESTING_PROHIBITED,                399,    "Operator %1% cannot be nested")
X(SCIDB_LE_OP_WINDOW_ERROR5,                  400,    "Algorithm must be one of %1%")
X(SCIDB_LE_OP_WINDOW_ERROR6,                  401,    "Using _materialize op without materialized input")
X(SCIDB_LE_CONSTANT_EXPRESSION_EXPECTED,      402,    "Constant expression expected")
X(SCIDB_LE_TYPE_EXPECTED,                     403,    "Expected constant or expression with type '%1%'")
X(SCIDB_LE_INCORRECT_CHUNK_SIZE,              404,    "Chunk size must be between 1 and '%1%'")
X(SCIDB_LE_INCORRECT_OVERLAP_SIZE,            405,    "Overlap length must be between 0 and %1%")
X(SCIDB_LE_INCORRECT_DIMENSION_BOUNDARY,      406,    "Dimension boundaries must be between '%1%' and '%2%'")
X(SCIDB_LE_WRONG_LANGUAGE_STRING,             407,    "Language string should be AQL or AFL")
X(SCIDB_LE_BAD_SINGLE_ATTRIBUTE_ARRAY,        408,    "Operator '%1%' requires a %3% valued array as argument %2%")
X(SCIDB_LE_BAD_ARRAY_DIMENSIONS,              409,    "Operator '%1%' requires a %3% dimensional array as"
                                                      " argument %2%")
X(SCIDB_LE_BAD_VECTOR_LENGTH,                 410,    "Operator '%1%' requires a vector of length at least %3%"
                                                      " as argument %2%")
X(SCIDB_LE_OP_CAST_BAD_TYPE_CONVERSION,       411,    "Cannot cast attribute '%1%' type from '%2%' to '%3%'")
X(SCIDB_LE_CANNOT_RECOVER_RESTARTABLE_WORK,   412,    "Cannot recover restartable work")
X(SCIDB_LE_CANNOT_MODIFY_ENVIRONMENT,         413,    "Internal error, could not modify environment.")
X(SCIDB_LE_MISMATCHED_OPAQUE_FORMAT_VERSION,  414,    "This version of SciDB cannot read this opaque file"
                                                      " (file version %1%, system version %2%)")
X(SCIDB_LE_CANNOT_ADD_AGGREGATE,              415,    "Cannot add aggregate '%1%'. Scalar function with same"
                                                      " signature already exists")
X(SCIDB_LE_CANNOT_ADD_FUNCTION,               416,    "Cannot add function '%1%'. Aggregate with same signature"
                                                      " already exists")
X(SCIDB_LE_QUERY_TOO_BIG,                     417,    "Query is too big. Query size: %1% bytes."
                                                      " Max allowed size: %2% bytes")
X(SCIDB_LE_TOO_MANY_OPEN_FILES,               418,    "Reached per-process file descriptor limit")
X(SCIDB_LE_OP_MULTIPLY_ERROR9,                419,    "Non-supported attribute type in sparse multiply")
X(SCIDB_LE_OPERATOR_NEEDS_AGGREGATES,         420,    "Operator '%1%' requires one input array and at least"
                                                      " one aggregate call")
X(SCIDB_LE_CUMULATE_NO_OVERLAP,               421,    "For now, cumulate does not handle dimensions with overlaps")
X(SCIDB_LE_CUMULATE_DIM_AFTER_AGGREGATES,     422,    "Operator 'cumulate' requires that the dimension parameter,"
                                                      " if exists, should be the last one")
X(SCIDB_LE_ARENA_OVERFLOWED,                  423,    "A request to allocate memory caused an arithmetic overflow."
                                                      "\n   arena: %1%")
X(SCIDB_LE_ARENA_EXHAUSTED,                   424,    "An attempt was made to allocate more memory than is available."
                                                      "\n   arena  : %1%\n   request: %2%")
X(SCIDB_LE_UNIQ_SINGLE_ATTRIBUTE_ONLY,        425,    "The input to operator 'uniq' must have only one attribute")
X(SCIDB_LE_UNIQ_SINGLE_DIMENSION_AT_ZERO,     426,    "The input to operator 'uniq' must have a single dimension"
                                                      " that starts at 0")
X(SCIDB_LE_CANNOT_PARSE_INTEGER_PARAMETER,    427,    "The parameter '%1%' could not be parsed into an integer value")
X(SCIDB_LE_PARAMETER_NOT_POSITIVE_INTEGER,    428,    "The parameter '%1%' is not valid; must be a positive integer")
X(SCIDB_LE_UNRECOGNIZED_PARAMETER,            429,    "Unrecognized parameter '%1%'")
X(SCIDB_LE_CANNOT_BE_SET_MORE_THAN_ONCE,      430,    "The '%1%' parameter cannot be set more than once")
X(SCIDB_LE_IMPROPER_INDEX_SHAPE,              431,    "The index array must have only one attribute and one dimension")
X(SCIDB_LE_NOT_AN_ATTRIBUTE_IN_INPUT,         432,    "%1% is not an attribute in the input array")
X(SCIDB_LE_ATTRIBUTES_DO_NOT_MATCH_TYPES,     433,    "The type of attribute %1% (%2%) does not match the type"
                                                      " of %3% (%4%)")
X(SCIDB_LE_ATTRIBUTE_CANNOT_BE_NULLABLE,      434,    "The index attribute %1% cannot be nullable")
X(SCIDB_LE_CANNOT_PARSE_BOOLEAN_PARAMETER,    435,    "The parameter '%1%' could not be parsed into a boolean")
X(SCIDB_LE_CHUNK_SEGMENT_SIZE_INCOMPATIBLE,   436,    "Configured chunk segment size: %1% bytes, doesn't match"
                                                      " stored chunk segment size: %2% bytes")
X(SCIDB_LE_CHUNK_SEGMENT_SIZE_ILLEGAL,        437,    "Cannot initialize database with chunk segment size"
                                                      " of %1% bytes")
X(SCIDB_LE_BAD_BLOCK_COMMENT,                 438,    "Block comment '/* ...' has no matching '*/' terminator")
X(SCIDB_LE_BAD_LITERAL_REAL,                  439,    "'%1%' is too large to represent as a double")
X(SCIDB_LE_BAD_LITERAL_INTEGER,               440,    "'%1%' is too large to represent as an integer")
X(SCIDB_LE_NAME_REDEFINED,                    441,    "'%1%' has already been defined")
X(SCIDB_LE_NAME_NOT_APPLICABLE,               442,    "'%1%' is not an entity that can be applied"
                                                      " (such as an operator or function)")
X(SCIDB_LE_NAME_IS_RECURSIVE,                 443,    "'%1%' is defined recursively (recursion not yet supported)")
X(SCIDB_LE_NAME_ARITY_MISMATCH,               444,    "'%1%' is applied to the wrong number of arguments")
X(SCIDB_LE_DATASTORE_GUID_NOT_UNIQUE,         445,    "Attempt to create a DataStore on file '%1%' with"
                                                      " non-unique guid '%2%'")
X(SCIDB_LE_STORAGE_FILE_INVALID_FORMAT,       446,    "The storage file '%1%' has an invalid format")
X(SCIDB_LE_DATASTORE_NOT_FOUND,               447,    "Attempt to access non-existent DataStore with guid '%1%'")
X(SCIDB_LE_DATASTORE_CHUNK_CORRUPTED,         448,    "Chunk header corrupted in DataStore with guid '%1%'"
                                                      " offset '%2%'")
X(SCIDB_LE_DATASTORE_CORRUPT_FREELIST,        449,    "Stored freelist for data store corrupted: '%1%'")
X(SCIDB_LE_AGGREGATION_ORDER_MISMATCH,        450,    "The aggregate '%1%' is order sensitive but the calling"
                                                      " operator does not do aggregation in order")
X(SCIDB_LE_DATASTORE_CHUNK_NOTFREE,           451,    "Attempt to realloc a chunk that was not free."
                                                      " guid '%1%' offset '%2%'")
X(SCIDB_LE_DATASTORE_NOT_EMPTY,               452,    "Attempt to remove non-empty datastore with guid '%1%'")
X(SCIDB_LE_BAD_TEMP_ARRAY,                    453,    "The temporary array '%1%' is no longer available")
X(SCIDB_LE_CROSSBETWEEN_RANGES_ARRAY_ATTRIBUTE_NOT_INT64, 454,
                                                      "Each attribute in rangesArray must have int64 data type")
X(SCIDB_LE_CROSSBETWEEN_NUM_ATTRIBUTES_MISMATCH, 455, "The rangesArray must contain twice as many attributes as"
                                                      " the srcArray has dimensions")
X(SCIDB_LE_CHUNK_POSITION_OUT_OF_ORDER,       456,    "Input chunk position '%1%' violates row-major order")
X(SCIDB_LE_BAD_MISSING_REASON,                457,    "The integer '%1%' is not a valid missing reason code")
X(SCIDB_LE_DATA_COLLISION,                    458,    "Duplicate values at '%1%'")
X(SCIDB_LE_TOO_MANY_CHUNKS_IN_REDIMENSION,    459,    "%1% chunks with overhead of %2% bytes per chunk exceeds"
                                                      " redimension chunk overhead limit (%3% MiB)")
X(SCIDB_LE_CHUNK_TOO_LARGE,                   460,    "Attempt to create chunk of size %1% which exceeds"
                                                      " configured limit (%2% MiB)")
X(SCIDB_LE_CHUNK_TOO_POPULATED,               461,    "Attempt to create chunk with %1% RLE runs, which exceeds"
                                                      " implementation limit (%2% runs)")
X(SCIDB_LE_CHUNK_TOO_HUGE,                    462,    "Attempt to create chunk of size %1% which exceeds"
                                                      " the limitations of the current storage format")
X(SCIDB_LE_BAD_CHUNK_HEADER,                  463,    "Bad chunk header detected in %1%")
X(SCIDB_LE_NUM_CHUNKSIZES_NOT_MATCH_NUM_DIMS, 470,    "In %1%, the number of specified chunk sizes (if any) must"
                                                      " match the number of dimensions")
X(SCIDB_LE_MIN_TOO_SMALL,                     471,    "The minimum value %1% is too small. It should be greater"
                                                      " than or equal to %2%")
X(SCIDB_LE_MAX_TOO_LARGE,                     472,    "The maximum value %1% is too large. It should be less than"
                                                      " or equal to %2%")
X(SCIDB_LE_REPART_FORBIDDEN,                  473,    "Operator '%1%' forbids manual repartition of its input(s)")
X(SCIDB_LE_BAD_EXPLICIT_REPART,               474,    "Operator '%1%' inputs have incorrect chunk intervals"
                                                      " and/or overlaps: %2% vs. %3%")
X(SCIDB_LE_EXPRESSION_HAS_TOO_MANY_OPERANDS,  475,    "A SciDB expression may have no more than %1% operands")
X(SCIDB_LE_QUERY_HAS_TOO_DEEP_NESTING_LEVELS, 476,    "A SciDB query may have no more than %1% levels of nesting")
X(SCIDB_LE_CANNOT_RELOAD_LABELS_IN_PARALLEL,  477,    "Ignoring request for labels in parallel save() output")
X(SCIDB_LE_VALUE_TOO_BIG,                     478,    "Maximum Value size exceeded")
X(SCIDB_LE_AUTOCHUNKING_NOT_SUPPORTED,        479,    "Operator '%1%' does not support automatic chunk interval"
                                                      " selection")
X(SCIDB_LE_ALL_INPUTS_AUTOCHUNKED,            480,    "At least one input to '%1%' must have a specified"
                                                      " chunk size")
X(SCIDB_LE_INSTANCE_PATH_FORMAT_ERROR,        481,    "Instance data path %1% does not conform to"
                                                      " <base-path>/<server-id>/<server-instance-id>")
X(SCIDB_LE_AUTOCHUNKED_EXECUTE_RESULT,        482,    "Operator '%1%' returned array with autochunked"
                                                      " dimensions <>[%2%] from physical execute()")
X(SCIDB_LE_BAD_EXPLICIT_REPART2,              483,    "Operator '%1%' inputs specified as repart(),"
                                                      " but redimension() is needed")
X(SCIDB_LE_DATA_CACHE_EXHAUSTED,              484,    "Unable to clear space in data cache,"
                                                      " cache size exceeds configured limit %1% (bytes)")
X(SCIDB_LE_DISK_INDEX_KEY_EXISTS,             485,    "Key already exists in disk index")
X(SCIDB_LE_DISK_INDEX_KEY_NOT_EXIST,          486,    "Key does not exist in disk index")
X(SCIDB_LE_DISK_INDEX_BUFFER_TOO_SMALL,       487,    "Buffer provided to disk index is too small")
X(SCIDB_LE_READ_ONLY_BUFFER,                  488,    "Attempt to write to read-only buffer")
X(SCIDB_LE_CANNOT_RESOLVE_NAMESPACE,          489,    "Cannot resolve namespace '%1%'")
X(SCIDB_LE_OPERATOR_FORBIDS_KEYWORD_PARAMS,   490,    "Operator '%1%' does not support keyword parameters")
X(SCIDB_LE_DUPLICATE_KEYWORD_PARAM,           491,    "Keyword parameter '%1%' already given for operator '%2%'")
X(SCIDB_LE_UNRECOGNIZED_KEYWORD_PARAM,        492,    "Keyword '%1%' not recognized by this operator")
X(SCIDB_LE_ILLEGAL_ARRAY_AS_KEYWORD_PARAM,    493,    "Keyword parameters may not specify array inputs!")
X(SCIDB_LE_KEYWORD_CONFLICTS_WITH_OPTIONAL,   494,    "Operator '%1%' keyword '%2%' conflicts with optional"
                                                      " parameter %3%")
X(SCIDB_LE_MUTUALLY_EXCLUSIVE_PARAMS,         495,    "Operator '%1%' parameters '%2%' and '%3%' are mutually"
                                                      " exclusive")
X(SCIDB_LE_NAMESPACE_NESTING,                 496,    "Namespace nesting is not supported")
X(SCIDB_LE_BAD_CREDENTIAL,                    497,    "Username or password contains illegal characters")

X(SCIDB_LE_CORRUPT_DATA_ON_DISK,              498,    "Data in DataStore is Corrupt")
X(SCIDB_LE_PARAMETER_OUT_OF_RANGE,            499,    "The value of '%1%' must be between '%2%' and '%3%',"
                                                      " inclusive")
X(SCIDB_LE_INVALID_DISTRIBUTION,              500,    "Distribution '%1%' is not valid in the system")
X(SCIDB_LE_RESERVED_NAMES_IN_SCHEMA,          501,    "Internal dimension or attribute names not allowed in"
                                                      " schema argument for operator '%1%'")
X(SCIDB_LE_CHUNK_INTERVAL_TOO_SMALL,          502,    "Chunk interval must be '%1%' to cover coordinate space"
                                                      " of dimension '%2%'")
X(SCIDB_LE_PROC_STATUS_PARSE_ERROR,           503,    "Cannot parse a '%1%' record in /proc/<pid>/status")

/*
 * Next long error code goes here!
 */

/*
 *  XXX Codes > 1000 are for _____.
 */
X(SCIDB_LE_PG_QUERY_EXECUTION_FAILED,         1001,   "Execution of query '%1%' failed with error %2%")
X(SCIDB_LE_LIBPQ_NOT_THREADSAFE,              1002,   "libpq is not threadsafe. Rebuild it with"
                                                      " --enable-thread-safety")
X(SCIDB_LE_CANT_CONNECT_PG,                   1003,   "Cannot connect to PostgreSQL catalog: '%1%'")
X(SCIDB_LE_UNREACHABLE_CODE,                  1004,   "Fatal: Unreachable code is reached in '%1%'")
X(SCIDB_LE_UNITTEST_FAILED,                   1005,   "Unit test failed! Test = '%1%', failure = '%2%'")
X(SCIDB_LE_NEED_UPGRADE_CONFIRMATION,         1006,   "In order to proceed, the 'enable-catalog-upgrade'"
                                                      " setting must be enabled")
X(SCIDB_LE_ASSERTION_FAILED,                  1007,   "Assertion '%1%' failed at %2%:%3% (%4%)")
X(SCIDB_LE_OPTION_NOT_ALLOWED,                1008,   "Option '%1%' not allowed")

/*
 * Error codes copied from P4.
 */
X(SCIDB_LE_DLA_ERROR13,                       2001,   "Request for unknown attribute")
X(SCIDB_LE_DLA_ERROR14,                       2002,   "Specified attribute not found in array")
X(SCIDB_LE_DLA_ERROR15,                       2003,   "Ranked attribute cannot be an empty indicator")
X(SCIDB_LE_DLA_ERROR16,                       2004,   "Specified dimension not found in array")
X(SCIDB_LE_DLA_ERROR17,                       2005,   "The number of samples passed to quantile must be at least 1")
X(SCIDB_LE_QUANTILE_REQUIRES_BOUNDED_ARRAY,   2006,   "The quantile operator requires groupby dimensions to be"
                                                      " bounded (for now)")

/*
 * User errors
 */
X(SCIDB_LE_PLUGIN_FUNCTION_ACCESS,            2100,   "Unable to access function plugin-name='%1%'")
X(SCIDB_LE_AUTHENTICATION_ERROR,              2101,   "Authentication error reason='%1%'")
X(SCIDB_LE_WAIT_FOR_CLIENT_TIMEOUT,           2102,   "Timeout waiting for client '%1%'")
X(SCIDB_LE_INVALID_BUFFER_LENGTH,             2103,   "Invalid buffer length")
X(SCIDB_LE_CONNECTION_SETUP,                  2104,   "Connection setup error")
X(SCIDB_LE_NULL_CLIENT_RESPONSE,              2105,   "NULL client response - '%1%'")
X(SCIDB_LE_UNKNOWN_REQUEST,                   2106,   "Unknown request")
X(SCIDB_LE_INVALID_OPERATION,                 2107,   "Invalid operation")
X(SCIDB_LE_NOT_UNIQUE,                        2108,   "Item is not unique - name='%1%'")

X(SCIDB_LE_ARRAY_NAME_ALREADY_QUALIFIED,      2200,   "ArrayName already qualified namespace='%1%' array='%2%'")
X(SCIDB_LE_CANNOT_RENAME_ACROSS_NAMESPACES,   2201,   "Cannot rename '%1%' to '%2%' across namespaces")
X(SCIDB_LE_CONFIGURATION_FILE_BLANK_NAME,     2202,   "Configuration filename is blank")
X(SCIDB_LE_CONFIGURATION_FILE_MISSING,        2203,   "Configuration file '%1%' is missing")
X(SCIDB_LE_CONFIGURATION_FILE_MODE,           2204,   "Configuration file '%1%' has incorrect mode")
X(SCIDB_LE_CONFIGURATION_FILE_OWNER,          2205,   "Configuration file '%1%' has incorrect owner")
X(SCIDB_LE_INVALID_PERMISSIONS,               2206,   "Invalid permissions '%1%'")
