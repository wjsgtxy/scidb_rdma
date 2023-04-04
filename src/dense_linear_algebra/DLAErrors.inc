/* -*- C++ -*-
 *
 * @file   DLAErrors.inc
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 * @author Mike Leibensperger <mjl@paradigm4.com>
 *
 * @brief Error codes for dense linear algebra plugin
 *
 * @description
 * ATTENTION: DO NOT INCLUDE THIS FILE DIRECTLY!
 *
 * Instead you should include some file that includes this file, like DLAErrors.h.
 *
 *  @see http://www.drdobbs.com/the-new-c-x-macros/184401387 .
 */
/*
* BEGIN_COPYRIGHT
*
* Copyright (C) 2015-2019 SciDB, Inc.
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
X(DLA_ERROR1, "Inconsistent data in array bitmap")
X(DLA_ERROR2, "Matrix must contain one attribute")
X(DLA_ERROR3, "Input arrays must have 2 dimensions")
X(DLA_ERROR4, "Matrix dimensions must match: %1%")
X(DLA_ERROR5, "Attribute should have double type")
X(DLA_ERROR6, "Corr accepts 'kendall', 'spearman' and 'pearson' parameters")
X(DLA_ERROR7, "Invalid matrix")
X(DLA_ERROR8, "Attribute should have double type")
X(DLA_ERROR9, "Unbounded arrays not supported")
X(DLA_ERROR10, "Matrix chunk size should match")
X(DLA_ERROR11, "Matrix origin must match")
X(DLA_ERROR12, "Failed to solve the system of linear equations")
X(DLA_ERROR13, "Request for unknown attribute")
X(DLA_ERROR14, "Specified attribute not found in array")
X(DLA_ERROR15, "Ranked attribute cannot be an empty indicator")
X(DLA_ERROR16, "Specified dimension not found in array")
X(DLA_ERROR17, "The number of samples passed to quantile must be at least 1")
X(DLA_ERROR18, "One of the input arrays contain missing observations")
X(DLA_ERROR19, "No complete element pairs")
X(DLA_ERROR20, "ssvdNorm: Matrix must contain 1 attribute")
X(DLA_ERROR21, "ssvdNorm: Matrix (vector) must contain 1 attribute")
X(DLA_ERROR22, "ssvdNorm: Argument #%1% must have exactly two dimensions")
X(DLA_ERROR23, "ssvdNorm: First argument must have same number of rows as first argument")
X(DLA_ERROR24, "ssvdNorm: Second argument must have one column")
X(DLA_ERROR25, "ssvdNorm: Third argument must have one row")
X(DLA_ERROR26, "ssvdNorm: Third argument must have same number of columns as first argument")
X(DLA_ERROR27, "ssvdNorm: Argument #%1% must have type double")
X(DLA_ERROR28, "ssvdNorm: Argument #%1% must not be nullable")
X(DLA_ERROR29, "ssvdNorm: Argument #%1% must be bounded")
X(DLA_ERROR30, "linregr:: final parameter must be \n'coefficients', \n'residuals', \n'multiple R2',"
               "  \n'adjusted R2', \n'F-statistic', \n'p-value', \n'residual stderror', \n'stderror',"
               " \n'tvalue', \n'P-statistic', \n'confidence intervals'")
X(DLA_ERROR31, "logistregr:: last parameter must be 'coefficients' or 'summary'")
X(DLA_ERROR32, "unused message")
X(DLA_ERROR33, "SVD accepts 'left', 'right' and 'values' parameters")
X(DLA_ERROR34, "corr accepts 'kendall', 'spearman' and 'pearson' parameters")
X(DLA_ERROR35, "Singular values overflow")
X(DLA_ERROR36, "Initial matrix is zero")
X(DLA_ERROR37, "Norm of some eigenvectors is equal to 0 during calculation of singular vectors or bidiagonal matrix")
X(DLA_ERROR38, "# of successes + # of failures can not be equal to 0")
X(DLA_ERROR39, "# of successes (failures) can not be less than 0")
X(DLA_ERROR40, "Non-zero chunk overlap is not supported %1%")
X(DLA_ERROR41, "ChunkInterval is too small")
X(DLA_ERROR42, "ChunkInterval is too large")
X(DLA_ERROR43, "array dimensions must be of equal size, temporarily")
X(DLA_ERROR44, "dimensions must start at 0")
X(DLA_WARNING1, "convergence is not reached; iteration limit exceeded")
X(DLA_WARNING2, "rank deficient problem")
X(DLA_WARNING3, "the model is overparameterized and some coefficients are not identifiable")
X(DLA_WARNING4, "the chunkSize is outside the optimal range of %1% to %2%")
