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
 * @file PluginApi.h
 * @author Mike Leibensperger <mjl@paradigm4.com>
 *
 * @brief Definitions to help with plugin linkage etc.
 *
 * @details For now, only one concept in here: linkage declarations to
 * ensure that certain symbols are accessible to plugins and not
 * hidden by dint of some obscure C++ism.  For example, the typeinfo
 * information required in order to use scidb::Exception-derived
 * objects across the core/plugin boundary can become mysteriously
 * hidden if any subclass is declared in a private context.  Yes,
 * weird.  The definitive explanation can be found at:
 *
 *    https://gcc.gnu.org/wiki/Visibility
 */

#ifndef PLUGIN_API_H
#define PLUGIN_API_H

static_assert(__GNUC__ >= 4, "Stale GCC version does not support visibility");

// Used in the SciDB core to force linkage that's accessible to plugins.
#define PLUGIN_EXPORT   __attribute__ ((visibility ("default")))

// Used in plugins to access SciDB core to force linkage that's
// accessible to plugins.  Yes, for GCC it is coincidentally the same
// as PLUGIN_EXPORT.
#define PLUGIN_IMPORT   __attribute__ ((visibility ("default")))

#endif
