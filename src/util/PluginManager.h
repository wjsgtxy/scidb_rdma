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
 * @file PluginManager.h
 *
 * @author roman.simakov@gmail.com and friends.
 *
 * @brief A manager of plugable modules.
 *
 * Loads modules, finds symbols, manages instances.
 * May have different implementations for different OSs.
 */

#ifndef UTIL_PLUGIN_MANAGER_H_
#define UTIL_PLUGIN_MANAGER_H_

#include <map>

#include "util/Mutex.h"
#include "util/Singleton.h"

namespace scidb
{
class PluginManager : public Singleton<PluginManager>
{
public:
    struct Plugin
    {
        std::string _name;      ///< The plugin name (or "SciDB" for core)
        void*       _handle;    ///< The plugin file handle
        uint32_t    _major;     ///< The major version number
        uint32_t    _minor;     ///< The minor version number
        uint32_t    _patch;     ///< The patch version number
        uint32_t    _build;     ///< The build version number
        std::string _buildType; ///< The build type string (e.g. "Debug")

        Plugin(std::string const& name      = std::string(),
               void*              handle    = NULL,
               uint32_t           major     = 0,
               uint32_t           minor     = 0,
               uint32_t           patch     = 0,
               uint32_t           build     = 0,
               std::string const& buildType = std::string());
    };

    typedef std::function<void(const Plugin&)> Visitor;

private:
    /**
     * The function finds symbol in the given module handle.
     * @param plugin a pointer of module to given by findModule method.
     * @param symbolName a name of symbol to load.
     * @return a pointer to loaded symbol.
     */
    static void* openSymbol(void* plugin, const std::string& symbolName, bool throwException = false);

    /**
     * The function finds module and symbol in it. Currently it's looking for a module in
     * plugins folder specified in config.
     * @param moduleName a name of module to load
     * @param symbolName a name of symbol to load
     * @return a pointer to loaded symbol
     */
    void* findSymbol(const std::string& moduleName, const std::string& symbolName)
    {
        return openSymbol(findModule(moduleName)._handle, symbolName, true);
    }

    /**
     * The function finds module. Currently it's looking for a module in
     * plugins folder specified in config.
     * @param moduleName a name of module to load
     * @return a reference to loaded module descriptor
     */
    Plugin& findModule(const std::string& moduleName, bool* was = NULL);

public:
    PluginManager();
   ~PluginManager();

    /**
     * This method loads module and all user defined objects.
     * @param libraryName a name of library
     * @param isCoordinator true iff called on query coordinator
     * @note If isCoordinator, register the library in the system catalog.
     */
    void loadLibrary(const std::string& libraryName, bool isCoordinator);

    /**
     * This method unloads module and all user defined objects.
     * @param libraryName a name of library
     */
    void unLoadLibrary(const std::string& libraryName);

    /**
     * Load all of the libraries that are registered in the system catalog;
     * to be called on startup.
     */
    void preLoadLibraries();

    /**
     * Take the given visitor to each loaded plugin, as well as to a pseudo-
     * plugin that represents SciDB itself.
     */
    void visitPlugins(const Visitor&) const;

    /**
     *  Get the name of the library that is currently being loaded.
     *  This is a call-back invoked by the loaded plugin, on the same thread.
     *  @return the name of the currently loaded library
     */
    const std::string loadingLibrary() {
        ScopedMutexLock cs (_mutex, PTW_SML_PLUGIN_MANAGER); //copy under lock
        return _loadingLibrary;
    }

    /**
     * Change the directory to load plugins from.
     * @param pluginsDirectory the path
     */
    void setPluginsDirectory(const std::string &pluginsDirectory);

private:
    typedef std::map<std::string,Plugin> Plugins;

private:
    Mutex   mutable _mutex;
    Plugins         _plugins;
    std::string     _loadingLibrary;
    std::string     _pluginsDirectory;
};

} // namespace

#endif /* PLUGINMANAGER_H_ */
