#!/usr/bin/python

# Initialize, start, and stop SciDB in a cluster.

# BEGIN_COPYRIGHT
#
# Copyright (C) 2008-2019 SciDB, Inc.
# All Rights Reserved.
#
# SciDB is free software: you can redistribute it and/or modify
# it under the terms of the AFFERO GNU General Public License as published by
# the Free Software Foundation.
#
# SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
# INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
# the AFFERO GNU General Public License for the complete license terms.
#
# You should have received a copy of the AFFERO GNU General Public License
# along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
#
# END_COPYRIGHT

import errno
import os
import socket
import signal
import subprocess
import sys
import textwrap
import traceback
import argparse
import re
import ConfigParser
import getpass
import multiprocessing
import shutil
import glob

_PGM = None
_DBG = None

# Supported build types.
_BUILD_TYPES = ("RelWithDebInfo", "Assert", "Debug", "CC",
                "Profile", "Release", "Valgrind", "DebugNoAssert")


class AppError(Exception):
    """Base class for all exceptions that halt script execution."""
    pass


def _log_to_file(file_, *args):
    print >>file_, _PGM, ' '.join(map(str, args))
    # stderr is line buffered, no flush needed
    if file_ is not sys.stderr:
        file_.flush()


def printDebug(*args):
    if _DBG:
        _log_to_file(sys.stderr, "DEBUG:", *args)


def printDebugForce(*args):
    _log_to_file(sys.stderr, "DEBUG:", *args)


def printInfo(*args):
    _log_to_file(sys.stdout, *args)


def printWarn(*args):
    _log_to_file(sys.stderr, "WARNING:", *args)


def printError(*args):
    _log_to_file(sys.stderr, "ERROR:", *args)


# borrowed from http://code.activestate.com/recipes/541096/
def confirm(prompt=None, resp=False):
    """prompts for yes or no response from the user. Returns True for yes and
    False for no.
    """
    if prompt is None:
        prompt = 'Confirm'

    if resp:
        prompt = '%s [%s]|%s: ' % (prompt, 'y', 'n')
    else:
        prompt = '%s [%s]|%s: ' % (prompt, 'n', 'y')

    while True:
        ans = raw_input(prompt)
        if not ans:
            return resp

        if ans not in ['y', 'Y', 'n', 'N']:
            print 'please enter y or n.'
            continue

        if ans == 'y' or ans == 'Y':
            return True

        if ans == 'n' or ans == 'N':
            return False
    return False


def parseConfig(filename, section_name, _cache={}):
    """Parse a config file section, caching the result."""
    if (filename, section_name) in _cache:
        return _cache[(filename, section_name)]

    # JK: Please forgive the late import but the correct sys.path
    # doesn't occur reliably until this point.  We need to rethink our
    # python strategy and make sure the appropriate PYTHONPATH is set
    # for setting up, installing, running, and testing.

    from scidblib.scidbctl_common import parse_config_ini
    cfg, servers, iids = parse_config_ini(filename, cluster=section_name)
    cfg['scidb-servers'] = servers
    cfg['scidb-instances'] = iids
    _cache[(filename, section_name)] = cfg
    return cfg


# from http://www.chiark.greenend.org.uk/ucgi/~cjwatson/blosxom/2009-07-02
def subprocess_setup():
    # Python installs a SIGPIPE handler by default. This is usually not what
    # non-Python subprocesses expect.
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)


#
# Execute OS command
# This is a wrapper method for subprocess.Popen()
# If waitFlag=True and raiseOnBadExitCode=True and the exit code of the child process != 0,
# an exception will be raised.
def executeIt(cmdList,
              env=None,
              cwd=None,
              useShell=False,
              cmd=None,
              stdinFile=None,
              stdoutFile=None,
              stderrFile=None,
              waitFlag=True,
              collectOutput=False,
              raiseOnBadExitCode=True):
    ret = 0
    out = ''
    err = ''

    my_env = os.environ.copy()
    if env:
        my_env.update(env)

    if useShell:
        cmdList = [" ".join(cmdList)]

    try:
        stdIn = None
        if stdinFile:
            stdIn = open(stdinFile, "r")

        stdOut = None
        if stdoutFile:
            stdOut = open(stdoutFile, "w")
        elif not waitFlag:
            stdOut = open("/dev/null", "w")

        stdErr = None
        if stderrFile:
            stdErr = open(stderrFile, "w")
        elif not waitFlag:
            stdErr = open("/dev/null", "w")

        if collectOutput:
            if not waitFlag:
                raise RuntimeError(
                    "Inconsistent arguments: waitFlag={0} and collectOutput={1}".format(waitFlag, collectOutput))
            if not stdErr:
                stdErr = subprocess.PIPE
            if not stdOut:
                stdOut = subprocess.PIPE

        printDebug("Executing: " + str(cmdList))

        p = subprocess.Popen(cmdList,
                             preexec_fn=subprocess_setup,
                             env=my_env, cwd=cwd,
                             stdin=stdIn, stderr=stdErr, stdout=stdOut,
                             shell=useShell, executable=cmd)
        if collectOutput:
            out, err = p.communicate()  # collect stdout,stderr, wait

        if waitFlag:
            p.wait()
            ret = p.returncode
            if ret != 0 and raiseOnBadExitCode:
                if err:
                    printError(err)
                raise RuntimeError("Abnormal return code: %s on command %s" % (ret, cmdList))
    finally:
        if stdIn:
            stdIn.close()
        if isinstance(stdOut, file):
            stdOut.close()
        if isinstance(stdErr, file):
            stdErr.close()

    return (ret, out, err)


def rm_rf(path, force=False, throw=True):
    if not force and not confirm("WARNING: about to delete *all* contents of " + path, True):
        if throw:
            raise RuntimeError("Cannot continue without removing the contents of " + path)
        else:
            return
    for dir in glob.glob(path):
        try:
            shutil.rmtree(dir)
        except OSError as e:
            if e.errno == errno.ENOTDIR:
                os.remove(dir)
            else:
                raise e


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST or not os.path.isdir(path):
            raise


def runSetup(scidbEnv,
             force,
             sourcePath,
             buildPath,
             installPath,
             cmake_defs=None,
             name='scidb'):
    oldCmakeCache = os.path.join(sourcePath, "CMakeCache.txt")
    if os.access(oldCmakeCache, os.R_OK):
        printWarn("Deleting old CMakeCache file:" + oldCmakeCache)
        os.remove(oldCmakeCache)

    oldCmakeCache = os.path.join(buildPath, "CMakeCache.txt")
    if os.access(oldCmakeCache, os.R_OK):
        printWarn("Deleting old CMakeCache file:" + oldCmakeCache)
        os.remove(oldCmakeCache)

    build_type = scidbEnv.args.build_type
    rm_rf(os.path.join(buildPath, "*"), force, throw=False)
    mkdir_p(buildPath)
    cmdList = ["cmake",
               "-DRUN_PY=1",
               "-DCMAKE_BUILD_TYPE=%s" % build_type,
               "-DCMAKE_INSTALL_PREFIX=%s" % installPath]

    # add -D's
    if cmake_defs is not None:
        for (key, value) in cmake_defs.items():
            cmdList.append("-D%s=%s" % (key, str(value)))
    cmdList.append(sourcePath)

    executeIt(cmdList, cwd=buildPath)
    # ..............................................................
    # Record the source path inside the file <name>_installpath.txt
    # (saved on the root of the build folder).
    with open(os.path.join(buildPath, name + '_srcpath.txt'), 'w') as fd:
        fd.write(sourcePath)
    # ..............................................................


def setup(scidbEnv):
    runSetup(scidbEnv,
             scidbEnv.args.force,
             scidbEnv.source_path,
             scidbEnv.build_path,
             scidbEnv.install_path)


def getPluginBuildPath(buildPath, name):
    return os.path.join(buildPath, "external_plugins", name)


def pluginSetup(scidbEnv):
    cmake_defs = {'SCIDB_SOURCE_DIR': scidbEnv.source_path,
                  'SCIDB_BUILD_DIR': scidbEnv.build_path}
    pluginBuildPath = getPluginBuildPath(scidbEnv.build_path, scidbEnv.args.name)
    runSetup(scidbEnv,
             scidbEnv.args.force,
             os.path.abspath(scidbEnv.args.path),
             pluginBuildPath,
             scidbEnv.install_path,
             cmake_defs,
             name=scidbEnv.args.name)


def runMake(scidbEnv, makeDir):
    cmdList = ["/usr/bin/make", "--keep-going", "-j%d" % scidbEnv.args.jobs,
               "-C", makeDir, '--no-print-directory']
    if scidbEnv.args.verbose:
        cmdList.extend(['VERBOSE=1'])
    if scidbEnv.args.target:
        cmdList.append(scidbEnv.args.target)
    executeIt(cmdList)


def make(scidbEnv):
    runMake(scidbEnv, scidbEnv.build_path)


def pluginMake(scidbEnv):
    pluginBuildPath = getPluginBuildPath(scidbEnv.build_path, scidbEnv.args.name)
    runMake(scidbEnv, pluginBuildPath)


def make_packages(scidbEnv):
    bin_path = os.path.join(scidbEnv.source_path, "deployment/deploy.sh")

    rm_rf(os.path.join(scidbEnv.args.package_path, "*"), scidbEnv.args.force)

    extra_env = {'SCIDB_BUILD_PATH': scidbEnv.build_path,
                 'SCIDB_SOURCE_PATH': scidbEnv.source_path}
    cmdList = [bin_path,
               "build_fast",
               scidbEnv.args.package_path,
               "paradigm4"]
    executeIt(cmdList,
              env=extra_env,
              cwd=scidbEnv.build_path)


def pluginMakePackages(scidbEnv):
    pluginBuildPath = getPluginBuildPath(scidbEnv.build_path, scidbEnv.args.name)
    pluginPath = getRecordedVar(pluginBuildPath, "CMAKE_HOME_DIRECTORY")
    buildType = getRecordedVar(pluginBuildPath, "CMAKE_BUILD_TYPE")
    packagePath = scidbEnv.args.package_path
    scidbBinPath = scidbEnv.args.scidb_bin_path
    if not packagePath:
        packagePath = os.path.join(pluginBuildPath, "packages")

    rm_rf(os.path.join(packagePath, "*"), scidbEnv.args.force)

    binPath = os.path.join(pluginPath, "deployment/deploy.sh")

    # Usage: ./deployment/deploy.sh build <build_type> <packages_path> <scidb_packages_path> <scidb_bin_path>

    cmdList = [binPath,
               "build", buildType,
               packagePath, scidbEnv.source_path, scidbBinPath]
    executeIt(cmdList,
              cwd=pluginPath)


def confirmRecordedInstallPath(scidbEnv):
    installPath = getRecordedVar(scidbEnv.build_path, "CMAKE_INSTALL_PREFIX")
    if installPath != scidbEnv.install_path:
        raise RuntimeError("Inconsistent install path: recorded by setup=%s vs default/environment=%s" %
                           (installPath, scidbEnv.install_path))


def getRecordedVar(buildPath, varName):
    cmakeConfFile = os.path.join(buildPath, "CMakeCache.txt")
    cmdList = ["grep", "^" + varName, cmakeConfFile]
    ret, out, err = executeIt(cmdList, collectOutput=True)

    out = out.strip()
    printDebug(out + " (raw var line)")
    value = out.partition('=')[2].strip()
    printDebug(value + " (parsed var)")
    return value


def getScidbVersion(scidbEnv):
    bin_path = os.path.join(scidbEnv.source_path, "deployment/deploy.sh")

    cmdList = [bin_path,
               "usage", "2>&1", "|", "grep", "\"SciDB version:\""]
    ret, out, err = executeIt(cmdList,
                              useShell=True,
                              collectOutput=True,
                              cwd=scidbEnv.build_path)
    out = out.strip()
    printDebug(out + " (raw)")
    version = out.split(':')[1]
    printDebug(version + " (parsed)")
    return version.strip()


def version(scidbEnv):
    print getScidbVersion(scidbEnv)


def make_src_package(scidbEnv):
    mkdir_p(scidbEnv.args.package_path)

    scidbEnv.args.target = "src_package"
    runMake(scidbEnv, scidbEnv.build_path)

    tar = os.path.join(scidbEnv.build_path, "scidb-*.tgz")
    shutil.move(tar, scidbEnv.args.package_path)


def cleanup(scidbEnv):
    configFile = getConfigFile(scidbEnv)
    if os.access(configFile, os.R_OK):
        stop(scidbEnv)
        db_name = scidbEnv.args.scidb_name
        dataPath = getDataPath(configFile, db_name)
        rm_rf(dataPath, scidbEnv.args.force)

    rm_rf(scidbEnv.install_path, scidbEnv.args.force)
    rm_rf(scidbEnv.build_path, scidbEnv.args.force)


def generateConfigFile(scidbEnv, db_name, host, port, data_path, instance_num, no_watchdog, configFile):

    # Note: If you change things here, consider also changing the
    # scidb_config function in deployment/deploy.sh (used by CDash).

    if os.access(configFile, os.R_OK):
        os.remove(configFile)

    secondName = ""
    if instance_num > 1:
        secondName = socket.getfqdn()
        if host == secondName:
            secondName = socket.gethostname()
        if host == secondName:
            secondName = socket.gethostbyname(secondName)
    assert host != secondName, "Cannot find host alias for server-1 (server-0=%s)" % host

    with open(configFile, "w") as fd:
        print >>fd, "[%s]" % (db_name)
        if instance_num > 1:
            print >>fd, "server-0=%s,%d" % (host, instance_num / 2 - 1)
            print >>fd, "server-1=%s,%d-%d" % (secondName, instance_num / 2, instance_num - 1)
            print >>fd, "redundancy=1"
        else:
            print >>fd, "server-0=%s,%d" % (host, instance_num - 1)
            # Although redundancy=0 is the default value, run.py requires that
            # a value exist when executing the 'test' subcommand.
            print >>fd, "redundancy=0"
        print >>fd, "db-user=%s" % (db_name)
        print >>fd, "install-root=%s" % (scidbEnv.install_path)
        print >>fd, "pluginsdir=%s" % (os.path.join(scidbEnv.install_path, "lib/scidb/plugins"))
        print >>fd, "logconf=%s" % (os.path.join(scidbEnv.install_path, "share/scidb/log1.properties"))
        print >>fd, "base-path=%s" % (data_path)
        print >>fd, "base-port=%d" % (scidbEnv.args.port)
        print >>fd, "interface=eth0"
        print >>fd, "io-paths-list=/tmp:/dev/shm:/public/data"  # last is for certain harness tests
        if no_watchdog:
            print >>fd, "no-watchdog=true"

        # Note:  you must execute "./run.py stop" and then "./run.py start" after changing
        # this setting if you want it to take effect.
        print >>fd, "\n# \"trust\" - anyone can connect without a password"
        print >>fd, "# \"password\" - password required for database login"
        print >>fd, "# \"pam\" - password required, PAM authentication"
        print >>fd, "security=trust"

        # settings to compare with that resulting from <p4>/cdash2/p4/install.sh
        print >>fd, "datastore-punch-holes=1"
        #TODO, in a later separate commit
        #print >>fd, "liveness-timeout=10"
        #print >>fd, "perf-wait-timing=1"
        print >>fd, "ccm-use-tls=false"
        print >>fd, "ccm-session-time-out=30"
        print >>fd, "ccm-read-time-out=3"


def getConfigFile(scidbEnv):
    return os.path.join(scidbEnv.install_path, "etc", "config.ini")


def getDataPath(configFile, dbName):
    configOpts = parseConfig(configFile, dbName)
    return str(configOpts["base-path"])


def getCoordHost(configFile, dbName):
    configOpts = parseConfig(configFile, dbName)
    return configOpts['scidb-servers'][0].name

def getCoordPort(configFile, dbName):
    configOpts = parseConfig(configFile, dbName)
    return str(configOpts["base-port"])


def getDBUser(configFile, dbName):
    configOpts = parseConfig(configFile, dbName)
    return str(configOpts["db-user"])


def getDBPort(configFile, dbName):
    configOpts = parseConfig(configFile, dbName)
    try:
        return str(configOpts["db-port"])
    except KeyError:
        return None


def getInstances(configFile, dbName):
    configOpts = parseConfig(configFile, dbName)
    return len(configOpts['scidb-instances'])


def getNodes(configFile, dbName):
    configOpts = parseConfig(configFile, dbName)
    return len(configOpts['scidb-servers'])


def getRedundancy(configFile, dbName):
    configOpts = parseConfig(configFile, dbName)
    return int(configOpts['redundancy'])


def getSecurity(configFile, dbName):
    configOpts = parseConfig(configFile, dbName)
    return configOpts['security']


def getDBPassword(scidbEnv, dbname, dbuser):
    # At last we know the source path etc. and can import the PgpassUpdater.
    # Since run.py itself is not installed, we have to jump through this hoop.
    imported = False
    for place in (None,                 # First, try without hacking sys.path
                  os.sep.join((scidbEnv.build_path, "bin")),
                  os.sep.join((scidbEnv.source_path, "utils")),
                  os.sep.join((scidbEnv.install_path, "bin"))):
        if place is not None:
            if place in sys.path:
                continue
            printDebug("Appending", place, "to sys.path")
            sys.path.append(place)
        try:
            from scidblib.pgpass_updater import (
                PgpassUpdater, PgpassError, I_PASS)
            from scidblib.util import getVerifiedPassword
        except ImportError as e:
            printDebug("Delayed import failed:", e)
        else:
            printDebug("Delayed import succeeded")
            imported = True
            break
    pup = None
    config = getConfigFile(scidbEnv)
    dbhost = getCoordHost(config, dbname)
    dbport = getDBPort(config, dbname)
    if not imported:
        printWarn("Cannot import PgpassUpdater, ignoring ~/.pgpass file")
    else:
        # OK, now to use it...
        try:
            pup = PgpassUpdater()
        except PgpassError as e:
            printWarn("PgpassUpdater:", e)
        else:
            printDebug("Checking", pup.filename(), "for",
                       '/'.join(map(str, [dbuser, dbname, dbhost, dbport])))
            found = pup.find(dbuser, dbname, dbhost, dbport)
            if found:
                return found[I_PASS]
    # Still here?  We'll have to prompt for it.
    if imported:
        dbpass = getVerifiedPassword(
            "Enter password for db-user '{0}' [{0}]: ".format(dbuser))
    else:
        dbpass = getpass.getpass(
            "Enter password for db-user '{0}' [{0}]: ".format(dbuser))
    if not dbpass:
        dbpass = dbuser
    if pup and confirm("Update %s?" % pup.filename(), resp=True):
        pup.update(dbuser, dbpass, dbname, dbhost, dbport)
        pup.write_file()
    return dbpass


def removeAlternatives(scidbEnv):
    # remove the local alternatives information
    # effectively removing all previously installed alternatives
    altdir = os.path.join(scidbEnv.install_path, "alternatives*")
    rm_rf(altdir, scidbEnv.args.force, throw=False)


def install(scidbEnv):
    scidbEnv.set_install(True)
    configFile = getConfigFile(scidbEnv)

    if os.access(configFile, os.R_OK):
        stop(scidbEnv)

    #
    # This section is for update-alternatives
    # It MUST be kept in sync with the scidb.spec file
    #
    if scidbEnv.args.light:
        # remove all links to the "alternative" libraries
        # thus, allow for the scidb libraries to be loaded again
        printInfo("Removing linker metadata for alternative plugins/libraries, please confirm")
        removeAlternatives(scidbEnv)

    if not scidbEnv.args.light:
        rm_rf(scidbEnv.install_path, scidbEnv.args.force)

    mkdir_p(scidbEnv.install_path)

    jobs = scidbEnv.args.jobs
    if jobs is None:
        printError("Unable to determine make jobs from {0}".format(scidbEnv.args))
        jobs = 1
    else:
        printDebug("Running make with -j{0}".format(jobs))

    scidbEnv.args.target = "install"
    runMake(scidbEnv, scidbEnv.build_path)

    if scidbEnv.args.light:
        return

    # Generate config.ini or allow for a custom one
    data_path = ""
    if scidbEnv.args.config:
        cmdList = ["cp", scidbEnv.args.config, configFile]
        executeIt(cmdList)
        data_path = getDataPath(configFile, scidbEnv.args.scidb_name)
    else:
        data_path = os.path.join(scidbEnv.stage_path, "DB-" + scidbEnv.args.scidb_name)
        data_path = os.environ.get("SCIDB_DATA_PATH", data_path)
        instance_num = scidbEnv.args.instance_count
        port = scidbEnv.args.port
        host = scidbEnv.args.host
        no_watchdog = scidbEnv.args.nowatchdog
        generateConfigFile(scidbEnv,
                           scidbEnv.args.scidb_name,
                           host,
                           port,
                           data_path,
                           instance_num,
                           no_watchdog,
                           configFile)

    # Copy disable.tests file
    disableFileSrc = os.path.join(scidbEnv.source_path, "tests/harness/testcases/disable.tests")
    disableFileTgt = os.path.join(scidbEnv.build_path, "tests/harness/testcases/disable.tests")
    shutil.copy(disableFileSrc, disableFileTgt)

    # Create log4j config files
    log4jFileSrc = os.path.join(scidbEnv.build_path, "bin/log1.properties")
    log4jFileTgt = os.path.join(scidbEnv.install_path, "share/scidb/log1.properties")
    shutil.copy(log4jFileSrc, log4jFileTgt)

    # Create PG user/role
    collect_output = False
    cmdList = ["sudo", "-u", scidbEnv.args.pg_user,
               os.path.join(scidbEnv.install_path, "bin", "scidbctl.py"),
               '--config', os.path.join(scidbEnv.install_path,
                                        "etc", "config.ini"),
               "init-syscat",
               '--db-password',
               getDBPassword(scidbEnv, scidbEnv.args.scidb_name,
                             getDBUser(configFile, scidbEnv.args.scidb_name)),
               scidbEnv.args.scidb_name]
    collect_output = True

    ret, out , err = executeIt(cmdList, collectOutput=collect_output)
    if out:
        printInfo(out)

    # Initialize SciDB
    scidbEnv.execute_scidbctl_py("init-cluster --force")


def pluginInstall(scidbEnv):
    scidbEnv.set_install(True)
    pluginBuildPath = getPluginBuildPath(scidbEnv.build_path, scidbEnv.args.name)

    if not os.access(pluginBuildPath, os.R_OK):
        raise RuntimeError("Invalid plugin %s build directory %s" % (scidbEnv.args.name, pluginBuildPath))

    pluginInstallPath = os.path.join(scidbEnv.install_path, "lib", "scidb", "plugins")
    if not os.access(pluginInstallPath, os.R_OK):
        raise RuntimeError("Invalid plugin install directory %s" % (pluginInstallPath))

    scidbEnv.args.target = "install"
    runMake(scidbEnv, pluginBuildPath)

    # Copy disable.tests file
    sourceTxtFile = os.path.join(
            pluginBuildPath, scidbEnv.args.name + '_srcpath.txt'
            )
    with open(sourceTxtFile, 'r') as fd:
        contents = fd.read()

    pluginSourceTestsPath = os.path.join(contents.strip(), 'test', 'testcases')

    disableFileSrc = os.path.join(pluginSourceTestsPath, "disable.tests")
    disableFileTgt = os.path.join(pluginBuildPath, "test/testcases/disable.tests")
    shutil.copy(disableFileSrc, disableFileTgt)


def start(scidbEnv):
    scidbEnv.execute_scidbctl_py("start")


def stop(scidbEnv):
    scidbEnv.execute_scidbctl_py("stop")


def initall(scidbEnv):
    print "initializing cluster '{0}' from {1}".format(scidbEnv.args.scidb_name,
                                                       os.path.join(scidbEnv.install_path, "etc/config.ini"))
    scidbEnv.execute_scidbctl_py("init-cluster")


def getScidbPidsCmd(dbName=None):
    cmd = "ps --no-headers -e -o pid,cmd | awk \'{print $1 \" \" $2}\' | grep SciDB-[0-9]"
    if dbName:
        cmd = cmd + " | grep \'%s\'" % (dbName)
    cmd = cmd + " | awk \'{print $1}\'"
    return cmd


def forceStop(scidbEnv):
    cmdList = [getScidbPidsCmd(scidbEnv.args.scidb_name) + ' | xargs kill -9']
    executeIt(cmdList,
              useShell=True,
              cwd=scidbEnv.build_path,
              stdoutFile="/dev/null",
              stderrFile="/dev/null")


def runTests(scidbEnv, testsPath, srcTestsPath, commands=None):
    if commands is None:
        commands = []
    binPath = os.path.join(scidbEnv.install_path, "bin")
    testEnv = os.path.join(scidbEnv.build_path, "tests", "harness", "scidbtestharness_env.sh")
    testBin = os.path.join(scidbEnv.install_path, "bin", "scidbtestharness")

    configFile = getConfigFile(scidbEnv)
    dataPath = getDataPath(configFile, scidbEnv.args.scidb_name)
    coordHost = getCoordHost(configFile, scidbEnv.args.scidb_name)
    coordPort = getCoordPort(configFile, scidbEnv.args.scidb_name)
    dbUser = getDBUser(configFile, scidbEnv.args.scidb_name)
    dbPasswd = getDBPassword(scidbEnv, scidbEnv.args.scidb_name, dbUser)
    instances = getInstances(configFile, scidbEnv.args.scidb_name)
    nodes = getNodes(configFile, scidbEnv.args.scidb_name)
    redundancy = getRedundancy(configFile, scidbEnv.args.scidb_name)
    security = getSecurity(configFile, scidbEnv.args.scidb_name)

    # ...........................................................................
    # Add paths to PYTHONPATH.
    pythonPath = ':'.join(
        [
            os.path.join(scidbEnv.install_path, 'lib'),
            os.path.join(scidbEnv.build_path, 'bin')
        ]
        )
    if ('PYTHONPATH' in os.environ.keys()):
        pythonPath = '${PYTHONPATH}:' + os.path.join(scidbEnv.install_path, 'lib')

    cmdList = ["export", "SCIDB_NAME={0};".format(scidbEnv.args.scidb_name),
               "export", "SCIDB_DB_NAME={0};".format(scidbEnv.args.scidb_name),
               "export", "SCIDB_HOST={0};".format(coordHost),
               "export", "SCIDB_PORT={0};".format(coordPort),
               "export", "SCIDB_BUILD_PATH={0};".format(scidbEnv.build_path),
               "export", "SCIDB_INSTALL_PATH={0};".format(scidbEnv.install_path),
               "export", "SCIDB_SOURCE_PATH={0};".format(scidbEnv.source_path),
               "export", "SCIDB_CONFIG_FILE={0};".format(configFile),
               "export", "SCIDB_CONFIG_USER={0};".format(
                   scidbEnv.args.auth_file if scidbEnv.args.auth_file else ''),
               "export", "SCIDB_DATA_PATH={0};".format(dataPath),
               "export", "SCIDB_DB_USER={0};".format(dbUser),
               "export", "SCIDB_DB_PASSWD={0};".format(dbPasswd),
               "export", "PYTHONPATH={0};".format(pythonPath),
               "export", "PYTHONDONTWRITEBYTECODE=1;"
               ".", testEnv, ";"]

    for cmd in commands:
        cmdList.extend([cmd, ";"])
    # .........................................................................
    # Determine test root directory:
    testRootDir = srcTestsPath
    scratchDir = os.path.join(testsPath, 'testcases')
    disabledTests = os.path.join(srcTestsPath, 'disable.tests')

    # .........................................................................
    # Wipe out *.expected files from the scratch folder to ensure
    # the harness pulls everything from the source tree.
    # .........................................................................
    cmdList.extend(['find', scratchDir, '-name', '"*.expected"', '-print0',
                    '|',
                    'xargs', '--null', 'rm', '-f', ';'
                    ])
    # ...........................................................
    if scidbEnv.args.add_disabled:  # User specified list of files to add to disable.tests.
        if (os.path.isfile(scidbEnv.args.add_disabled)):
            with open(disabledTests, 'r') as fd:
                disabled_contents = fd.read()
            with open(scidbEnv.args.add_disabled, 'r') as fd:
                add_disabled = fd.read()
            new_disabled_file = disabledTests + '.add'
            with open(new_disabled_file, 'w') as fd:
                fd.write(''.join([
                    disabled_contents,
                    '\n',
                    add_disabled
                    ]
                    ))
            disabledTests = os.path.abspath(new_disabled_file)
        else:
            raise RuntimeError('Bad path specified for --add-disabled option: ' + scidbEnv.args.add_disabled + '!')

    if scidbEnv.args.disabled_tests:  # User specified custom disable.tests file.
        if (os.path.isfile(scidbEnv.args.disabled_tests)):
            disabledTests = os.path.abspath(scidbEnv.args.disabled_tests)
        else:
            raise RuntimeError('Bad path specified for --disabled option: ' + scidbEnv.args.disabled_tests + '!')

    # Pardon the late import.  See other comments vis-a-vis PYTHONPATH problems.
    from disable import parse_disable_file
    new_disabled_tests = os.path.join(testsPath, 'testcases', 'disable.tests')
    # The build_type that is used to determine disabled tests is taken from
    # the built/installed BUILD_TYPE.
    # If that is not the case, then the 'tests' and 'plugin_tests' subparsers
    # will need to add 'build_type_argument' as a parent parser.
    build_type = getRecordedVar(scidbEnv.build_path, "CMAKE_BUILD_TYPE")
    parse_disable_file(disable_file=disabledTests,
                       new_disable_file=new_disabled_tests,
                       config_file=configFile,
                       build=build_type)
    if os.path.isfile('/tmp/parsetab.py'):
        os.remove('/tmp/parsetab.py')
    disabledTests = new_disabled_tests

    # create the log directory for running tests
    # NOTE:
    # This is the path passed as the --log-dir option for scidbtestharness
    cmdList.extend(["mkdir", "-p", "${SCIDB_BUILD_PATH}/tests/harness/testcases/log", ";"])

    cmdList.extend([
        "PATH=%s:${PATH}" % (binPath),
        testBin,
        "--port=${IQUERY_PORT}",
        "--connect=${IQUERY_HOST}",
        "--scratch-dir=" + scratchDir,
        "--log-dir=$SCIDB_BUILD_PATH/tests/harness/testcases/log",
        "--disabled-tests=" + disabledTests,
        "--root-dir=" + testRootDir])

    if _DBG:
        cmdList.append('--debug=5')

    if scidbEnv.args.auth_file:
        cmdList.extend(["--auth-file={0}".format(scidbEnv.args.auth_file)])

    if scidbEnv.args.all:
        pass  # nothing to add
    elif scidbEnv.args.test_id:
        cmdList.append("--test-id=" + scidbEnv.args.test_id)
    elif scidbEnv.args.suite_id:
        cmdList.append("--suite-id=" + scidbEnv.args.suite_id)
    else:
        raise RuntimeError("Cannot figure out which tests to run")

    if scidbEnv.args.record:
        cmdList.append("--record")

    cmdList.extend(["|", "tee", "run.tests.log"])

    executeIt(cmdList,
              useShell=True,
              cwd=testsPath)


def tests(scidbEnv):
    srcTestsPath = os.path.join(scidbEnv.source_path, 'tests', 'harness', 'testcases')
    testsPath = os.path.join(scidbEnv.build_path, "tests", "harness")
    runTests(scidbEnv, testsPath, srcTestsPath)


def pluginTests(scidbEnv):
    pluginBuildPath = getPluginBuildPath(scidbEnv.build_path, scidbEnv.args.name)

    if not os.access(pluginBuildPath, os.R_OK):
        raise RuntimeError("Invalid plugin %s build directory %s" % (scidbEnv.args.name, pluginBuildPath))

    pluginInstallPath = os.path.join(scidbEnv.install_path, "lib", "scidb", "plugins")
    if not os.access(pluginInstallPath, os.R_OK):
        raise RuntimeError("Invalid plugin install directory %s" % (pluginInstallPath))

    pluginTestsPath = os.path.join(pluginBuildPath, 'test')

    sourceTxtFile = os.path.join(
            pluginBuildPath, scidbEnv.args.name + '_srcpath.txt'
            )
    with open(sourceTxtFile, 'r') as fd:
        contents = fd.read()

    pluginSourceTestsPath = os.path.join(contents.strip(), 'test', 'testcases')

    pluginTestEnv = os.path.join(pluginBuildPath, 'test', 'scidbtestharness_env.sh')

    commands = []
    if os.access(pluginTestEnv, os.R_OK):
        commands.append(". " + pluginTestEnv)

    runTests(
            scidbEnv,
            pluginTestsPath,
            pluginSourceTestsPath,
            commands
            )


# XXX TODO: support optional CMAKE -D
class SciDBEnv(object):
    """A bunch of paths, and a place to stash parsed arguments."""

    def __init__(self, argv, parsed_args=None):
        self.bin_path = os.path.abspath(os.path.dirname(argv[0]))
        self.source_path = self.bin_path
        self.stage_path = os.path.join(self.source_path, "stage")
        self.build_path = os.environ.get("SCIDB_BUILD_PATH",
                                         os.path.join(self.stage_path, "build"))
        self.install_path = os.environ.get("SCIDB_INSTALL_PATH",
                                           os.path.join(self.stage_path, "install"))
        self.args = parsed_args
        self.installing = False
        printDebug("Source path:", self.source_path)
        printDebug("Build path:", self.build_path)
        printDebug("Install path:", self.install_path)

    def set_install(self, x):
        self.installing = x

    def execute_scidbctl_py(self, command):
        authfile_arg = None
        if 'auth_file' in self.args and self.args.auth_file:
            authfile_arg = ["--auth-file", self.args.auth_file]
        binDir = os.path.join(self.install_path, "bin")
        cmdList = [os.path.join(binDir, 'scidbctl.py'),
                   '--config',
                   os.path.join(self.install_path, "etc", "config.ini")]
        if authfile_arg:
            cmdList.extend(authfile_arg)
        cmdList.extend(command.split())
        printDebug("scidbctl: %s" % (' '.join(cmdList)))
        executeIt(cmdList)


class RunPyUsageFormatter(argparse.RawTextHelpFormatter):
    def __init__(self,
                 prog,
                 indent_increment=2,
                 max_help_position=24,
                 width=None):
        super(RunPyUsageFormatter, self).__init__(prog,
                                                  indent_increment,
                                                  max_help_position)

    def format_description(self, txt, column=None):
        """Wrap txt at column, but keep paragraphs"""
        txt = re.sub('\n +', ' ', txt)
        paragraphs = txt.splitlines(True)
        first_paragraph = textwrap.fill(
            textwrap.dedent(paragraphs[0]),
            # Make 'environment variable defaults' in the epilog help section
            # have the same indent as other help sections:
            #    usage
            #    positional arguments
            #    optional arguments
            initial_indent='' if 'environment variable defaults' in paragraphs[0] else '  ',
            subsequent_indent='   ',
            width=column)
        paragraphs = paragraphs[1:]
        return """{0}\n{1}""".format(first_paragraph, "\n".join(
            textwrap.fill(
                textwrap.dedent(p),
                initial_indent='  ',
                subsequent_indent=' ' * 6,
                width=column) for p in paragraphs))

    def add_text(self, text):
        if text:
            text = self.format_description(text, self._width)
        super(RunPyUsageFormatter, self).add_text(text)

    def add_argument(self, action):
        if action.help:
            action.help = self.format_description(action.help, self._width)
        super(RunPyUsageFormatter, self).add_argument(action)

    def _get_help_string(self, action):
        help = action.help
        if '%(default)' not in action.help:
            if action.default is not argparse.SUPPRESS:
                defaulting_nargs = [argparse.OPTIONAL, argparse.ZERO_OR_MORE]
                if action.option_strings or action.nargs in defaulting_nargs:
                    if hasattr(action, 'original_default') and action.original_default:
                        help += '    (default: %(original_default)s)'
                    elif action.default is not None:
                        help += '    (default: %(default)s)'
        return help


class ArgumentParserWithEnvironment(argparse.ArgumentParser):
    """An adaptation to argparse.ArgumentParser that 'handles' 'env_var'
    settings for parser arguments. This allows for settings to be specified
    with the following order of precedence:
    1. argument specified 'default' is overriden by
    2. environment variable specified by 'env_var' is overriden by
    3. value passed on command line using the argument name

    Usage text (when --help is given) is modified in two ways:
    * By altering the 'help' text given for an argument appending [env ENV_VALUE].
    * Adding a section of 'environment variable defaults:' with the ENV_VALUEs of all
      added arguments.

    NOTE: This functionality is totally accomplished with ConfigArgParser,
          but that requires `pip install [--user] ConfigArgParser` on
          every developer machine...
          ConfigArgParser also provides the ability to find values based
          upon reading a configuration file, where the precedence is between
          items 1 (default) and 2 (environment variable).

    """

    FALSE_VALUES = map(lambda a: a.title(), "false f 0 off".split())
    TRUE_VALUES = map(lambda a: a.title(), "true t 1 on".split())

    def __init__(self,
                 environment_vars=None,
                 formatter_class=RunPyUsageFormatter,
                 **kwargs):
        super(ArgumentParserWithEnvironment, self).__init__(formatter_class=formatter_class, **kwargs)
        self.environment_vars = environment_vars or {}
        if 'parents' in kwargs:
            for p in kwargs['parents']:
                if p.epilog:
                    if self.epilog:
                        self.epilog = '\n\n'.join((self.epilog, p.epilog))
                    else:
                        self.epilog = p.epilog
                if hasattr(p, 'environment_vars'):
                    for key, val in p.environment_vars.items():
                        self.environment_vars[key] = val

    def add_argument(self, *args, **kwargs):
        """
        Add the argument to the parser, but also do the following when an
        env_var is provided:
         1. Add the environment variable and its description to the
            'epilog' (usage text) for the parser
         2. Update the action.default (and action.required) based upon
            the ENV_VAR if  necessary
        """
        kw = dict(kwargs)
        if 'env_var' in kwargs:
            # ArgumentParser.add_argument will puke it you give it
            # a dictionary entry it's not expecting
            del kwargs['env_var']
        action = super(ArgumentParserWithEnvironment, self).add_argument(*args, **kwargs)
        required = kw.get('required', None)
        env_var = kw.get('env_var', None)
        default = kw.get('default', None)
        original_default = default
        if env_var:
            if 'help' in kw and kw['help']:
                # Add the environment variable information to the help text
                # of the argument section
                if action.help:
                    action.help += '\n\n  -- [env {0}]'.format(kw['env_var'])
                else:
                    action.help = '\n\n  -- [env {0}]'.format(kw['env_var'])
                if isinstance(action, argparse._StoreTrueAction):
                    # the default was set to None above, but
                    # in the case of action='store_true' the default is False.
                    default = False
                    original_default = False
                elif isinstance(action, argparse._StoreFalseAction):
                    default = True
                    original_default = True

            # Add the envar to the dictionary, so that it can be printed in the
            # epilog in the "Environemnt Variables" section
            added_EnvText = ''
            if isinstance(action, (argparse._StoreTrueAction, argparse._StoreFalseAction)):
                added_EnvText = "  -- valid values are {0}".format(
                    self.__class__.FALSE_VALUES + self.__class__.TRUE_VALUES)
            self.environment_vars[env_var] = "{0}{1}".format(
                '' if 'help' not in kw else kw['help'],
                added_EnvText)

            def printEnvError(env_var, value):
                printWarn("Environment variable {0} has invalid value: '{1}'."
                          .format(env_var, value),
                          'Valid choices are as follows:\n  {0}.'
                          .format(action.choices) if action.choices else '')

            if env_var in os.environ:
                # use the value of the environment variable as the
                # default, if it's available.
                environ_value = os.environ[env_var]
                if 'type' in kw:
                    # blindly cast the result back into the expected type
                    # specified in the add_argument call.
                    try:
                        default = kw['type'](environ_value)
                    except ValueError:
                        printEnvError(env_var, environ_value)
                        # Set the default and see if the base class
                        # can handle it during the parse phase.
                        default = environ_value
                elif isinstance(action, (argparse._StoreTrueAction, argparse._StoreFalseAction)):
                    if environ_value.title() in self.__class__.TRUE_VALUES:
                        default = True
                    elif environ_value.title() in self.__class__.FALSE_VALUES:
                        default = False
                    else:
                        # The environment variable is neither in the
                        # FALSE set nor in the TRUE set.
                        # We could throw, but that will not allow the passed
                        #  command line argument to override.
                        # Instead, we pretend that the user didn't supply
                        #  anything and just keep the original default, and
                        #  log an error. Maybe, the parse phase can
                        #  figure it out.
                        printEnvError(env_var, environ_value)
                else:
                    default = os.environ[env_var]

                try:
                    # Check and print error if the value set as the default
                    # from the environment variable is invalid.
                    # This check really needs to happen during the parse
                    # phase of argparse, but take_action() is a nested
                    # function inside _parse_known_args().
                    self._check_value(action, default)
                except argparse.ArgumentError:
                    printEnvError(env_var, os.environ[env_var])

            if default and required:
                # The action cannot be required and have a default
                # at the same time.
                # If the original_default is None but the "environment default"
                # is not None then 'required' needs to be set to False.
                required = False
            action.default = default
            action.required = required
            action.original_default = original_default
        return action

    def format_help(self):
        if self.environment_vars:
            environ_usage = 'environment variable defaults:\n{0}'.format(
                '\n'.join("{0}  -  {1}".format(k, v)
                          for (k, v) in self.environment_vars.items()))
            self.epilog = "{0}{1}".format(
                environ_usage,
                '\n\n{0}'.format(self.epilog) if self.epilog else '')
        return super(ArgumentParserWithEnvironment, self).format_help()


def main(argv=None):
    if argv is None:
        argv = sys.argv

    global _PGM
    _PGM = "%s:" % os.path.basename(argv[0])  # colon for easy use by print

    scidbEnv = SciDBEnv(argv)

    # Please forgive the last PYTHONPATH addition.
    # We can't reliable import until the bin directory is available during install.
    sys.path.append(os.path.join(scidbEnv.install_path, "bin"))

    pluginBuildPathStr = "$SCIDB_BUILD_PATH/external_plugins/<name>"

    build_type_argument = ArgumentParserWithEnvironment(add_help=False)
    build_type_argument.add_argument('--build-type',
                                     choices=_BUILD_TYPES,
                                     default='Assert',
                                     env_var='SCIDB_BUILD_TYPE',
                                     help='value to pass as CMAKE_BUILD_TYPE')
    scidb_name_argument = ArgumentParserWithEnvironment(add_help=False)
    scidb_name_argument.add_argument('--scidb-name',
                                     default='mydb',
                                     env_var='SCIDB_NAME',
                                     help='SciDB Cluster Name')

    plugin_arguments = ArgumentParserWithEnvironment(add_help=False)
    plugin_arguments.add_argument('-n', '--name',
                                  required=True,
                                  help="plugin name")

    make_arguments = ArgumentParserWithEnvironment(add_help=False)
    make_arguments.add_argument(
        '-j', '--jobs',
        type=int,
        default=multiprocessing.cpu_count(),
        env_var='SCIDB_MAKE_JOBS',
        help="number of make jobs to spawn (the -j parameter of make)")
    make_target_arguments = ArgumentParserWithEnvironment(add_help=False)
    make_target_arguments.add_argument(
        'target',
        nargs='?',
        default=None,
        help="make target, default is no target")

    force_argument = ArgumentParserWithEnvironment(add_help=False)
    force_argument.add_argument('-f', '--force',
                                action='store_true',
                                help='automatically confirm any old state/directory cleanup')

    test_arguments = ArgumentParserWithEnvironment(add_help=False)
    test_arguments.add_argument(
        '--record',
        action='store_true',
        help="record the expected output")
    test_arguments.add_argument(
        '--add-disabled',
        help="path to the file containing test names to add to the end of disable.tests.")
    test_arguments.add_argument(
        '--disabled-tests',
        help="path to the custom disable.tests file (will be used in place of the default one).")
    test_group = test_arguments.add_mutually_exclusive_group()
    test_group.add_argument(
        '--all',
        action='store_true',
        help="run all scidbtestharness tests")
    test_group.add_argument(
        '--test-id',
        help="run a specific scidbtestharness test")
    test_group.add_argument(
        '--suite-id',
        default='checkin',
        help="run a specific scidbtestharness test suite, default is \'checkin\'")

    auth_file_argument = ArgumentParserWithEnvironment(add_help=False)
    auth_file_argument.add_argument(
        '--auth-file',
        env_var='SCIDB_CONFIG_USER',
        help="specify the authentication file which contains the user name and password")

    verbose_argument = ArgumentParserWithEnvironment(add_help=False)
    verbose_argument.add_argument('-v', '--verbose',
                                  action='store_true',
                                  default=False,
                                  env_var='SCIDB_DBG',
                                  # # TODO: Someday, it would be nice to
                                  # # allow for -v -v -v and use the python logging module
                                  # action='count',
                                  # default=0,
                                  # help='Increase debug logging. 1=info, 2=debug, 3=debug+')
                                  help="display verbose output")

    parser = ArgumentParserWithEnvironment(parents=[verbose_argument],
                                           epilog="""
Environment variables affecting all subcommands:\n
    SCIDB_BUILD_PATH - \n
    SCIDB_INSTALL_PATH - """)

    subparsers = parser.add_subparsers(dest='subparser_name',
                                       metavar='COMMAND',
                                       parser_class=ArgumentParserWithEnvironment,
                                       description="""Use -h/--help with a particular subcommand
                                       from the list below to learn its usage""")

    setup_parser = subparsers.add_parser(
        'setup',
        parents=[verbose_argument, force_argument, build_type_argument],
        help="Setup for out-of-source-tree build",
        description="""Create a new build directory for an out-of-tree build and runs cmake there.""")
    setup_parser.set_defaults(func=setup)

    make_parser = subparsers.add_parser(
        'make',
        parents=[verbose_argument, make_arguments, make_target_arguments],
        help="Builds the sources",
        description="""Builds the sources""",)
    make_parser.set_defaults(func=make)

    install_parser = subparsers.add_parser(
        'install',
        parents=[verbose_argument,
                 make_arguments,
                 scidb_name_argument],
        help="Install and initialize SciDB",
        description="""Re-create SciDB Postgres user. Install and initialize SciDB.""",
        epilog="""Additional environment variables:\n
        SCIDB_DATA_PATH - the common directory path prefix used to create SciDB
instance directories (aka base-path). Default is '$SCIDB_BUILD_PATH/DB-$SCIDB_NAME'\n
        """)
    install_parser.add_argument(
        'config',
        default=None,
        nargs='?',
        help="config.ini file to use with scidbctl.py, default is generated")
    install_parser.add_argument(
        '--pg-user',
        env_var='SCIDB_PG_USER',
        default='postgres',
        help='OS user under which the Postgres DB is running')
    install_parser.add_argument(
        '--host',
        env_var='SCIDB_HOST',
        default='localhost',
        help='Coordinator host DNS name')
    install_parser.add_argument(
        '--port',
        type=int,
        env_var='SCIDB_PORT',
        default=1239,
        help='Coordinator TCP port')
    install_parser.add_argument(
        '--nowatchdog',
        action='store_true',
        env_var='SCIDB_NO_WATCHDOG',
        help='Do not start a watch-dog process')
    install_parser.add_argument(
        '--instance-count',
        type=int,
        env_var='SCIDB_INSTANCE_NUM',
        default=4,
        help='Number of SciDB instances to initialize.')
    install_group = install_parser.add_mutually_exclusive_group()
    # This '--force' is NOT like the others, it is mutually exclusive of --light
    # TODO: In other cases --force is the equivalent to 'answer yes to questions'
    # In this case, if you use --light, you may not use --force. Ideally --force
    # should be changed to something like:
    #    -y  Assume Yes to all queries and do not prompt
    install_group.add_argument('-f', '--force',
                               action='store_true',
                               help="automatically confirm any old state/directory cleanup")
    install_group.add_argument('-l', '--light',
                               action='store_true',
                               help="just install new binaries, no changes to configuration are made")
    install_parser.set_defaults(func=install)

    plugin_setup_parser = subparsers.add_parser(
        'plugin_setup',
        parents=[verbose_argument, build_type_argument, force_argument, plugin_arguments],
        help="Create build directory and run cmake for out-of-tree plugin build",
        description="""
Creates the {0} directory for an out-of-tree plugin build and runs cmake there.

The plugin in the directory specified by --path must conform to the following rules:

1. It is based on cmake.
2. The plugin build directory {1} must be configurable by
    \'cmake -DCMAKE_INSTALL_PREFIX=<scidb_installation_dir>
      -DSCIDB_SOURCE_DIR=<scidb_source_dir> ...\'
3. Running \'make\' in that directory must build all the deliverables.
4. Running \'make install\' in that directory must install all the
   deliverables into the scidb installation.
5. Running \'./deployment/deploy.sh build $SCIDB_BUILD_TYPE <package_path>
    <scidb_bin_path> {2}\' in the directory specified by --path must
    generate installable plugin packages. See \'plugin_make_packages --help\'
6. \'scidbtestharness --rootdir=PATH/test/tescases
   --scratchDir=$SCIDB_BUILD_DIR/tests/harness/testcases ...\' must be runnable in {3}."""
        .format(pluginBuildPathStr,
                pluginBuildPathStr,
                scidbEnv.bin_path,
                pluginBuildPathStr + "test"))
    plugin_setup_parser.add_argument(
        '-p', '--path',
        required=True,
        help="directory path for plugin src")
    plugin_setup_parser.set_defaults(func=pluginSetup)

    plugin_make_parser = subparsers.add_parser(
        'plugin_make',
        parents=[verbose_argument, plugin_arguments, make_arguments, make_target_arguments],
        help="Builds the plugin sources",
        description="""Builds the plugin sources""")
    plugin_make_parser.set_defaults(func=pluginMake)

    plugin_install_parser = subparsers.add_parser(
        'plugin_install',
        parents=[verbose_argument, plugin_arguments, make_arguments],
        help="Install a plugin",
        description="""Installs the plugin by depositing the contents of {0} into {1}""".format(
            pluginBuildPathStr + "<name>/plugins",
            "$SCIDB_INSTALL_PATH/lib/scidbplugins"))
    plugin_install_parser.set_defaults(func=pluginInstall)

    start_parser = subparsers.add_parser(
        'start',
        parents=[verbose_argument, scidb_name_argument, auth_file_argument],
        help="Start SciDB",
        description="""Start SciDB (previously installed by \'install\')""")
    start_parser.set_defaults(func=start)

    stop_parser = subparsers.add_parser(
        'stop',
        parents=[verbose_argument, scidb_name_argument],
        help="Stop SciDB",
        description="""Stop SciDB (previously installed by \'install\')""")
    stop_parser.set_defaults(func=stop)

    initall_parser = subparsers.add_parser(
        'initall',
        parents=[verbose_argument, scidb_name_argument],
        help="Initialize SciDB",
        description="""Initialize SciDB (previously installed by \'install\')""")
    initall_parser.set_defaults(func=initall)

    forceStop_parser = subparsers.add_parser(
        'forceStop',
        parents=[verbose_argument, scidb_name_argument],
        help="kill SciDB instances",
        description="""Stop SciDB instances with \'kill -9\'""")
    forceStop_parser.set_defaults(func=forceStop)

    tests_parser = subparsers.add_parser(
        'tests',
        parents=[verbose_argument,
                 auth_file_argument,
                 test_arguments,
                 scidb_name_argument],
        help="Run scidbtestharness",
        description="""Run scidbtestharness for a given set of tests\n
        The results are stored in $SCIDB_BUILD_PATH/tests/harness/run.tests.log\n
        {0}/tests/harness/scidbtestharness_env.sh is source'd to create the
        environment for scidbtestharness""".format(scidbEnv.build_path))
    tests_parser.set_defaults(func=tests)

    plugin_tests_parser = subparsers.add_parser(
        'plugin_tests',
        parents=[verbose_argument,
                 plugin_arguments,
                 auth_file_argument,
                 test_arguments,
                 scidb_name_argument],
        help="Run scidbtestharness tests for a plugin",
        description="""Run scidbtestharness for a given set of tests
        of a given plugin.\n
        The results are stored in {0}/test/run.tests.log""".format(pluginBuildPathStr),
        epilog="""NOTE:\n
        SCIDB_BUILD_PATH and SCIDB_DATA_PATH are exported into the environment
        when calling the test harness\n\n
        $SCIDB_BUILD_PATH/tests/harness/scidbtestharness_env.sh is source'd to create the
           environment for scidbtestharness.""")
    plugin_tests_parser.set_defaults(func=pluginTests)

    cleanup_parser = subparsers.add_parser(
        'cleanup',
        parents=[verbose_argument, scidb_name_argument, force_argument],
        help="Remove SciDB build artifact directory trees",
        description="""Remove build, install, SciDB data directory trees.
        It will execute stop() if config.ini is present in the install directory.""")
    cleanup_parser.set_defaults(func=cleanup)

    package_parser = subparsers.add_parser(
        'make_packages',
        parents=[verbose_argument, force_argument],
        help="Build deployable SciDB packages",
        description="""Builds deployable SciDB packages""")
    package_parser.add_argument(
        'package_path',
        default=os.path.join(
            scidbEnv.build_path,
            "packages"),
        nargs='?',
        help="full directory path for newly generated_packages, default is $SCIDB_BUILD_PATH/packages")
    package_parser.set_defaults(func=make_packages)

    plugin_package_parser = subparsers.add_parser(
        'plugin_make_packages',
        parents=[verbose_argument, plugin_arguments, force_argument],
        help="Build deployable plugin packages",
        description="""Builds deployable plugin packages by invoking
 \'deploy.sh build $SCIDB_BUILD_TYPE ...\' in the plugin source directory.
 See \'plugin_setup --help\'.""",
        epilog="""WARNING:\n
        Currently, the plugin packages are allowed to be generated from scratch
(not from the results of \'plugin_make\') on every invocation.""")
    plugin_package_parser.add_argument(
        'package_path',
        nargs='?',
        help="""full directory path for newly generated_packages,
 default is $SCIDB_BUILD_PATH/external_plugins/<name>/packages""")
    plugin_package_parser.add_argument(
        'scidb_bin_path',
        default=os.path.join(scidbEnv.build_path,
                             "bin"),
        nargs='?',
        help="full path for scidb bin directory, default is $SCIDB_BUILD_PATH/bin")
    plugin_package_parser.set_defaults(func=pluginMakePackages)

    make_src_parser = subparsers.add_parser(
        'make_src_package',
        parents=[verbose_argument, make_arguments],
        help="Builds SciDB source tar file",
        description="""Builds SciDB source tar file""")
    make_src_parser.add_argument(
        'package_path',
        default=os.path.join(
            scidbEnv.build_path,
            "packages"),
        nargs='?',
        help="""directory path for newly generated tar file,
 default is $SCIDB_BUILD_PATH/packages""")
    make_src_parser.set_defaults(func=make_src_package)

    version_parser = subparsers.add_parser(
        'version',
        help="Show SciDB version.",
        description="""Print SciDB version (in short form)""")
    version_parser.set_defaults(func=version)

    # setup all combinations of ways to ask for basic help....
    # ./run.py [--help|h] [help] [--help|h]
    # This allows for a user to request help for a sub command using
    # the following:
    # ./run.py help <subcommand>
    help_parser = subparsers.add_parser('help',
                                        parents=[verbose_argument],
                                        add_help=False)
    help_parser.add_argument('-h', '--help', action='store_true')
    help_parser.add_argument('command', nargs='?', default=None)

    if not argv[1:]:
        parser.print_help()
        return 2
    args = parser.parse_args(argv[1:])
    if args.subparser_name == "help":
        if not args.command:
            parser.parse_args(['--help'])
        else:
            parser.parse_args([args.command, '--help'])
    scidbEnv.args = args
    global _DBG
    _DBG = args.verbose
    printDebug("cmd=" + args.subparser_name)
    printDebug("passed arguments: {0}".format(args))

    try:
        # NOTE: The default for 'build_type' may have been set to invalid value
        #       by a setting of an  environment variable.
        #
        # TODO: update the ArgumentParserWithEnvironment to handle this
        #       case with "choices" after the default is reset.
        #       One problem about checking when setting the default can
        #       occur with a call like:
        #           env SCIDB_BUILD_PATH=INVALID ./run.py --build_type Assert
        #       The environment given is "invalid" but the Command line
        #       is valid. Checking the passed environment variable will throw
        #       an exception at argument setup point not at parse time.
        build_type = None if 'build_type' not in args else args.build_type
        if build_type and build_type not in _BUILD_TYPES:
                printError("Unrecognized build type '{0}', must be one of: {1}\n\n"
                           .format(build_type, ", ".join(_BUILD_TYPES)))
                parser.parse_args([args.subparser_name, '--help'])
                raise AppError()
        if args.subparser_name not in ("setup", "cleanup", "forceStop"):
            confirmRecordedInstallPath(scidbEnv)
        args.func(scidbEnv)
    except AppError as e:
        printError(e)
        return 1
    except Exception as e:
        printError("Command", args.subparser_name, "failed:", e)
        printError(' '.join(("Make sure commands setup,make,install,start are",
                             "performed (in that order) before stop,stopForce,tests")))
        if _DBG:
            traceback.print_exc()
        return 1
    else:
        return 0


if __name__ == "__main__":
    sys.exit(main())
