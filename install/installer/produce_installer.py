#!/usr/bin/python
#
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
#
"""
This script produces a package, either debian or rpm,
that will install SciDB Community Edition on a single node.
"""
import argparse
import atexit
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile

# GLOBALS
_args = None    # Parsed arguments.
_MYDIR = None   # Directory this script is in.
_OS = None      # The OS we are running on.
_PGM = None     # This script's name (basename).
_TMPDIR = None  # Temporary directory used as packaging directory.

# CONSTANTS
_OS_CENTOS = "CentOS"
_OS_UBUNTU = "Ubuntu"

# UTILITY ROUTINES
def create_tempdir():
    global _TMPDIR

    _TMPDIR = tempfile.mkdtemp()
    atexit.register(remove_tempdir)


def remove_tempdir():
    if not _args.verbose:
        shutil.rmtree(_TMPDIR)


class AppError(Exception):
    pass


def printDebug(string, force=False):
    if _args.verbose or force:
        print >> sys.stderr, "DEBUG(%s): %s" % (_PGM, string)

def replaceVERSION(inFile, outFile):
    # Rewrite the inFile file to the outFile file substituting the scidb version for the string VERSION
    with open(inFile, "r") as source:
        contents = source.read()
    with open(outFile, "w") as output:
        output.write(contents.replace('VERSION',_args.version))
    printDebug("Rewrote %s" % outFile)


def prepare_package(OS, OSversion):
    """Prepare the packaging directory."""
    if OS == _OS_CENTOS:
        source_dir=os.path.join(_MYDIR, "rpm_packaging")
        common_dir=os.path.join(_MYDIR, "common")
        SOURCES_dir=os.path.join(_TMPDIR, "SOURCES")
        SPECS_dir=os.path.join(_TMPDIR, "SPECS")
        # Create SOURCES directory
        if not os.path.exists(SOURCES_dir):
            try:
                os.makedirs(SOURCES_dir)
            except:
                raise AppError("Unable to create SOURCES directory '%s'." % SOURCES_dir)
        printDebug("Created %s" % SOURCES_dir)
        # Create SPECS directory
        if not os.path.exists(SPECS_dir):
            try:
                os.makedirs(SPECS_dir)
            except:
                raise AppError("Unable to create SPECS directory '%s'." % SPECS_dir)
        printDebug("Created %s" % SPECS_dir)
        # Rewrite specs file, substituting scidb version for string VERSION
        replaceVERSION(os.path.join(source_dir, "scidb-installer.spec"), os.path.join(SPECS_dir, "scidb-installer.spec"))
        # Copy into SOURCES directory all scripts specified in the spec file.
        # These are listed as SourceX:
        with open(os.path.join(source_dir, "scidb-installer.spec"), "r") as source:
            contents = source.read()
        L=re.findall('(^Source[0-9]+:\s*)([^\n]+)', contents, re.M|re.I)
        for source_file in [x[1].strip() for x in L]:
            replaceVERSION(os.path.join(common_dir, source_file), os.path.join(SOURCES_dir, source_file))
    elif OS == _OS_UBUNTU:
        source_dir=os.path.join(_MYDIR, "debian_packaging")
        common_dir=os.path.join(_MYDIR, "common")
        package_dir=os.path.join(_TMPDIR,  "scidb-" + _args.version + "-installer")
        DEBIAN_dir=os.path.join(package_dir, "DEBIAN")
        OPT_SCRIPT_dir=os.path.join(package_dir, "opt", "scidb", _args.version, "script")
        # Create DEBIAN directory
        if not os.path.exists(DEBIAN_dir):
            try:
                os.makedirs(DEBIAN_dir)
            except:
                raise AppError("Unable to create DEBIAN directory '%s'." % DEBIAN_dir)
        printDebug("Created %s" % DEBIAN_dir)
        # Create OPT SCRIPT directory
        if not os.path.exists(OPT_SCRIPT_dir):
            try:
                os.makedirs(OPT_SCRIPT_dir)
            except:
                raise AppError("Unable to create OPT_SCRIPT directory '%s'." % OPT_SCRIPT_dir)
        printDebug("Created %s" % OPT_SCRIPT_dir)
        # Rewrite control file, substituting scidb _args.version
        replaceVERSION(os.path.join(source_dir, "control"), os.path.join(DEBIAN_dir, "control"))
        # Rewrite postinst file, substituting scidb _args.version
        replaceVERSION(os.path.join(source_dir, "postinst"), os.path.join(DEBIAN_dir, "postinst"))
        os.chmod(os.path.join(DEBIAN_dir, "postinst"), 0555)
        # Copy into /opt/scidb/<version>/scripts directory all scripts specified in the control file.
        # These are listed as 'Source: <filename>'
        with open(os.path.join(source_dir, "postinst"), "r") as source:
            contents = source.read()
        L=re.findall('(^Source[0-9]+:\s*)([^\n]+)', contents, re.M|re.I)
        for source_file in [x[1].strip() for x in L]:
            replaceVERSION(os.path.join(common_dir, source_file), os.path.join(OPT_SCRIPT_dir, source_file))
            os.chmod(os.path.join(OPT_SCRIPT_dir, source_file), 0555)


def create_package(OS):
    """Call the package creation method particular to the OS"""
    if OS == _OS_CENTOS:
        cmdline = ["rpmbuild",
                   "--quiet",
                   "--define", "_topdir " + _TMPDIR,
                   "-bb", os.path.join(_TMPDIR, "SPECS", "scidb-installer.spec")]
        if _args.verbose:
            cmdline.remove("--quiet")
        printDebug(cmdline)
        subprocess.call(cmdline)
    elif OS == _OS_UBUNTU:
        cmdline = ["dpkg-deb",
                   "--build",
                   os.path.join(_TMPDIR, "scidb-" + _args.version + "-installer")]
        printDebug(cmdline)
        with open("/dev/null", "w") as null:
            subprocess.call(cmdline,stdout=null,stderr=null)


def copyout_results(OS):
    """Copy out the resulting package to the results directory."""
    if OS == _OS_CENTOS:
        shutil.copy(
            os.path.join(_TMPDIR, "RPMS", "noarch", "scidb-" + _args.version + "-installer-0-1.noarch.rpm"),
            os.path.join(_args.results_dir, "scidb-" + _args.version + "-installer.rpm"))
    elif OS == _OS_UBUNTU:
        shutil.copy(
            os.path.join(_TMPDIR, "scidb-" + _args.version + "-installer.deb"),
            os.path.join(_args.results_dir, "scidb-" + _args.version + "-installer.deb"))


def main(argv=None):
    global _args
    global _MYDIR
    global _OS
    global _PGM

    if argv is None:
        argv = sys.argv

    _PGM = os.path.basename(argv[0])
    _MYDIR = os.path.dirname(os.path.realpath(argv[0]))

    # Where are we (what OS).
    _OS, OSversion, OScodename = platform.linux_distribution()

    # Argument processing
    if _OS == _OS_CENTOS:
        parser = argparse.ArgumentParser(
            description='Produce a yum installer for SciDB',
            epilog='Type "pydoc %s" for more information.' % _PGM)
    elif _OS == _OS_UBUNTU:
        parser = argparse.ArgumentParser(
            description='Produce a apt installer for SciDB',
            epilog='Type "pydoc %s" for more information.' % _PGM)
    else:
        raise AppError("Unsupported Platform.")
    parser.add_argument('version', help='The SciDB Version of the installer.')
    parser.add_argument('results_dir',  help='Directory to put the installer file in.')
    parser.add_argument('-v','--verbose', action='store_true', help="display verbose output")
    _args = parser.parse_args(argv[1:])

    printDebug("OS=%s, OSversion=%s, OScodename=%s" % (_OS,OSversion,OScodename))
    printDebug("version=%s, results_dir=%s" % (_args.version,_args.results_dir))

    # Argument Checks
    if re.match("^\d{2}?\.\d{1,2}?$", _args.version) is None:
        raise AppError("Version '%s' is not valid." % _args.version)
    if not os.path.exists(_args.results_dir):
        try:
            os.makedirs(_args.results_dir)
        except:
            raise AppError("Unable to create results directory '%s'." % _args.results_dir)
    if not os.access(_args.results_dir, os.W_OK):
        raise AppError("Unable to write to results directory '%s'." % _args.results_dir)
    # User ID Check
    if os.geteuid() != 0:
        raise AppError("Script must be run with root privileges.")

    create_tempdir()
    printDebug("TMPDIR=%s" % _TMPDIR)

    prepare_package(_OS, OSversion)

    create_package(_OS)

    copyout_results(_OS)

    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except AppError as e:
        print >>sys.stderr, "ERROR(%s): %s" % (_PGM, e)
        sys.exit(2)
