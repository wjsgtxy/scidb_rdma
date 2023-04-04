#!/bin/bash
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

set -eu

username=${1}

function die()
{
    echo 1>&2 "$@"
    exit 1
}

function chroot_sudoers_mock ()
{
    CHROOT_SUDOERS=/etc/sudoers.d/chroot_builder
    echo "Defaults:${username} !requiretty" > ${CHROOT_SUDOERS}
    echo "Cmnd_Alias RMSCIDB_PACKAGING = /bin/rm -rf /tmp/scidb_packaging.*" >> ${CHROOT_SUDOERS}
    echo "${username} ALL = NOPASSWD:/usr/sbin/mock, NOPASSWD:/bin/which, NOPASSWD:RMSCIDB_PACKAGING" >> ${CHROOT_SUDOERS}
    chmod a-wx,o-r,ug+r ${CHROOT_SUDOERS}
}

function centos6 ()
{
    yum -y makecache fast
    yum install --enablerepo=scidb3rdparty -y gcc make rpm-build mock python-argparse
    chroot_sudoers_mock
}

function centos7 ()
{
    yum -y makecache fast
    yum install --enablerepo=scidb3rdparty -y gcc make rpm-build mock
    chroot_sudoers_mock
}

function chroot_sudoers_pbuilder ()
{
    CHROOT_SUDOERS=/etc/sudoers.d/chroot_builder
    echo "Defaults:${username} !requiretty" > ${CHROOT_SUDOERS}
    echo "Cmnd_Alias RMSCIDB_PACKAGING = /bin/rm -rf /tmp/scidb_packaging.*" >> ${CHROOT_SUDOERS}
    echo "${username} ALL = NOPASSWD:/usr/sbin/pbuilder, NOPASSWD:/bin/which, NOPASSWD:RMSCIDB_PACKAGING" >> ${CHROOT_SUDOERS}
    chmod a-wx,o-r,ug+r ${CHROOT_SUDOERS}
}
#
# Need to add apt-get install apt-transport-https
# before the othermirror (https://downloads.paradigm4.com)
# is loaded.
#
# This creates a pbuilder --create hook that loads the https transport into apt-get.
# It will be found by pbuilder because the flag "--hookdir /var/cache/pbuilder/hook.d"
# has been added to the class UbuntuChroot(): init function in utils/chroot_build.py
#
# From the man page:
#   G<digit><digit><whatever-else-you-want> is executed just after
#   debootstrap  finishes, and configuration is loaded, and pbuilder
#   starts mounting /proc and invoking apt-get install in --create target.
#
function pbuilder_apt-transport-https ()
{
    mkdir -p /var/cache/pbuilder/hook.d/
    echo "#!/bin/sh" > /var/cache/pbuilder/hook.d/G01https
    echo "apt-get install -y apt-transport-https" >> /var/cache/pbuilder/hook.d/G01https
    echo "apt-get install -y ca-certificates" >> /var/cache/pbuilder/hook.d/G01https
    chmod 555 /var/cache/pbuilder/hook.d/G01https
}
#
# Need to run apt-get update before trying to satisfy build-dependency
#
# This creates a pbuilder --build hook that runs "apt-get update"
# It will be found by pbuilder because the flag "--hookdir /var/cache/pbuilder/hook.d"
# has been added to the class UbuntuChroot(): init function in utils/chroot_build.py
#
# From the man page:
#   D<digit><digit><whatever-else-you-want> is executed before unpacking the source
#   inside  the chroot, after setting up the chroot environment.
#   This is called before build-dependency is satisfied.
#   Also useful for calling apt-get update
#
function pbuilder_apt-get-update ()
{
    mkdir -p /var/cache/pbuilder/hook.d/
    echo "#!/bin/sh" > /var/cache/pbuilder/hook.d/D01apt-get-update
    echo "apt-get update" >> /var/cache/pbuilder/hook.d/D01apt-get-update
    chmod 555 /var/cache/pbuilder/hook.d/D01apt-get-update
}
#
# This will run a %prep script like rpm in order to install cmake with pip
#
# This creates a pbuilder --build hook that runs "apt-get update"
# It will be found by pbuilder because the flag "--hookdir /var/cache/pbuilder/hook.d"
# has been added to the class UbuntuChroot(): init function in utils/chroot_build.py
#
# From the man page:
#   A<digit><digit><whatever-else-you-want> is for --build target.
#   It is executed before build starts; after unpacking the build system,
#    and unpacking the source, and satisfying the build-dependency.
#
#   We are using it here as rpm build does for a pre-build step, %prep.
#
function pbuilder_prep ()
{
    mkdir -p /var/cache/pbuilder/hook.d/
    echo "#!/bin/sh" > /var/cache/pbuilder/hook.d/A01build_prep
    echo "apt-get install python-pip" >> /var/cache/pbuilder/hook.d/A01build_prep
    echo "python -Wignore -m pip install --upgrade pip" >> /var/cache/pbuilder/hook.d/A01build_prep
    echo "python -Wignore -m pip install --upgrade cmake" >> /var/cache/pbuilder/hook.d/A01build_prep
    echo "python -Wignore -m pip install --upgrade --ignore-installed protobuf" >> /var/cache/pbuilder/hook.d/A01build_prep
    chmod 555 /var/cache/pbuilder/hook.d/A01build_prep
}

function u_common ()
{
    echo "Updating apt repositories..."
    apt-get update &> /dev/null
    echo "Installing packages for chroot-ing..."
    apt-get install -y -q build-essential dpkg-dev pbuilder debhelper m4 cdbs quilt apt-transport-https
    chroot_sudoers_pbuilder
    pbuilder_apt-transport-https
    pbuilder_apt-get-update
    pbuilder_prep
}

case $(./os_detect.sh) in
    "CentOS 6")
	centos6
	;;
    "CentOS 7")
	centos7
	;;
    "RedHat 6" | "RedHat 7")
	echo "We do not support building SciDB under RedHat. Please use CentOS instead"
	exit 1
	;;
    "Ubuntu 14.04"|"Ubuntu 16.04")
	u_common
	;;
    *)
	echo "Not a supported OS";
	exit 1
	;;
esac
