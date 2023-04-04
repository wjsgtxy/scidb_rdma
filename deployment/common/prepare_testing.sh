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
set -u

#
# Note that rpm --import of keys are or'd with true
#   rpm --import https://downloads.paradigm4.com/key || true
# because if the key import fails, yum install will ask the user
# to validate the key when downloading from that repo.
#
# So key import is a recoverable error.
#
source $(dirname $(readlink -f $0))/deploy_common.sh

SCIDB_VER="${1}"
OS=$(./os_detect.sh)

function die()
{
    echo 1>&2 "$@"
    exit 1
}

echo "Preparing server for testing SciDB..."

case ${OS} in
    "CentOS 7"|"RedHat 7")
        #
        # This assumes register_repositories has been run
        #
	yum clean all
	yum -y makecache fast

	INSTALL="yum install --enablerepo=scidb3rdparty -y -q"

        # Get yum-config-manager
        ${INSTALL} yum-utils

	### Compiler's debug
	# gcc/g++/gfort version 4.9
	${INSTALL} devtoolset-3-gdb

	# Get pip
	pipSetup
	# Get pip-packages.txt
	pipInstallPackages

	# extras for testing
	${INSTALL} bc nss-pam-ldapd python-ldap net-tools python-ply
	;;
    "CentOS 6"|"RedHat 6")
        #
        # This assumes register_repositories has been run
        #
	yum clean all
	yum -y makecache fast

	INSTALL="yum install --enablerepo=scidb3rdparty -y -q"

        # Get yum-config-manager
        ${INSTALL} yum-utils

	### Compiler's debug
	# gcc/g++/gfort version 4.9
	${INSTALL} devtoolset-3-gdb

	# Get pip
	pipSetup
	# Get pip-packages.txt
	pipInstallPackages

	# extras for testing
	${INSTALL} bc pam_ldap python-ldap python-ply
	;;
    "Ubuntu 14.04"|"Ubuntu 16.04")
        #
        # This assumes register_repositories has been run
        #
	echo "Updating apt repositories..."
	apt-get update > /dev/null

	export DEBIAN_FRONTEND=noninteractive
	INSTALL="apt-get install -y -q"

	# Get pip
	pipSetup
	# Get pip-packages.txt
	pipInstallPackages

	# extras for testing
	${INSTALL} bc libnss-ldapd libpam-ldapd nslcd python-ldap python-ply
	;;
    *)  die "Not a supported OS: $OS"
	;;
esac

echo "...prepared server for SciDB testing"
