#!/bin/bash
#
# BEGIN_COPYRIGHT
#
# Copyright (C) 2016-2019 SciDB, Inc.
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
#
dc_deploy_err=$(mktemp /var/tmp/deploy_common.err.XXXXXX)
trap "sudo rm -rf $dc_deploy_err" 0 15 3 2 1

DC_INSTALL=""
DC_CHECK=""

DC_DIR=$(dirname $(readlink -f $0))
DC_OS=$(${DC_DIR}/os_detect.sh)
DC_PIP_PACKAGES=${DC_DIR}/pip-packages.txt

function dc_die()
{
    echo 1>&2 "$@"
    exit 1
}
#
# Installs a package
#   Tests if package is already installed
#   First argument is the package name
#   Optional second argument is the package url
#
function installPkg()
{
    [ "$DC_INSTALL" != "" ] || dc_die DC_INSTALL not defined
    [ "$DC_CHECK" != "" ] || dc_die DC_CHECK not defined
    local pkg_name="${1}"
    local pkg_location=""

    if [ $# -eq 1 ]; then
	pkg_location="$pkg_name"
    else
	pkg_location="${2}"
    fi
    echo "    Checking for $pkg_name..."
    ${DC_CHECK} "$pkg_name" 2>"${dc_deploy_err}" 1> /dev/null
    if [ $? -eq 0 ]; then
	echo "    $pkg_name currently installed."
    else
	echo "    Installing $pkg_name..."
	${DC_INSTALL} "$pkg_location" 2>"${dc_deploy_err}" 1> /dev/null
	if [ $? -eq 0 ]; then
	    echo "    $pkg_name successfully installed."
	else
	    echo -e "    \033[1m\E[31;40mWARNING:\033[0m ${DC_INSTALL} ${pkg_location} failed:"
	    cat "${dc_deploy_err}"
	fi
    fi
}
#
# Setup to use latest pip
#
function pipSetup()
{
    case ${DC_OS} in
	"CentOS 6"|"RedHat 6")
	    DC_CHECK="yum list installed"
	    DC_INSTALL="yum install -y -q"
	    #
	    # For CentOS6 and RedHat6
	    #   the system python is 2.6
	    #   to pip install the latest Cmake (3.13)
	    #   we need to use python2.7
	    #
	    # Get python2.7 from saltstack
	    echo "installing saltstack repo in order to install python27"
	    rpm -i https://repo.saltstack.com/yum/redhat/salt-repo-latest-2.el6.noarch.rpm
	    # Install python 2.7
	    installPkg python27
	    # Install pip for python 2.7
	    installPkg python27-pip
	    #
	    # Upgrade pip for python 2.7
	    #   if package was there but an update is available
	    #
	    /usr/bin/python2.7 -m pip install --upgrade pip
	    #
	    # Install pip for python 2.6
	    installPkg python-pip
	    ;;
	"CentOS 7"|"RedHat 7")
	    DC_CHECK="yum list installed"
	    DC_INSTALL="yum install -y -q"
	    # install pip
	    installPkg python-pip
	    #
	    # Upgrade pip
	    #   if package was there but an update is available
	    #
	    python -m pip install --upgrade pip
	    ;;
	"Ubuntu 14.04"|"Ubuntu 16.04")
	    DC_CHECK="dpkg -s"
	    DC_INSTALL="apt-get install -y -q"
	    # install pip
	    installPkg python-pip
	    #
	    # Upgrade pip
	    #   if package was there but an update is available
	    #
	    python -m pip install --upgrade pip
	    ;;
	*)
	    dc_die Not a supported OS
	    ;;
    esac
}

function pipInstallCmake()
{
    case ${DC_OS} in
	"CentOS 6"|"RedHat 6")
	    DC_PIP_INSTALL="/usr/bin/python2.7 -m pip install --upgrade"
	    ;;
	*)
	    DC_PIP_INSTALL="python -m pip install --upgrade"
	    ;;
    esac
    ${DC_PIP_INSTALL} cmake
}

function pipInstallPackages()
{
    DC_PIP_INSTALL="pip install --upgrade --ignore-installed"
    if [ -f ${DC_PIP_PACKAGES} ]
    then
	grep -v -E '^#' ${DC_PIP_PACKAGES} | while read LINE
	do
            ${DC_PIP_INSTALL} $LINE || true
	done
    fi
    # On c6/r6 the native python is 2.6 but for protobufs need python 2.7
    # Do pip install twice on c6/r6 once for 2.6 and once for 2.7
    case ${DC_OS} in
	"CentOS 6"|"RedHat 6")
	    DC_PIP_INSTALL="/usr/bin/python2.7 -m pip install --upgrade --ignore-installed"
	    grep -v -E '^#' ${DC_PIP_PACKAGES} | while read LINE
	    do
                ${DC_PIP_INSTALL} $LINE || true
	    done
	    ;;
    esac
}
