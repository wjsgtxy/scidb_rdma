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

function die()
{
    echo 1>&2 "$@"
    exit 1
}

function installThem()
{
    [ "$INSTALL" != "" ] || die INSTALL not defined
    [ "$CHECK" != "" ] || die CHECK not defined
    for pkg_name in "${@}"
    do
	p=`basename $pkg_name | sed 's/-[^-]*-[^-]*\.[^.]*\.[^.]*$//'`
	echo "    Checking for $p..."
	${CHECK} "$p" 2>"${deploy_err}" 1> /dev/null
	if [ $? -eq 0 ]; then
	    echo "    $p currently installed."
	else
	    echo "    Installing $p..."
	    ${INSTALL} "$pkg_name" 2>"${deploy_err}" 1> /dev/null
	    if [ $? -eq 0 ]; then
		echo "    $p successfully installed."
	    else
		echo -e "    \033[1m\E[31;40mWARNING:\033[0m ${INSTALL} $pkg_name failed:"
		cat "${deploy_err}"
	    fi
	fi
    done
}

function installIt()
{
    [ "$INSTALL" != "" ] || die INSTALL not defined
    [ "$CHECK" != "" ] || die CHECK not defined
    p="${1}"
    echo "    Checking for $p..."
    ${CHECK} "$p" 2>"${deploy_err}" 1> /dev/null
    if [ $? -eq 0 ]; then
	echo "    $p currently installed."
    else
	echo "    Installing $p..."
	${INSTALL} "${2}" 2>"${deploy_err}" 1> /dev/null
	if [ $? -eq 0 ]; then
	    echo "    $p successfully installed."
	else
	    echo -e "    \033[1m\E[31;40mWARNING:\033[0m ${INSTALL} ${2} failed:"
	    cat "${deploy_err}"
	fi
    fi
}

function ubuntu1404 ()
{
    #
    # This assumes register_repositories has been run
    #
    echo "Updating apt repositories..."
    apt-get update > /dev/null
    apt-get --assume-yes dist-upgrade > /dev/null

    export DEBIAN_FRONTEND=noninteractive

    # TODO It's highly likely that only one or two of these is really
    #      needed, but it's difficult to diagnose what the appropriate
    #      subset is.
    declare -a psp=('software-properties-common'
                    'python-software-properties'
                    'python3-software-properties'
                   )
    installThem "${psp[@]}"

    echo "Installing Compilers, etc..."
    declare -a gcc=("gcc-4.9" "g++-4.9" "gfortran-4.9" "gfortran")
    installThem "${gcc[@]}"

    # Boostrapping
    echo "Installing the tools to get started..."
    declare -a bstp=("git"
                     "expect"
                     "sudo"
                     "openssh-client"
                     "openssh-server"
                     "unzip")
    installThem "${bstp[@]}"

    # Build dependencies:
    declare -a bdeps=("build-essential"
                      "debhelper"
                      "doxygen"
                      "flex"
                      "bison"
                      "libpq5"
                      "libpqxx-dev"
                      "libbz2-dev"
                      "libz-dev"
                      "libedit-dev"
                      "libcppunit-dev"
                      "libprotobuf-dev"
                      "protobuf-compiler"
                      "python-crypto"
                      "python-paramiko"
                      "python-ply"
                      "libssl-dev"
                      "intel-mkl-2019.1-053"
                      "liblapack-dev"
                      "libpam-dev"
                      "bc"
                      "lsof"
                     )
    installThem "${bdeps[@]}"

    echo "Packages that really are not necessary, and should probably go away"
    declare -a unnecessaries=("ccache")
    installThem "${unnecessaries[@]}"

    echo "Python3"
   # Boost package build requires:
    py[0]="python3"
    installThem "${py[@]}"

    # Scidb 3rd party packages
    echo "Installing tools and libraries built specifically for SciDB..."
    declare -a sdb=("scidb-${SCIDB_VER}-libboost1.54-all-dev"
                    "libcsv-dev"
                    "liblog4cxx10-dev"
                    "librocksdb-dev"
                    "libmpich2scidb-dev"
                    "libscalapack-scidb-mpich2scidb-dev"
                   )
    installThem "${sdb[@]}"

    # Testing:
    echo "Installing Postgres 9.3..."
    declare -a pg=("postgresql-client-9.3"
	           "postgresql-9.3"
		   "postgresql-contrib-9.3")
    installThem "${pg[@]}"

    # ScaLAPACK tests:
    echo "Installing time module..."
    t[0]="time"
    installThem "${t[@]}"

    # Get pip
    pipSetup
    # Get cmake
    pipInstallCmake
    # Get pip-packages.txt
    pipInstallPackages
}

function ubuntu1604 ()
{
    #
    # This assumes register_repositories has been run
    #
    echo "Updating apt repositories..."
    apt-get update > /dev/null
    apt-get --assume-yes dist-upgrade > /dev/null

    export DEBIAN_FRONTEND=noninteractive

    # TODO It's highly likely that only one or two of these is really
    #      needed, but it's difficult to diagnose what the appropriate
    #      subset is.
    declare -a psp=('software-properties-common'
                    'python-software-properties'
                    'python3-software-properties'
                   )
    installThem "${psp[@]}"

    echo "Installing Compilers, etc..."
    declare -a gcc=("gcc" "g++" "gfortran")
    installThem "${gcc[@]}"

    # Boostrapping
    echo "Installing the tools to get started..."
    declare -a bstp=("git"
                     "expect"
                     "sudo"
                     "openssh-client"
                     "openssh-server"
                     "unzip")
    installThem "${bstp[@]}"

    # Build dependencies:
    declare -a bdeps=("build-essential"
                      "debhelper"
                      "doxygen"
                      "libboost1.58-all-dev"
                      "flex"
                      "bison"
                      "libpq5"
                      "libpqxx-dev"
                      "libbz2-dev"
                      "libz-dev"
                      "libedit-dev"
                      "libcsv-dev"
                      "libcppunit-dev"
                      "libprotobuf-dev"
                      "protobuf-compiler"
                      "python-crypto"
                      "python-paramiko"
                      "python-ply"
                      "libssl-dev"
                      "intel-mkl-64bit-2019.1-053"
                      "liblapack-dev"
                      "libopenmpi-dev"
                      "libpam-dev"
                      "bc"
                      "lsof"
                     )
    installThem "${bdeps[@]}"

    echo "Packages that really are not necessary, and should probably go away"
    declare -a unnecessaries=("ccache")
    installThem "${unnecessaries[@]}"

    echo "Python3"
   # Boost package build requires:
    py[0]="python3"
    installThem "${py[@]}"

    # Scidb 3rd party packages
    echo "Installing tools and libraries built specifically for SciDB..."
    declare -a sdb=("libscalapack-openmpi-dev"
                    "liblog4cxx10-dev"
                    "librocksdb-dev"
                   )
    installThem "${sdb[@]}"

    # Testing:
    echo "Installing Postgres 9.3..."
    declare -a pg=("postgresql-client"
                   "postgresql"
                   "postgresql-contrib")
    installThem "${pg[@]}"

    # ScaLAPACK tests:
    echo "Installing time module..."
    t[0]="time"
    installThem "${t[@]}"

    # Get pip
    pipSetup
    # Get cmake
    pipInstallCmake
    # Get pip-packages.txt
    pipInstallPackages
}

function centos6 ()
{
    #
    # This assumes register_repositories has been run
    #
    yum clean all
    yum -y makecache fast

    # Get yum-config-manager
    yumutils[0]="yum-utils"
    installThem "${yumutils[@]}"

    declare -a c6=("python-argparse" "python-paramiko")
    installThem "${c6[@]}"

    # install createrepo package used by utils/make_repo.{sh,py}
    echo "Installing createrepo..."
    repo[0]="createrepo"
    installThem "${repo[@]}"

    # Get pip
    pipSetup
    # Get cmake
    pipInstallCmake
    # Get pip-packages.txt
    pipInstallPackages
}

function centos7 ()
{
    #
    # This assumes register_repositories has been run
    #
    yum clean all
    yum -y makecache fast

    # Get yum-config-manager
    yumutils[0]="yum-utils"
    installThem "${yumutils[@]}"

    declare -a c7=("python2-paramiko")
    installThem "${c7[@]}"

    # install createrepo package used by utils/make_repo.{sh,py}
    echo "Installing createrepo..."
    repo[0]="createrepo"
    installThem "${repo[@]}"

    # Get pip
    pipSetup
    # Get cmake
    pipInstallCmake
    # Get pip-packages.txt
    pipInstallPackages
}

function centos ()
{
    # Boostrapping
    echo "Installing the tools to get started..."
    # These are probably already installed.
    declare -a bstp=("git"
	             "expect"
		     "sudo"
		     "openssh"
		     "openssh-server")
    installThem "${bstp[@]}"

    ### Compilers
    # gcc/g++/gfort version 4.9
    echo "Installing devtoolset-3 - compilers, etc..."
    declare -a dts3=("devtoolset-3-runtime"
                     "devtoolset-3-toolchain"
                     "devtoolset-3-gdb")
    installThem "${dts3[@]}"

    # Build dependencies:
    echo "Installing tools and libraries for building SciDB..."
    declare -a bdeps=("doxygen"
		      "flex" "flex-devel" "bison"
		      "zlib-devel" "bzip2-devel"
                      "redhat-lsb-core"
		      "make" "rpm-build"
		      "postgresql93-devel" "libpqxx-devel"
		      "python-devel"
		      "python-ply"
		      "cppunit-devel"
                      "openssl-devel"
		      "lapack-devel" "blas-devel"
		      "protobuf-devel" "protobuf-compiler"
		      "pam-devel"
                      "lsof"
		      "bc"
		      "openssl-devel"
                      "intel-mkl-2019.1-053"
		      "libedit-devel" "libcsv-devel")
    installThem "${bdeps[@]}"

    echo "Packages that really are not necessary, and should probably go away"
    declare -a unnecessaries=("ccache")
    installThem "${unnecessaries[@]}"

    # Scidb 3rd party packages
    echo "Installing tools and libraries built specifically for SciDB..."
    declare -a sdb=("scidb-${SCIDB_VER}-libboost-devel"
                    "mpich2scidb-devel"
                    "scalapack-scidb-mpich2scidb-devel"
                    "log4cxx-devel"
                    "rocksdb-devel"
                   )
    installThem "${sdb[@]}"

    # Testing:
    echo "Installing Postgres 9.3..."
    declare -a pg=("postgresql93"
	           "postgresql93-server"
		   "postgresql93-contrib")
    installThem "${pg[@]}"

    # ScaLAPACK tests:
    echo "Installing time module..."
    t[0]="time"
    installThem "${t[@]}"
}

echo "Preparing SciDB build VM..."

deploy_err=$(mktemp /var/tmp/prepare_toolchain.err.XXXXXX)
trap "sudo rm -rf $deploy_err" 0 15 3 2 1


case $(./os_detect.sh) in
    "CentOS 6")
	INSTALL="yum install --enablerepo=scidb3rdparty -y -q"
	CHECK="yum list installed"
	centos6
	centos
	;;
    "CentOS 7")
	INSTALL="yum install --enablerepo=scidb3rdparty -y -q"
	CHECK="yum list installed"
	centos7
	centos
	;;
    "RedHat 6")
	echo "We do not support build SciDB under RedHat 6. Please use CentOS 6 instead"
	exit 1
	;;
    "RedHat 7")
	echo "We do not support build SciDB under RedHat 7. Please use CentOS 7 instead"
	exit 1
	;;
    "Ubuntu 14.04")
	INSTALL="apt-get install -y -q"
	CHECK="dpkg -s"
	ubuntu1404
	;;
    "Ubuntu 16.04")
	INSTALL="apt-get install -y -q"
	CHECK="dpkg -s"
	ubuntu1604
	;;
    *)
	echo "Not a supported OS";
	exit 1
	;;
esac
sudo rm -rf $deploy_err

echo "...prepared SciDB build VM"
# Local Variables:
# indent-tabs-mode: nil
# tab-width: 20
# End:
