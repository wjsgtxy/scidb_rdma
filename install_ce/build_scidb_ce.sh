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

function usage
{
    cat - 1>&2 << EOF
Usage: $0 <rpm|deb|both> <working directory> <result directory> <version>

This script creates a package for the SciDB Community Edition installer.
Specify debian, rpm, or both kinds of packages.
The working directory is where the build procedure will take place.
The result directory is where the script will write the package(s).
Obviously, both directories must be writable.

In addition, the environment variables SCIDB_VER and SCIDB_SOURCE_PATH
must be set to their appropriate values.
EOF

    exit 1
}

function die
{
    echo 1>&2 "Fatal: $1"
    exit 1
}

# Main script begins here.

[ $# -lt 4 ] && usage
[[ "$1" != "rpm" && "$1" != "deb" && "$1" != "both" ]] && usage

work_dir=$2
result_dir=$3
PKG_VER=$4

[ -d $work_dir -a -w $work_dir ] || die "Working directory $work_dir does not exist or is not writable."
[ -d $result_dir -a -w $result_dir ] || die "Results directory $result_dir does not exist or is not writable."

if [ -z "$SCIDB_VER" ]; then
    echo "Need to set SCIDB_VER"
    exit 1
fi

if [ -z "$SCIDB_SOURCE_PATH" ]; then
    echo "Need to set SCIDB_SOURCE_PATH"
    exit 1
fi

m4 -DVERSION=$SCIDB_VER $SCIDB_SOURCE_PATH/install_ce/install-scidb-ce.sh.in > $result_dir/install-scidb-ce.sh
chmod +x $result_dir/install-scidb-ce.sh

if [[ "$1" == "rpm" || "$1" == "both" ]]; then

    echo $work_dir
    cd $work_dir
    [ -e rpmbuild ] && rm -rf rpmbuild
    rpmdev-setuptree

    m4 -DVERSION=$SCIDB_VER $SCIDB_SOURCE_PATH/install_ce/config.ini.in > $work_dir/rpmbuild/SOURCES/config.ini
    m4 -DVERSION=$SCIDB_VER -DPKG_VERSION=$PKG_VER $SCIDB_SOURCE_PATH/install_ce/scidb-ce.spec.in > $work_dir/rpmbuild/SPECS/scidb-ce.spec

    cp $SCIDB_SOURCE_PATH/install_ce/scripts/start_postgres.sh $work_dir/rpmbuild/SOURCES
    cp $SCIDB_SOURCE_PATH/install_ce/scripts/setup_scidb.sh $work_dir/rpmbuild/SOURCES
    cp $SCIDB_SOURCE_PATH/deployment/common/configure_postgresql.py $work_dir/rpmbuild/SOURCES
    cp $SCIDB_SOURCE_PATH/deployment/common/os_detect.sh $work_dir/rpmbuild/SOURCES

    cd $work_dir/rpmbuild/SPECS
    rpmbuild -ba scidb-ce.spec

    cp $work_dir/rpmbuild/RPMS/noarch/scidb-$SCIDB_VER-ce-$PKG_VER-1.noarch.rpm $result_dir
fi

if [[ "$1" == "deb" || "$1" == "both" ]]; then
    pushd $work_dir >/dev/null 2>&1
    mkdir -p ./scidb-$SCIDB_VER-ce-$PKG_VER/DEBIAN
    mkdir -p ./scidb-$SCIDB_VER-ce-$PKG_VER/opt/scidb/$SCIDB_VER/scripts
    mkdir -p ./scidb-$SCIDB_VER-ce-$PKG_VER/opt/scidb/$SCIDB_VER/etc

    m4 -DVERSION=$SCIDB_VER $SCIDB_SOURCE_PATH/install_ce/config.ini.in > ./scidb-$SCIDB_VER-ce-$PKG_VER/opt/scidb/$SCIDB_VER/etc/config.ini

    m4 -DVERSION=$SCIDB_VER $SCIDB_SOURCE_PATH/install_ce/DEBIAN/control > ./scidb-$SCIDB_VER-ce-$PKG_VER/DEBIAN/control
    hname=$(hostname); hname="$hname.$(hostname -d)"
    m4 -DVERSION=$SCIDB_VER -DDATE="$(date)" -DHOSTNAME=$hname $SCIDB_SOURCE_PATH/install_ce/DEBIAN/copyright > ./scidb-$SCIDB_VER-ce-$PKG_VER/DEBIAN/copyright
    cp $SCIDB_SOURCE_PATH/install_ce/DEBIAN/compat ./scidb-$SCIDB_VER-ce-$PKG_VER/DEBIAN/
    userinfo=$(getent passwd $(whoami) | cut -d: -f 5)
    [[ $userinfo == "" ]] && userinfo=$(whoami)
    userinfo="$userinfo <$(whoami)@paradigm4.com>"
    m4 -DVERSION=$SCIDB_VER -DUSERINFO="$userinfo" $SCIDB_SOURCE_PATH/install_ce/DEBIAN/changelog > ./scidb-$SCIDB_VER-ce-$PKG_VER/DEBIAN/changelog

    cp $SCIDB_SOURCE_PATH/install_ce/scripts/start_postgres.sh ./scidb-$SCIDB_VER-ce-$PKG_VER/opt/scidb/$SCIDB_VER/scripts
    cp $SCIDB_SOURCE_PATH/install_ce/scripts/setup_scidb.sh ./scidb-$SCIDB_VER-ce-$PKG_VER/opt/scidb/$SCIDB_VER/scripts
    cp $SCIDB_SOURCE_PATH/deployment/common/configure_postgresql.py ./scidb-$SCIDB_VER-ce-$PKG_VER/opt/scidb/$SCIDB_VER/scripts
    cp $SCIDB_SOURCE_PATH/deployment/common/os_detect.sh ./scidb-$SCIDB_VER-ce-$PKG_VER/opt/scidb/$SCIDB_VER/scripts

    dpkg-deb --build ./scidb-$SCIDB_VER-ce-$PKG_VER
    mv scidb-$SCIDB_VER-ce-$PKG_VER.deb $result_dir
    popd >/dev/null 2>&1
fi
