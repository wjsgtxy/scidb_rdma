#!/bin/bash
#
# BEGIN_COPYRIGHT
#
# Copyright (C) 2008-2018 SciDB, Inc.
# All Rights Reserved.
#
# This file is part of the Paradigm4 Enterprise SciDB distribution kit
# and may only be used with a valid Paradigm4 contract and in accord
# with the terms and conditions specified by that contract.
#
# END_COPYRIGHT
#


function usage()
{
    echo "Usage: $0 <result dir> [<src_dir>]"
    echo "If you don't specify the src_dir, you need to set SCIDB_SOURCE_PATH (usually <dev_dir>/scidbtrunk)."
    exit 1
}

function die()
{
    echo 1>&2
    exit 1
}

[ "$#" -gt 2 ] && usage
[ "$#" -lt 1 ] && usage

if [ "$#" -ne 2 ]; then
    if [ -z ${SCIDB_SOURCE_PATH} ]; then
	usage
    else
	src_dir="${SCIDB_SOURCE_PATH}"
    fi
else
    src_dir="$2"
fi

export SCIDB_VERSION_MAJOR=`awk -F . '{print $1}' ${src_dir}/version`
export SCIDB_VERSION_MINOR=`awk -F . '{print $2}' ${src_dir}/version`
export SCIDB_VERSION_PATCH=`awk -F . '{print $3}' ${src_dir}/version`

build_dir=$(mktemp -d /var/tmp/c7-build.XXXXXX)
trap "sudo rm -rf $build_dir" 0 15 3 2 1

chroot_result_dir="$1"
source_dir=~/scidb_3rdparty_sources
SCIDB_VER=$SCIDB_VERSION_MAJOR.$SCIDB_VERSION_MINOR

#pushd "$(dirname "$0")"
#script_dir="`pwd`"
#popd
spec_dir="${src_dir}/3rdparty/centos7"

baseurl="https://downloads.paradigm4.com/centos7/3rdparty_sources"

# original URLs stored in ${base_url}/original.txt
sources="
boost_1_54_0.tar.bz2
"

echo Preparing dirs
mkdir -p "${build_dir}"/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS} "${source_dir}" "${chroot_result_dir}" || die "Unable to make build directories in ${build_dir}."
echo build: ${build_dir}, source: ${source_dir}, chroot: ${chroot_result_dir}

echo Downloading sources to "${source_dir}"
pushd "${source_dir}"
    for filename in $sources; do
        [ ! -f $filename ] && wget "$baseurl/${filename}" -O tmp && mv tmp $filename
    done
popd

echo Copying sources to "${build_dir}/SOURCES"
cp -p "${source_dir}"/*  "${spec_dir}"/centos-7-x86_64.cfg "${spec_dir}"/patches/*.patch "${spec_dir}"/patches/*.in "${build_dir}/SOURCES"

echo Copying specs to "${build_dir}/SOURCES"
for spec_file_name in `(cd ${spec_dir}; ls *.spec)`; do
    cat "${spec_dir}"/${spec_file_name} | sed -e "s/SCIDB_VERSION_MAJOR/${SCIDB_VERSION_MAJOR}/" | sed -e "s/SCIDB_VERSION_MINOR/${SCIDB_VERSION_MINOR}/" | sed -e "s/SCIDB_VERSION_PATCH/${SCIDB_VERSION_PATCH}/" > "${build_dir}"/SPECS/${spec_file_name}
done;

echo Building source packages
pushd "${build_dir}"/SPECS
    for f in *.spec; do
        echo Building $f source package
        rpmbuild -D"_topdir ${build_dir}" -bs $f || die "Cannot build $f"
    done
popd

echo Building dependencies in chroot
pushd "${build_dir}"/SRPMS
    for f in *.src.rpm; do
        echo Building binary package from $f
        python ${spec_dir}/../../utils/chroot_build.py -b -d centos-7-x86_64 -s $f -r "${chroot_result_dir}" || die Cannot build $f
    done
popd

echo Removing build dir "${build_dir}"
sudo rm -rf "${build_dir}"

echo Done. Take result from "${chroot_result_dir}"
