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

user=${1}
key=${2}
update=0

# Add the passed in public key to the authorized_keys file
#
# Note: I have no idea why just doing a grep and checking return code does NOT work
#       instead subshell doing a cat grep wc works
function add_public_key ()
{
    local new_key="${1}"
    if [ "0" == `cat ${HOME}/.ssh/authorized_keys | grep "${new_key}" | wc -l || true` ]; then
	echo "${new_key}" >> ${HOME}/.ssh/authorized_keys
	update=1
    fi;
}

# disable_host_checking
#   Otherwise ssh connect would/can ask about the "adding the host to the known host list"
#
# Note: I have no idea why just doing a grep and checking return code does NOT work
#       instead subshell doing a cat grep wc works
function disable_host_checking ()
{
    if [ $(cat ${HOME}/.ssh/config | grep 'Host \*' | wc -l) -eq 0 ]; then
cat <<EOF >> ${HOME}/.ssh/config
Host *
   StrictHostKeyChecking no
   UserKnownHostsFile=/dev/null
   LogLevel=error
EOF
    fi
}

function selinux_home_ssh ()
{
    if /usr/sbin/selinuxenabled ; then
        chcon -R -v -t user_ssh_home_t ~/.ssh > /dev/null 2>&1 || true
    fi
}

# Update right to ~/.ssh directory
function update_rights ()
{
    disable_host_checking
    chmod go-rwx,u+rwx ${HOME}/.ssh
    chmod a-x,go-rw,u+rw ${HOME}/.ssh/*

    case $(./os_detect.sh) in
	"CentOS 6"|"CentOS 7"|"RedHat 6"|"RedHat 7")
	    selinux_home_ssh
	    ;;
	"Ubuntu 14.04"|"Ubuntu 16.04")
	    :
	    ;;
	*)
	    echo "Not a supported OS";
	    exit 1
	    ;;
    esac
}

mkdir -p ${HOME}/.ssh
private=${HOME}/.ssh/id_rsa
public=${HOME}/.ssh/id_rsa.pub

if [[ ("1" != `ls ${private} | wc -l || true`) || ("1" != `ls ${public} | wc -l || true`)]]; then
    rm -f ${private}
    rm -f ${public}
    echo "" | ssh-keygen -t rsa
fi;

add_public_key "${key}"
update_rights

exit 0
