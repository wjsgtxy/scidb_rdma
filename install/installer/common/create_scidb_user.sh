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
################################################################################
# This script creates the user account "scidb" (if necessary).
# It will also modify the account to use the bash shell (if needed),
# create a password-less ssh key pair (if needed),
# create a .bash_scidb file with SciDB environment variable settings.
################################################################################
getent passwd scidb > /dev/null 2>&1
if [ $? -ne 0 ]; then
    # Create user account scidb with password "paradigm4" and shell /bin/bash
    useradd --comment "SciDB user" --home-dir /home/scidb --create-home --password paTnMxusNRE7E --shell /bin/bash scidb
fi
if [ "`getent passwd scidb|awk -F: '{print $7}'`" != "/bin/bash" ]; then
    # Change scidb's shell to bash
    chsh --shell /bin/bash scidb
fi
if [ "`getent passwd scidb|awk -F: '{print $6}'`" != "/home/scidb" ]; then
    echo '********************************************************************************'
    echo '* PROBLEM: User scidb home directory must be /home/scidb.                      *'
    echo '* SOLUTION: Move scidb home directory to /home/scidb and try again.            *'
    echo '********************************************************************************'
    exit 2
fi
# SSH KEY
#
# Check if there is an id_rsa key
#
if [ -f /home/scidb/.ssh/id_rsa ]; then
    # Check that it is password-less
    grep ENCRYPTED /home/scidb/.ssh/id_rsa > /dev/null 2>&1
    if [ $? -eq 0 ]; then
	# Its not
	# Move it
	mv -f /home/scidb/.ssh/id_rsa /home/scidb/.ssh/id_rsa_orig
	mv -f /home/scidb/.ssh/id_rsa.pub /home/scidb/.ssh/id_rsa_orig.pub
    fi
fi
if [ ! -f /home/scidb/.ssh/id_rsa ]; then
    # Generate a password-less ssh key pair
    sudo -u scidb ssh-keygen -N "" -t rsa -f /home/scidb/.ssh/id_rsa
fi
# Authorize id_rsa key for use with localhost
cat /home/scidb/.ssh/id_rsa.pub >> /home/scidb/.ssh/authorized_keys
chmod 600 /home/scidb/.ssh/authorized_keys
chown scidb:$(id -g scidb) /home/scidb/.ssh/authorized_keys
# Add id_rsa key to known_hosts (to avoid authenticity questions)
sudo -u scidb ssh -n -o StrictHostKeyChecking=no -o BatchMode=yes -o LogLevel=QUIET localhost date
sudo -u scidb ssh -n -o StrictHostKeyChecking=no -o BatchMode=yes -o LogLevel=QUIET 127.0.0.1 date
sudo -u scidb ssh -n -o StrictHostKeyChecking=no -o BatchMode=yes -o LogLevel=QUIET `hostname` date
# Restore SELinux settings on the .ssh directory
restorecon -R -v /home/scidb/.ssh > /dev/null 2>&1 || true
# BASH SETTINGS
cat > /home/scidb/.bash_scidb <<EOF
export SCIDB_VER=VERSION
export PATH=/opt/scidb/\$SCIDB_VER/bin:/opt/scidb/\$SCIDB_VER/share/scidb:\$PATH
export LD_LIBRARY_PATH=/opt/scidb/\$SCIDB_VER/lib:\$LD_LIBRARY_PATH
export IQUERY_PORT=1239
export IQUERY_HOST=localhost
EOF
chmod 644 /home/scidb/.bash_scidb
sed -i '/bash_scidb/d' /home/scidb/.bashrc
echo '. /home/scidb/.bash_scidb' >> /home/scidb/.bashrc
# ALLOW POSTGRES IN
usermod -G $(id -g scidb) -a postgres
chmod g+rx /home/scidb
# PUT README.installer
sudo -u scidb cp /opt/scidb/VERSION/script/README.installer /home/scidb
