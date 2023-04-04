/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2015-2019 SciDB, Inc.
* All Rights Reserved.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/
package org.scidb.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
//import org.ini4j.Ini;


/**
 * @file AuthenticationFile.java
 *
 * @brief AuthenticationFile is a class for reading the user authenticationFile file.
 * @author Marty Corbett <mcorbett@paradigm4.com>
 */
public class AuthenticationFile
{
    private static Logger _log = Logger.getLogger(AuthenticationFile.class.getName());

    String      _fileName;
    String      _userName;
    String      _userPassword;

    /**
     * Construct via a filename
     *
     * @throws on error
     */
    public AuthenticationFile(String fileName)
        throws FileNotFoundException, IOException
    {
        _fileName   = new String(fileName);
        Properties  p = new Properties();
        p.load(new FileInputStream(_fileName));
        _userName = new String(p.getProperty("user-name"));
        _userPassword = new String(p.getProperty("user-password"));
    }


    public String getUserName()
    {
        return _userName;
    }

    public String getUserPassword()
    {
        return _userPassword;
    }

    public String toString()
    {
        return "user-name=" + getUserName() + " user-password=********";
    }


    /**
     * Verify that the file permissions are set to one of the following:
     *     (a regular file and owner has read and write permission)
     * or:
     *     (a regular file and owner has read permission)
     *
     * In addition, verify that the currently logged in user is the
     * owner of the file.
     */
    public void verifySafeFile(
        String filename,
        Boolean bypassPermissionsCheck)
        throws ConfigUserException, IOException, SecurityException
    {
        // -- Does the filename appear to be valid? --
        if(filename.length() == 0)
        {
            throw new ConfigUserException("Null configuration filename");
        }


        // -- Get the path object from the filename --

        Path path = Paths.get(filename);


        // -- Does file exist? --

        if(!Files.exists(path))
        {
            throw new ConfigUserException("file not found");
        }


        // -- Is the file a regular file? --

        if (!Files.isRegularFile(path))
        {
            throw new ConfigUserException("not a file");
        }


        // -- Does ONLY the user have read or read/write permissions? --

        try
        {
            Set<PosixFilePermission> set = Files.getPosixFilePermissions(path);
            String perms = PosixFilePermissions.toString(set);
            if(!(perms.equals("rw-------") || perms.equals("r--------")))
            {
                throw new ConfigUserException("invalid permissions");
            }
        } catch (UnsupportedOperationException e) {
            if(!bypassPermissionsCheck)
            {
                throw e;
            }

            _log.fine("the file system does not support PosixFileAttributeView");
        }


        // -- Is the individual logged in the owner of the file? --

        PosixFileAttributeView view = Files.getFileAttributeView(
            path, PosixFileAttributeView.class);
        PosixFileAttributes attr = view.readAttributes();
        String fileOwner = attr.owner().getName();

        String userName = System.getProperty("user.name");
        if(userName.compareTo(fileOwner) != 0)
        {
            throw new ConfigUserException("owner mismatch");
        }
    }
};
