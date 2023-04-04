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
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.util.*;
import java.util.logging.Logger;


/**
 * @file ConfigUser.java
 *
 * @brief ConfigUser is a class for reading the user configuration file.
 * @author Marty Corbett <mcorbett@paradigm4.com>
 */
public class ConfigUser
{
    private static Logger _log = Logger.getLogger(ConfigUser.class.getName());

    public class ConfigOption
    {
        private Object _defaultValue = null;

        public ConfigOption(Object defaultValue)
        {
            _defaultValue = defaultValue;
        }

        public Object getDefaultValue() { return _defaultValue; }
    };


    /**
     * _configUser holds the singleton 'instance' of the class
     */
    private static ConfigUser _configUser = new ConfigUser();

    /**
     * _hashMap holds the key/value pairs retrieved during parse()
     */
    private HashMap<String, Object> _hashMap =
        new HashMap<String, Object>();

    /**
     * _options allows the caller of this class to specify the which
     *   keys are valid in the file and what their object type should be
     */
    private HashMap<String, ConfigOption> _options =
        new HashMap<String, ConfigOption>();


    /**
     * Provide a private constructor thus preventing other classes
     * from instantiating instances of the ConfigUser.
     */
    private ConfigUser() { }

    /**
     * @brief Retrieve the singleton 'instance' of the class
     */
    public static ConfigUser getInstance()
    {
        return _configUser;
    }

    public void addObject(String key, Object defaultValue)
    {
        _hashMap.put(key, defaultValue);
        _options.put(key, new ConfigOption(defaultValue));
    }

    /**
     * @brief Retrieve the Object from the local key/value pair map
     *
     * @param key - The key to search for in the map
     * @return Object - The value associated with the key in the map
     */
    public Object get(String key)
    {
        return _hashMap.get(key);
    }

    public void show()
    {
        for (Map.Entry<String, Object> entry : _hashMap.entrySet())
        {
            String key = entry.getKey();
            Object obj = (Object) entry.getValue();

            _log.fine(
                "key=" + key + " object type is " + obj.getClass());

            if(obj.getClass() == Vector.class)
            {
                Vector v = (Vector) obj;
                Iterator<Integer> it = v.iterator();
                while(it.hasNext())
                {
                    Object value = it.next();
                    _log.fine(
                        "  value=:  " + value +
                        "  type: " + value.getClass());
                }
            } else {
                _log.fine("  value=:  " + obj.toString());
            }
        }
    }


    /**
     * @brief Parse the filename extracting key/value pairs
     *
     * The file is expected to have the following format:
     *     {
     *         "key1" : "value1",
     *         "key2" : "value2",
     *         "key3" : "value3",
     *         "key4" : "value4a":"value4b":"value4c",
     *         ...
     *         "keyN" : "valueN"
     *     }
     *
     *     Example:
     *     {
     *     "user-name":"root",
     *     "user-password":"Paradigm4",
     *     "test-int":1234:9824,
     *     "test-double":5.04,
     *     "test-bool":false
     *     }
     *
     * The parsing will populate the _hashMap with String/Object pairs.
     *
     * @param filename - The file to be parsed
     * @throws on error
     */
    public void parse(String filename)
        throws ConfigUserException, IOException
    {
        BufferedReader br = null;
        String line = null;
        Boolean beginParsing=false;
        ConfigUserException lastException = null;

        try
        {
            br = new BufferedReader(new FileReader(filename));

            while((line = br.readLine()) != null)
            {
                if(line.compareTo("{") == 0)
                {
                    beginParsing=true;
                } else if(line.compareTo("}") == 0) {
                    beginParsing=false;
                } else if(beginParsing == true) {
                    StringTokenizer st = new StringTokenizer(line, ":,\"");
                    while(st.hasMoreElements())
                    {
                        // --  Get the next key from the file
                        String key = st.nextElement().toString();
                        if(!st.hasMoreElements())
                        {
                            throw new ConfigUserException(
                                "Received key=" + key + " but no value");
                        }

                        ConfigOption cfgOption = _options.get(key);
                        Object defaultValue = cfgOption.getDefaultValue();

                        int numTokens = st.countTokens();
                        Vector values = new Vector(numTokens);

                        for(int token = 0;  token < numTokens;  token++)
                        {
                            Object value = st.nextElement();

                            if(defaultValue.getClass() == Integer.class) {
                                value = Integer.parseInt(value.toString());
                            } else if(defaultValue.getClass() == Double.class) {
                                value = Double.parseDouble(value.toString());
                            } else if(defaultValue.getClass() == Boolean.class) {
                                value = Boolean.parseBoolean(value.toString());
                            }

                            if(cfgOption == null)
                            {
                                throw new ConfigUserException(
                                    "Invalid key=" + key);
                            }

                            if(value.getClass() != defaultValue.getClass())
                            {
                                throw new ConfigUserException(
                                    "Invalid type=" + value.getClass() +
                                    " should be =" + defaultValue.getClass());
                            }

                            values.add(value);
                        }

                        if(values.size() == 1)
                        {
                            _hashMap.put(key, values.firstElement());
                        } else {
                            _hashMap.put(key, values);
                        }
                    }
                }
            }
        } catch(Exception ex) {
            lastException = new ConfigUserException(ex);
            throw lastException;
        } finally {
            try
            {
                if(br != null)
                {
                    br.close();
                }
            } catch(Exception ex) {
                if(lastException != null)
                {
                    // throw the orginal exception, not the one caused
                    // by br.close()
                    throw lastException;
                }
            }
        }
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
