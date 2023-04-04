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
package org.scidb.iquery;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;

/**
 * Config is a class managing the config parameters for Iquery.
 * It could be placed inside Iquery.
 * I keep it separate for now, in case it is needed by a GUI client.
 * @author Donghui Zhang (original)
 * @author Marty Corbett (add setters, getters, and show method)
 */
public class Config
{
    private int _precision         = 6;
    private String _host           = new String("localhost");
    private int _port              = 1239;
    private String _query          = new String();
    private String _queryFile      = new String();
    private String _result         = new String("console");
    private String _format         = new String("dcsv");
    private String _authFile       = new String();
    private String _userName       = new String();
    private String _userPassword   = new String();
    private boolean _verbose       = false;
    private boolean _timer         = false;
    private boolean _noFetch       = false;
    private boolean _afl           = false;
    private boolean _version       = false;
    private boolean _ignoreErrors  = false;
    private boolean _bypassUsrCfgPermsChk = false;
    private boolean _admin         = false;

    public void setPrecision(int newValue) { _precision = newValue; }
    public void setHost(String newValue) { _host = newValue; }
    public void setPort(int newValue) { _port = newValue; }
    public void setQuery(String newValue) { _query = newValue; }
    public void setQueryFile(String newValue) { _queryFile = newValue; }
    public void setResult(String newValue) { _result = newValue; }
    public void setFormat(String newValue) { _format = newValue; }
    public void setAuthFile(String newValue) { _authFile = newValue; }
    public void setUserName(String newValue) { _userName = newValue; }
    public void setUserPassword(String newValue) { _userPassword = newValue; }
    public void setVerbose(boolean newValue) { _verbose = newValue; }
    public void setTimer(boolean newValue) { _timer = newValue; }
    public void setNoFetch(boolean newValue) { _noFetch = newValue; }
    public void setAfl(boolean newValue) { _afl = newValue; }
    public void setAdmin(boolean newValue) { _admin = newValue; }
    public void setVersion(boolean newValue) { _version = newValue; }
    public void setIgnoreErrors(boolean newValue) { _ignoreErrors = newValue; }
    public void setBypassUsrCfgPermsChk(
        boolean newValue)
    {
        _bypassUsrCfgPermsChk = newValue;
    }

    public int getPrecision() { return _precision; }
    public String getHost() { return _host; }
    public int getPort() { return _port; }
    public String getQuery() { return _query; }
    public String getQueryFile() { return _queryFile; }
    public String getResult() { return _result; }
    public String getFormat() { return _format; }
    public String getAuthFile() { return _authFile; }
    public String getUserName() { return _userName; }
    public String getUserPassword() { return _userPassword; }
    public boolean getVerbose() { return _verbose; }
    public boolean getTimer() { return _timer; }
    public boolean getNoFetch() { return _noFetch; }
    public boolean getAfl() { return _afl; }
    public boolean getAdmin() { return _admin; }
    public boolean getVersion() { return _version; }
    public boolean getIgnoreErrors() { return _ignoreErrors; }
    public boolean getBypassUsrCfgPermsChk()
    {
        return _bypassUsrCfgPermsChk;
    }

    public void show()
    {
        System.out.println("precision    = " + getPrecision());
        System.out.println("host         = " + getHost());
        System.out.println("port         = " + getPort());
        System.out.println("query        = " + getQuery());
        System.out.println("queryFile    = " + getQueryFile());
        System.out.println("result       = " + getResult());
        System.out.println("format       = " + getFormat());
        System.out.println("authFile     = " + getAuthFile());
        System.out.println("userName     = " + getUserName());
        System.out.println("userPassword = *");
        System.out.println("verbose      = " + getVerbose());
        System.out.println("timer        = " + getTimer());
        System.out.println("noFetch      = " + getNoFetch());
        System.out.println("afl          = " + getAfl());
        System.out.println("admin        = " + getAdmin());
        System.out.println("version      = " + getVersion());
        System.out.println("ignoreErrors = " + getIgnoreErrors());
        System.out.println("bypassUsrCfgPermsChk = " + getBypassUsrCfgPermsChk());
    }

    /**
     * Parse command-line parameters into the Config object.
     *
     * @note (For developers:) Please do not use a number as either a short option name or a long option name.
     *       This will ensure that DefaultParser::isArgument() returns true if and only if the token is not an option.
     */
    public void parse(String[] args) throws ParseException
    {
	    Options options = new Options();
	    options.addOption(Option.builder("w").required(false).hasArg(true).longOpt("precision").build());
	    options.addOption(Option.builder("c").required(false).hasArg(true).longOpt("host").build());
	    options.addOption(Option.builder("p").required(false).hasArg(true).longOpt("port").build());
	    options.addOption(Option.builder("q").required(false).hasArg(true).longOpt("query").build());
	    options.addOption(Option.builder("f").required(false).hasArg(true).longOpt("query_file").build());
	    options.addOption(Option.builder("r").required(false).hasArg(true).longOpt("result").build());
	    options.addOption(Option.builder("o").required(false).hasArg(true).longOpt("format").build());
	    options.addOption(Option.builder("A").required(false).hasArg(true).longOpt("auth-file").build());
	    //options.addOption(Option.builder("U").required(false).hasArg(true).longOpt("user-name").build());
	    //options.addOption(Option.builder("P").required(false).hasArg(true).longOpt("user-password").build());
	    options.addOption(Option.builder("v").required(false).hasArg(false).longOpt("verbose").build());
	    options.addOption(Option.builder("t").required(false).hasArg(false).longOpt("timer").build());
	    options.addOption(Option.builder("n").required(false).hasArg(false).longOpt("no_fetch").build());
	    options.addOption(Option.builder("a").required(false).hasArg(false).longOpt("afl").build());
	    options.addOption(Option.builder("d").required(false).hasArg(false).longOpt("admin").build());
	    options.addOption(Option.builder("h").required(false).hasArg(false).longOpt("help").build());
	    options.addOption(Option.builder("V").required(false).hasArg(false).longOpt("version").build());
	    options.addOption(Option.builder("i").required(false).hasArg(false).longOpt("ignore_errors").build());
	    options.addOption(Option.builder("b").required(false).hasArg(false).longOpt("bypass_usr_cfg_perms_chk").build());

	    CommandLineParser parser = new DefaultParser();
	    CommandLine line = parser.parse(options, args);
	    Option[] processedOptions = line.getOptions();

	    for (Option o: processedOptions)
	    {
            switch (o.getId())
            {
            case 'w':
                setPrecision(Integer.parseInt(o.getValue()));
                break;
            case 'c':
                setHost(o.getValue());
                break;
            case 'p':
                setPort(Integer.parseInt(o.getValue()));
                break;
            case 'q':
                setQuery(o.getValue());
                break;
            case 'f':
                setQueryFile(o.getValue());
                break;
            case 'r':
                setResult(o.getValue());
                break;
            case 'o':
                setFormat(o.getValue());
                break;
            case 'A':
                setAuthFile(o.getValue());
                break;
            case 'v':
                setVerbose(true);
                break;
            case 't':
                setTimer(true);
                break;
            case 'n':
                setNoFetch(true);
                break;
            case 'a':
                setAfl(true);
                break;
            case 'd':
                setAdmin(true);
                break;
            case 'i':
                setIgnoreErrors(true);
                break;
            case 'b':
                setBypassUsrCfgPermsChk(true);
                break;
            case 'h':
                printHelp();
                System.exit(0);
            case 'V':
                printVersion();
                System.exit(0);
            case '?':
                System.exit(1);
            }
        }
    }

    private void printVersion()
    {
        // Not implemented yet.
        assert false;
    }

    private void printHelp()
    {
        System.out.println("Iquery is a command-line tool that interacts with a SciDB server.\n");
        System.out.println("usage: CLASSPATH=.../scidb4j.jar  java  org.scidb.iquery.Iquery  [options] \"QUERY_STRING\"\n");
        System.out.println("Available options:");
        System.out.println("     -w [ --precision ]      arg Precision for printing floating point numbers. ");
        System.out.println("                                 Default is 6");
        System.out.println("     -c [ --host ]           arg Host of one of the cluster instances. Default is ");
        System.out.println("                                 'localhost'");
        System.out.println("     -p [ --port ]           arg Port for connection. Default is 1239");
        System.out.println("     -q [ --query ]          arg Query to be executed");
        System.out.println("     -f [ --query-file ]     arg File with query to be executed");
        System.out.println("     -r [ --result ]         arg Filename with result array data.");
        System.out.println("     -o [ --format ]         arg Output format: auto, csv, dense, csv+, lcsv+, text, ");
        System.out.println("                                 sparse, lsparse, store, text, opaque, tsv, tsv+, ");
        System.out.println("                                 ltsv+, dcsv. Default is 'dcsv'.");
        System.out.println("     -A [ --auth-file ]      arg Authentication file name");
        //System.out.println("     -U [ --user-name ]      arg User name");
        //System.out.println("     -P [ --user-password ]  arg User password");
        System.out.println("     -v [ --verbose ]        Print debug info. Disabled by default");
        System.out.println("     -t [ --timer ]          Print query execution time (in milliseconds)");
        System.out.println("     -n [ --no-fetch ]       Skip data fetching. Disabled by default");
        System.out.println("     -a [ --afl ]            Switch to AFL query language mode. AQL by default");
        System.out.println("        [ --admin ]          Create an administrative connection to use reserved system resources");
        System.out.println("     -h [ --help ]           Show help");
        System.out.println("     -V [ --version ]        Show version info");
        System.out.println("     -i [--ignore-errors]    Ignore execution errors in batch mode");
    }
};
