#!/usr/bin/env python2.7

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

from __future__ import print_function
import argparse
import cmd
import os
import re
import socket
import ssl
import sys
import uuid
import pprint
import traceback

from ConfigParser import RawConfigParser

from scidblib import Ccm_pb2 as cpb
# from scidblib import AppError
from scidblib.SciDBMessage import MessageTypes
from scidblib.SciDBMessage import dbg
from scidblib.SciDBMessage import SciDBMessage
import scidblib.SciDBMessage


class Iquery(cmd.Cmd, object):
    """Simple command line processor to send Ccm Messages and AFL queries to SciDB
    client communication manager."""

    def __init__(self, **kwargs):
        self._sock = kwargs.pop('socket', None)
        self.prompt = kwargs.pop('prompt', None)
        self.fmt = kwargs.pop('format', None)
        self.cliargs = kwargs.pop('cliargs', None)
        self._lastreply = None
        self._session = None
        self.encodedSessions = {}
        self.encodeOutput = False
        cmd.Cmd.__init__(self, **kwargs)
        self.allow_cli_args = False
        self.allow_redirection = False
        self._set_session(uuid.UUID('00000000-0000-0000-0000-000000000000'))
        # Unfortunately, we cannot use the history file written by the old
        # iquery client, since that uses libedit readline. The python
        # implementation uses the gnu readline. The formats of the history file
        # formats are incompatible. So we use the history file called
        # 'ccmtest.history'
        try:
            from xdg.BaseDirectory import xdg_config_home
            self.history_file = os.path.join(xdg_config_home, 'scidb', 'ccmtest.history')
        except ImportError:
            self.history_file = os.path.join(os.environ['HOME'], '.ccmtest.history')
        self.max_history_items = 1000
        dbg('History File: {0}'.format(self.history_file))

    def preloop(self):
        user, password = self._getAuthCredentials()
        self.do_authlogon(user)
        self.do_authresponse(password)
        # load the history file
        try:
            import readline
            readline.read_history_file(self.history_file)
            readline.set_history_length(self.max_history_items)
        except ImportError:
            pass
        except IOError:
            # no history file, no problem.
            pass
        super(Iquery, self).preloop()

    def postloop(self):
        # Saving history file.
        import readline
        readline.write_history_file(self.history_file)
        super(Iquery, self).postloop()

    def postcmd(self, stop, line):
        """Hook method executed just after a command dispatch is finished."""
        return super(Iquery, self).postcmd(stop, line)

    def emptyline(self):
        pass

    def default(self, a):
        """The default action is how an AFL Query, or ccmtest.py command is parsed and
        executed.

        Entries starting with 'set' are processed as ccmtest.py settings,
        otherwise the command is processed as an AFL Query.

        Commands starting with a '#' are considered to be comments and are
        not processed.
        """
        p = re.compile(' *#')
        if p.match(a):
            dbg("not doing anything with comment: {0}".format(a))
        else:
            a = a.rstrip(';')
            a = a.lstrip(' ')
            if len(a) > 0:
                self.do_executequery(a)
                if not self._isError():
                    self.do_fetchingot()

    def do_authlogon(self, a):
        """Send an AuthLogon Protobuf message to the server.
        String value passed is read as the username.

        If no username is provided, the auth credentials file is used if
        found.
        """
        msg = SciDBMessage(MessageTypes.AuthLogon)
        pbuf = msg.get_protobuf()
        if a:
            pbuf.username = a
        else:
            pbuf.username = self._getAuthCredentials()[0]
        self._lastreply = self._send_request_and_get_reply(msg)
        if self._isError():
            print('ERROR returned from AuthLogon: {0} {1}'.format(
                self._lastreply.get_protobuf().code,
                self._lastreply.get_protobuf().text))
        else:
            pbuf = self._lastreply.get_protobuf()
            if pbuf.challenge:
                dbg('Your Server has this Question: {0}'.format(pbuf.challenge))
                if pbuf.cookie:
                    self._authcookie = pbuf.cookie

    def do_authlogin(self, a):
        """For flubber fingers who type login rather than logon"""
        self.do_authlogon(a)

    def do_authresponse(self, a):
        msg = SciDBMessage(MessageTypes.AuthResponse)
        pbuf = msg.get_protobuf()
        if a:
            pbuf.answer = a
        else:
            pbuf.answer = self._getAuthCredentials()[1]
        if self._authcookie:
            pbuf.cookie = self._authcookie
        self._lastreply = self._send_request_and_get_reply(msg)
        pbuf = self._lastreply.get_protobuf()
        if isinstance(pbuf, cpb.Error):
            print('ERROR returned from AuthResponse: {0} {1}'.format(pbuf.code,
                                                                     pbuf.text))
        else:
            if not pbuf.authenticated:
                print('Authentication failed: {0}'.format(pbuf.reason))

    def do_executequery(self, a):
        msg = SciDBMessage(MessageTypes.ExecuteQuery)
        pbuf = msg.get_protobuf()
        if not a:
            a = 'list(\'arrays\')'
        a = self._remove_cmd2_stringstart(a)
        dbg('Sending ExecuteQuery: {0}'.format(a))
        pbuf.query = a
        if self.fmt is not None:
            dbg(' setting ingot_format: {0}'.format(self.fmt))
            pbuf.ingot_format = self.fmt
        # pbuf.ingotsize = 42
        self._lastreply = self._send_request_and_get_reply(msg)
        pbuf = self._lastreply.get_protobuf()
        if isinstance(pbuf, cpb.Error):
            print('ERROR returned from ExecuteQuery: {0} {1}'.format(
                pbuf.code,
                pbuf.text))
        else:
            dbg("Executed Query -- {0}\n  {1}".format(pbuf.conversation_id,
                                                      a))

    def do_fetchingot(self, a=None):
        msg = SciDBMessage(MessageTypes.FetchIngot)
        pbuf = msg.get_protobuf()
        if not a:
            a = str(self._session) if self._session else 'Default Filled'
        pbuf.conversation_id = a
        self._lastreply = self._send_request_and_get_reply(msg)
        pbuf = self._lastreply.get_protobuf()
        if isinstance(pbuf, cpb.Error):
            print('ERROR returned from FetchIngot: {0} {1}'.format(
                pbuf.code,
                pbuf.text))
        else:
            if self._lastreply.get_protobuf().binary_size > 0:
                dbg("binary_size = {0}, reply._recv_data = {1}".format(
                    self._lastreply.get_protobuf().binary_size,
                    len(self._lastreply._recv_data)))
            if self._lastreply._recv_data:
                dbg("We have {0} bytes in _recv_data:\n{1}".format(len(self._lastreply._recv_data),
                                                                   self._lastreply._recv_data))
                self._lastreply._recv_data = b''

    def do_echo(self, a):
        """Echo the text to stdout"""
        if a is None:
            print("")
        else:
            print("{0}".format(a))

    def do_get(self, a):
        """Get configuration vale of this client application:
           set <key>=<value>
        Keys
        ----
        format: (string) Response format of query (tsv, csv, dcsv)
        prompt: (string) User prompt string
        verbose: (number) indicating the verbosity level
        session: (string) get the current Session UUID to communicate with scidb.
                 (string is of the form '00000000-0000-0000-0000-000000000000'"""
        if 'format' == a.lower():
            print('{0}'.format(self.fmt))
        elif 'prompt' == a.lower():
            print('{0}'.format(self.prompt))
        elif 'verbose' == a.lower():
            print('{0}'.format(scidblib.SciDBMessage._verbose))
        elif 'session' == a.lower():
            if self.encodeOutput:
                print('Session-{0}'.format(self.encodedSessions[self._session]))
            else:
                print('{0}'.format(self._session))
        elif 'sessions' == a.lower():
            if self.encodeOutput:
                print('{0} Sessions'.format(len(self.encodedSessions)))
            else:
                print('{0}'.format(self.encodedSessions))
        else:
            print('ERROR: unknown setting \'{0}\''.format(a))

    def do_set(self, a):
        """Set configuration of this client application:
           set <key>=<value>
        Keys
        ----
        format: (string) Response format of query (tsv, csv, dcsv)
        prompt: (string) User prompt string
        verbose: (number) indicating the verbosity level
        session: (string) set the Session UUID to communicate with scidb.
             (string is of the form '00000000-0000-0000-0000-000000000000'"""
        keyval = dict(re.findall(r'(\S+)=(".*?"|\S+)', a))
        if 'encode' in keyval:
            if keyval['encode'].lower() in ('on', '1', 'true'):
                self.encodeOutput = True
            else:
                self.encodeOutput = False
        if 'format' in keyval:
            self.fmt = keyval['format']
        if 'prompt' in keyval:
            self.prompt = keyval['prompt'] + ' '
        if 'verbose' in keyval:
            scidblib.SciDBMessage._verbose = int(keyval['verbose'])
        if 'session' in keyval:
            self._set_session(uuid.UUID(keyval['session']))
        if 'debug' in keyval:
            global _debug
            _debug = True if keyval['debug'] in ['True', 'true', 'on'] else False
            print('_debug is {0}'.format(_debug))

    def do_reset(self, a=None):
        """Reset/Clear the CcmSessionId"""
        self._set_session(uuid.UUID('00000000-0000-0000-0000-000000000000'))
        # print('Reset Session')

    def do_reset_sessions(self, a):
        """Clear all the known sessions and reset the encodedSessions dict"""
        self.encodedSessions = {}
        self._set_session(uuid.UUID('00000000-0000-0000-0000-000000000000'))
        dbg('SESSIONS: \n{0}'.format(pprint.pformat(self.encodedSessions, indent=2)))

    def do_exit(self, line):
        """exit the program"""
        self.do_EOF(line)

    def do_quit(self, line):
        """exit the program"""
        self.do_EOF(line)

    def do_EOF(self, line):
        """exit the program"""
        print("{0}".format(line))
        return True

    def do_delayquery(self, a):
        res = a.split(' ', 1)
        delay = int(res[0])
        msg = SciDBMessage(MessageTypes.ExecuteQuery)
        if self._session is not None:
            msg._header.session = self._session
        pbuf = msg.get_protobuf()
        pbuf.query = res[1]
        pbuf.ingot_format = self.fmt
        msg.send_with_delay(self._sock, delay)
        self._lastreply = self._get_reply()
        pbuf = self._lastreply.get_protobuf()
        if isinstance(pbuf, cpb.Error):
            print('ERROR returned from ExecuteQuery: {0} {1}'.format(pbuf.code,
                                                                     pbuf.text))
        else:
            self.do_fetchingot(pbuf.conversation_id)

    def do_send_bad_protobuf(self, ptype):
        header_type = MessageTypes.AuthLogon
        try:
            if len(ptype.strip()) > 0 and ptype is not None:
                header_type = eval(ptype)
                # Test that we have an integer
                print('header_type: {0}'.format(header_type))
        except BaseException as e:
            print('Failure trying to set MessageType: {0}'.format(e))

        msg = SciDBMessage(header_type)
        msg._header.msg_type = header_type
        self._lastreply = self._send_bad_request_and_get_reply(msg)
        pbuf = self._lastreply.get_protobuf()
        if isinstance(pbuf, cpb.Error):
            print('Received expected response from server: {0} {1}'.format(pbuf.code,
                                                                           pbuf.text))
        else:
            print('Unexpected Return type: {0}'.format(
                self._lastreply._header.msg_type))

    def do_send_bad_version(self, a):
        try:
            self.do_reset()
            msg = SciDBMessage(MessageTypes.AuthLogon, version=3)
            pbuf = msg.get_protobuf()
            if a:
                pbuf.username = a
            self._lastreply = self._send_request_and_get_reply(msg)
            pbuf = self._lastreply.get_protobuf()
            # The new session is
            if isinstance(pbuf, cpb.Error):
                print('ERROR returned from AuthLogon: {0} {1}'.format(pbuf.code,
                                                                      pbuf.text))
            else:
                if pbuf.challenge:
                    dbg('Your Server has this Question: {0}'.format(pbuf.challenge))
                if pbuf.cookie:
                    self._authcookie = pbuf.cookie
        except BaseException as e:
            print("send_bad_version Exception: {0}".format(e))

    def do_send_invalid_type(self, a):
        try:
            self.do_reset()
            # Sending a response (rather than a request is invalid)
            msg = SciDBMessage(MessageTypes.ExecuteQueryResponse)
            self._lastreply = self._send_request_and_get_reply(msg)
            pbuf = self._lastreply.get_protobuf()
            # The new session is
            if isinstance(pbuf, cpb.Error):
                print('Received expected response from server: {0} {1}'.format(pbuf.code,
                                                                               pbuf.text))
            else:
                print('Unexpected Return type: {0}'.format(self._lastreply._header.msg_type))
        except BaseException as e:
            print("send_invalid_type Exception: {0}".format(e))

    def _set_session(self, sess):
        if sess not in self.encodedSessions:
            count = len(self.encodedSessions)
            self.encodedSessions[sess] = count
        self._session = sess
        dbg('SESSIONS: \n{0}'.format(pprint.pformat(self.encodedSessions, indent=2)))

    def _getAuthCredentials(self):
        user_name = ''
        user_password = ''
        if self.cliargs.auth_file:
            try:
                p = RawConfigParser()
                with open(self.cliargs.auth_file, 'r') as f:
                    p.readfp(f)
                user_name = p.get('security_password', 'user-name')
                user_password = p.get('security_password', 'user-password')
            except Exception:  # as e:
                # print("{0}".format(e))
                pass
        return user_name, user_password

    def _isError(self):
        if self._lastreply is None:
            return True
        pbuf = self._lastreply.get_protobuf()
        if isinstance(pbuf, cpb.Error):
            return True
        return False

    def _remove_cmd2_stringstart(self, a):
        if isinstance(a, basestring):
            return a.strip('\"')
        else:
            return a

    def _send_request(self, msg, bad_data=False):
        if self._session is not None:
            msg._header.session = self._session
        if self._sock is not None:
            msg.do_send(self._sock, bad_data)
            dbg('Sent Request message:')
            msg.do_print()

    def _get_reply(self):  # , msg):
        msg = SciDBMessage()
        if self._sock is not None:
            msg.receive_message(self._sock)
            dbg('Received Response message:')
            msg.do_print()
            self._set_session(msg._header.session)
        else:
            print("SOCKET is dead")
        return msg

    def _send_request_and_get_reply(self, msg):
        self._send_request(msg)
        return self._get_reply()

    def _send_bad_request_and_get_reply(self, msg):
        self._send_request(msg, True)
        return self._get_reply()


def parse_args(cli_args):
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', default=0, action='count',
                        help='If specified, progress of the algorithm will be reported.')

    parser.add_argument('-H', '-c', '--host',
                        help='Host in which to connect.',
                        default='')
    parser.add_argument('-p', '--port',
                        help='Listening Port on <host>',
                        type=int,
                        default=5239)

    parser.add_argument('-f', '--format',
                        help='format',
                        default='dcsv')

    # Set ~/.config/scidb/iquery.auth be the default Authentication file, if it
    # exists.
    auth_file = None
    try:
        from xdg.BaseDirectory import xdg_config_home
        auth_file = os.path.join(xdg_config_home, 'scidb', 'iquery.auth')
    except ImportError:
        auth_file = os.path.join(os.environ['HOME'], '.iquery.auth')
    if not os.path.isfile(auth_file):
        auth_file = None

    parser.add_argument('-A', '--auth-file',
                        help='File (INI format) containing authentication credentials',
                        # type=argparse.FileType('r'),
                        # Unfortunately argparse.FileType returns an open file handle, but
                        # doesn't close it. Additionally, it may be necessary to reopen it
                        # if re-authentication is necessary.
                        default=auth_file,
                        required=False)

    default_prompt = '%AFL '
    if not os.isatty(sys.stdin.fileno()):
        default_prompt = ''
    parser.add_argument('-P', '--prompt',
                        help='starting prompt',
                        default=default_prompt)
    args = parser.parse_args(cli_args)
    return args


def main(argv=None):
    try:
        if argv is None:
            argv = sys.argv

        _args = parse_args(argv[1:])

        scidblib.SciDBMessage._verbose = _args.verbose

        host = socket.gethostbyname(_args.host)
        port = _args.port
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        is_ssl = False
        iquery = None
        if is_ssl:
            dbg("ServerCert:\n    {0}".format(ssl.get_server_certificate((host, port))))
            # sock.connect((host, port))
            wrapped_sock = ssl.wrap_socket(sock,
                                           ssl_version=ssl.PROTOCOL_SSLv23,
                                           ca_certs='server.crt',
                                           cert_reqs=ssl.CERT_REQUIRED)
            dbg(repr(wrapped_sock))
            wrapped_sock.connect((host, port))
            iquery = Iquery(socket=wrapped_sock,
                            prompt=_args.prompt,
                            format=_args.format,
                            cliargs=_args)
        else:
            sock.connect((host, port))
            iquery = Iquery(socket=sock, prompt=_args.prompt, format=_args.format, cliargs=_args)
        if iquery is not None:
            iquery.cmdloop()
    except socket.error as e:
        print("Connection error {0}".format(e))
        return 1
    except KeyboardInterrupt:
        pass
    except BaseException as e:
        print('Exception: {0}'.format(e))
        if scidblib.SciDBMessage._verbose:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback)
        # TODO This should return non-zero, but this file, SciDBMessage.py and
        #      the Ccm tests need to be reworked.


if __name__ == '__main__':
    sys.exit(main())
