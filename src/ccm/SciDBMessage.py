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
from __future__ import print_function
from scidblib import Ccm_pb2 as cpb
import struct
import uuid
import time
import sys

_verbose = 0


def dbg(*args):
    if _verbose > 0:
        print("DBG: {0}".format(' '.join(str(x) for x in args)), file=sys.stderr)


class MessageTypes(object):
    # General Message Processing
    #
    Error = 0
    #
    # Authentication
    #
    AuthLogon = 1
    AuthChallenge = 2
    AuthResponse = 3
    AuthComplete = 4
    #
    # Query Execution
    #
    ExecuteQuery = 5
    ExecuteQueryResponse = 6
    FetchIngot = 7
    FetchIngotResponse = 8


# Map message type to callable message object ctor
messageLookup = {
    MessageTypes.Error: cpb.Error,
    MessageTypes.AuthLogon: cpb.AuthLogon,
    MessageTypes.AuthChallenge: cpb.AuthChallenge,
    MessageTypes.AuthResponse: cpb.AuthResponse,
    MessageTypes.AuthComplete: cpb.AuthComplete,
    MessageTypes.ExecuteQuery: cpb.ExecuteQuery,
    MessageTypes.ExecuteQueryResponse: cpb.ExecuteQueryResponse,
    MessageTypes.FetchIngot: cpb.FetchIngot,
    MessageTypes.FetchIngotResponse: cpb.FetchIngotResponse,
}


class MessageHeader(object):
    # 0         8        16        24        32
    # +=========+=========+=========+=========+
    # | version | msgCode | reserved          |
    # +---------+---------+---------+---------+
    # |    session                            |
    # |          universally                  |
    # |                    unique             |
    # |                         identifier    |
    # +---------+---------+---------+---------+
    # |       protoBuf body length            |
    # +---------+---------+---------+---------+

    @classmethod
    def size(cls):
        return struct.calcsize('!BBh16cI')

    def __init__(self,
                 version=1,
                 msg_type=MessageTypes.Error,
                 session=None,
                 body_size=0):
        self.version = version
        self.msg_type = msg_type
        if session is None:
            session = uuid.UUID(int=0)
        self.session = session
        self.body_size = body_size
        self.r1 = 0x0
        self.r2 = 0x0

    def __repr__(self):
        return '{}({!r}, {!r}, {!r}, {!r})'.format(
            self.__class__.__name__,
            self.version,
            self.msg_type,
            self.session,
            self.body_size)

    def __eq__(self, other):
        # Overrides the default implementation
        if isinstance(other, MessageHeader):
            return (self.version == other.version and
                    self.msg_type == other.msg_type and
                    self.session == other.session and
                    self.body_size == other.body_size)

        return NotImplemented

    def __ne__(self, other):
        # Overrides the default implementation
        x = self.__eq__(other)
        return x if x is NotImplemented else not x

    def __hash__(self):
        """Overrides the default implementation"""
        return hash(tuple(sorted(self.__dict__.items())))

    def unpack(self, vals):
        b1 = vals[0:1]
        b2 = vals[1:2]
        # reserved = vals[2:4]
        ses = vals[4:20]
        i = vals[20:25]
        (self.version,) = struct.unpack('!B', b1)
        (self.msg_type,) = struct.unpack('!B', b2)
        self.session = uuid.UUID(bytes=ses)
        (self.body_size,) = struct.unpack('!I', i)

    def pack(self):
        buffer = struct.pack('!BB', self.version, self.msg_type)
        buffer += struct.pack('!BB', self.r1, self.r2)
        buffer += struct.pack('!16c', *[c for c in self.session.bytes])
        buffer += struct.pack('!I', self.body_size)
        return buffer

    def do_print(self):
        dbg("""    MessageHeader:
    version:    {0}
    type:       {1}
        res0:       {4}
        res1:       {5}
    session:    {2}
    body_size:   {3}""".format(self.version,
                               self.msg_type,
                               self.session,
                               self.body_size,
                               self.r1,
                               self.r2))


class SciDBMessage(object):
    # 0         8        16        24        32
    # +=========+=========+=========+=========+
    # |                                       |
    # +-                                     -+
    # |                                       |
    # |                                       |
    # |             MsgHeader                 |
    # |                                       |
    # +-                                     -+
    # |                                       |
    # +=======================================+
    # |                                       |
    # .          protoBuf body                .
    # .               ...                     .
    # +-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-+
    # |  <Optional Data needed by Protobuf>   |
    # +---------------------------------------+
    #

    def __init__(self, msg_type=MessageTypes.Error, version=1):
        self._header = MessageHeader(msg_type=msg_type, version=version)
        msg = messageLookup[msg_type]
        self._body = msg()
        self._recv_data = b''

    def pack_too_little(self):
        """Pack a payload that is actually smaller than what is specified by the header
        size and size of the protobuf to be sent. This is a testing function to test
        the read time-out in the CCM.

        To do this set the record size in the header to be one byte greater than the
        protobuf actually is.

        Make certain to use a send/receive lock step processing (that is just
        send ONE message and then read the response) otherwise the CCM will
        read the first byte from the second message which results in a
        different error case. (Namely that the data for protobuf is incorrect
        -- read n+1 bytes and try and have protobuf parse the valid message and
        the extra one byte.)

        """
        self._header.record = self._body.ByteSize() + 1
        return b'{0}{1}'.format(self._header.pack(),
                                self._body.SerializeToString())[:-1]

    def pack_invalid_protobuf(self):
        """Pack a payload that contains an VALID protobuf message that is of a
DIFFERENT and invalid Request message type than is specified in the message header"""
        if self.msg_type != MessageTypes.Error:
            # Error is not a valid request to be sent to Server, so set the
            # type to Error
            self.msg_type = MessageTypes.Error
        else:
            # Well, Error is an invalid Request, but change contents of the
            # protobuf will be correct so change the type to some other invalid
            # message type.
            self.msg_type = MessageTypes.AuthComplete
        return self.pack()

    def pack(self):
        self._header.body_size = self._body.ByteSize()
        return b'{0}{1}'.format(
            self._header.pack(),
            self._body.SerializeToString())

    def bad_pack(self):
        bad_size = 40
        self._header.body_size = bad_size
        return b'{0}{1}'.format(
            self._header.pack(),
            '\1' * bad_size)

    def get_protobuf(self):
        return self._body

    def get_msg_type(self):
        return self._header.msg_type

    def do_print(self):
        dbg('SciDBMessage')
        self._header.do_print()
        if self._body is not None:
            dbg('    =============')
            dbg(self._body)

    def send_with_delay(self, sock, delay=3, initial_amount=20):
        """ Send a first 'initial_amount' bytes of data, wait 'delay' seconds
and then send remaining data.

If the initial_amount is the >= the full message size, then send half the
total data as the two hunks.

Each hunk of data is sent using sock.sendall"""
        data = self.pack()
        if initial_amount < 0 or len(data) < initial_amount:
            initial_amount = initial_amount / 2
        dbg('Sending msg_type {0}: first {1} bytes of {2} then sleeping {3} seconds'.format(
            self._header.msg_type,
            initial_amount,
            len(data),
            delay))
        sock.sendall(data[0:initial_amount])
        time.sleep(delay)
        dbg('Sending remaining {0} bytes of msg_type {1}'.format(
            len(data) - initial_amount,
            self._header.msg_type))
        sock.sendall(data[initial_amount:])

    def do_send(self, sock, bad=False):
        if bad:
            data = self.bad_pack()
        else:
            data = self.pack()
        dbg('Sending msg_type of {0} totaling {1} bytes to scidb'.format(
            self._header.msg_type,
            len(data)))
        sock.sendall(data)

    def _receive_header(self, sock):
        RECV_SIZE = 4096

        self._header = None
        if self._recv_data:
            print("WARNING: (TODO) we still have some _recv_data from last read")
            self._recv_data = b''
        header_string = self._recv_data
        # excess_data = b''
        total_received = 0
        while (len(header_string) < MessageHeader.size()):
            data = sock.recv(RECV_SIZE)
            if len(data) == 0:
                return
            total_received += len(data)
            if (total_received <= MessageHeader.size()):
                header_string += data
            else:
                idx = MessageHeader.size() - len(header_string)
                header_string = header_string + data[0:idx]
                self._recv_data += data[idx:]
        if len(header_string) == MessageHeader.size():
            self._header = MessageHeader()
            self._header.unpack(header_string)
        # else:
        #     raise Exception('Unable to read header')

    def receive_message(self, sock):
        RECV_SIZE = 4096
        self._receive_header(sock)
        protomsg_received = self._recv_data
        if protomsg_received:
            dbg('Had some protomsg_received data: {0}'.format(protomsg_received))
        self._recv_data = b''
        if self._header is None:
            print('We could not read the header')
            return
        # assert self._header is not None, 'We could not read the header'
        while (len(protomsg_received) < self._header.body_size):
            data = sock.recv(RECV_SIZE)
            protomsg_received += data

        if len(protomsg_received) > self._header.body_size:
            dbg("extra data received..TBD1")
            self._recv_data = protomsg_received[self._header.body_size:]
            protomsg_received = protomsg_received[0:self._header.body_size]
        protobuf = messageLookup[self._header.msg_type]
        self._body = protobuf()
        dbg("length of protobuf {0}, (expected:  {1})\n{2}".format(
            len(protomsg_received),
            self._header.body_size,
            protomsg_received))
        self._body.ParseFromString(protomsg_received)
        if self._recv_data:
            dbg('excess is (len = {0}):'.format(len(self._recv_data)))
            print('{0}'.format(self._recv_data))
            # TODO: we need to deal with this, but right now let's just reset it
