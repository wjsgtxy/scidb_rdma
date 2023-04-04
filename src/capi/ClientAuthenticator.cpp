/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2017-2019 SciDB, Inc.
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

/**
 * @file ClientAuthenticator.cpp
 *
 * @note Trying to stay flexible here: right now different auth
 * methods are handled via a switch statement, but I expect things
 * will get more complicated, so keeping the class hierarchy in place
 * for now.
 */

#include "ClientAuthenticator.h"

#include <network/BaseConnection.h>
#include <network/MessageDesc.h>
#include <network/MessageUtils.h>
#include <network/proto/scidb_msg.pb.h>
#include <rbac/SessionProperties.h>
#include <system/Auth.h>
#include <system/ErrorCodes.h>
#include <system/UserException.h>
#include <util/CryptoUtils.h>

#include <log4cxx/logger.h>

using namespace std;

namespace scidb {

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.services.network.auth"));

// ----------------------------------------------------------------------

struct MpiClientAuthenticator : public ClientAuthenticator
{
    MpiClientAuthenticator(SessionProperties const& props)
        : ClientAuthenticator(props)
    {}

    safebuf answerChallenge(int, string const&) override
    {
        // For now, it's the cluster UUID.
        // Make a copy so safebuf won't trash the _props token.
        safebuf result;
        vector<char> token(_props.getAuthToken().size() + 1);
        ::strncpy(&token[0], _props.getAuthToken().c_str(), token.size());
        result.grab(&token[0]);
        return result;
    }
};

// ----------------------------------------------------------------------

struct ExternalClientAuthenticator : public ClientAuthenticator
{
    ExternalClientAuthenticator(SessionProperties const& props)
        : ClientAuthenticator(props)
    { }

    safebuf answerChallenge(int, string const&) override;
};

safebuf ExternalClientAuthenticator::answerChallenge(int code, string const&)
{
    // NOTE: Do not use a safebuf to overwrite the _props auth token.
    // The scidbtestharness reuses it, so if it gets wiped reconnects
    // will fail.  Instead we'll make our own copy.

    stringstream ss;
    safebuf result;
    vector<char> token;

    // The code for the initial (and so far only) challenge indicates
    // which AuthMethod the server requires.

    switch (code) {
    case AUTH_TRUST:
        // Anything will do.
        result.grab(string("I seek the Holy Grail!"));
        break;
    case AUTH_RAW:
        // 16.9-style authentication.  No need to copy into token[]
        // since this produces a stack temporary.
        result.grab(crut::sha512_b64(_props.getAuthToken(/*withCb:*/true)));
        break;
    case AUTH_PAM:
        // PAM needs cleartext password, I hope you've set up TLS!
        // Make a copy so safebuf won't trash the _props token.
        token.resize(_props.getAuthToken(/*withCb:*/true).size() + 1);
        ::strncpy(&token[0], _props.getAuthToken().c_str(), token.size());
        result.grab(&token[0]);
        break;
    default:
        ss << "SciDB server insisted on unimplemented authentication method: "
           << code;
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_AUTHENTICATION_ERROR)
            << ss.str();
    }

    return result;
}

// ----------------------------------------------------------------------

ClientAuthenticator::ClientAuthenticator(SessionProperties const& props)
    : _props(props)
{
    LOG4CXX_TRACE(logger, "Begin client \""
                  << auth::strMethodTag(_props.getAuthMethod())
                  << "\" authentication for user \""
                  << _props.getUserName() << "\"");
}

ClientAuthenticator::~ClientAuthenticator()
{
    LOG4CXX_TRACE(logger, "Done client \""
                  << auth::strMethodTag(_props.getAuthMethod())
                  << "\" authentication for user \""
                  << _props.getUserName() << "\"");
}

ClientAuthenticator* ClientAuthenticator::create(SessionProperties const& props)
{
    switch (props.getAuthMethod()) {
    case AUTH_NONE:
    case AUTH_TRUST:
    case AUTH_RAW:
    case AUTH_PAM:
        // You'll use the method the server tells you to, and like it.
        return new ExternalClientAuthenticator(props);
    case AUTH_MPI:
        return new MpiClientAuthenticator(props);
    case AUTH_I2I:
        // Clients are not allowed to claim they are peers.
        throw USER_EXCEPTION(SCIDB_SE_AUTHENTICATION, SCIDB_LE_NOT_IMPLEMENTED)
            << auth::strMethodName(props.getAuthMethod());
    default:
        ASSERT_EXCEPTION_FALSE("Bad session property: authentication method");
    }
    SCIDB_UNREACHABLE();
}

/**
 * @brief Canned messaging for logon/challenge/response/complete handshake.
 *
 * @description I can't think of an auth method that would require a
 * different messaging pattern, but one might exist.  Inside the SciDB
 * server, a server-side Authenticator is allowed to issue many
 * challenge response pairs, so 6-, 8-, etc.-way handshakes are easy
 * to add.  But for now, 4-way is enough.
 *
 * @p Subclasses just need to implement an answerChallenge() method as
 * appropriate.
 */
void ClientAuthenticator::fourWayHandshake(BaseConnection* conn)
{
    LOG4CXX_TRACE(logger, "Login attempt for user=" << _props.getUserName()
                  << " with priority=" << _props.getPriority()
                  << ", auth=" << auth::strMethodTag(_props.getAuthMethod()));

    // Prepare AuthLogon message.
    MessageDescPtr logonMsg = make_shared<MessageDesc>(mtAuthLogon);
    auto authLogonRec = logonMsg->getRecord<scidb_msg::AuthLogon>();
    authLogonRec->set_username(_props.getUserName());
    authLogonRec->set_priority(_props.getPriority());
    authLogonRec->set_authtag(auth::strMethodTag(_props.getAuthMethod()));

    // Send AuthLogon, expect AuthChallenge.
    MessageDescPtr challenge = conn->sendAndReadMessage<MessageDesc>(logonMsg);
    if (challenge->getMessageType() != mtAuthChallenge) {
        ASSERT_EXCEPTION(challenge->getMessageType() == mtAuthError,
                         "Expected mtChallenge or mtAuthError but got neither");
        makeExceptionFromErrorMessageOnClient(challenge)->raise();
    }
    auto challengeRec = challenge->getRecord<scidb_msg::AuthChallenge>();

    LOG4CXX_TRACE(logger, "Challenge received: (" << challengeRec->code()
                  << ", " << challengeRec->text()
                  << "), cookie=" << challengeRec->cookie());

    // Answer the challenge by calling a subclass method.
    safebuf answer(answerChallenge(challengeRec->code(), challengeRec->text()));

    // Prepare and send AuthResponse, expect AuthComplete.
    MessageDescPtr respMsg = make_shared<MessageDesc>(mtAuthResponse);
    auto respRec = respMsg->getRecord<scidb_msg::AuthResponse>();
    respRec->set_cookie(challengeRec->cookie()); // MUST echo the cookie!
    respRec->set_text(answer.c_str());
    answer.wipe();

    // Send AuthResponse, expect AuthComplete.
    MessageDescPtr authDone = conn->sendAndReadMessage<MessageDesc>(respMsg);
    if (authDone->getMessageType() != mtAuthComplete) {
        ASSERT_EXCEPTION(authDone->getMessageType() == mtAuthError,
                         "Expected mtAuthDone or mtAuthError but got neither");
        makeExceptionFromErrorMessageOnClient(authDone)->raise();
    }

    // Are we good to go?
    auto doneRec = authDone->getRecord<scidb_msg::AuthComplete>();
    if (!doneRec->authenticated()) {
        throw USER_EXCEPTION(SCIDB_SE_AUTHENTICATION, SCIDB_LE_AUTHENTICATION_ERROR)
            << doneRec->reason();
    }
    if (doneRec->reason().empty()) {
        LOG4CXX_TRACE(logger, "User " << _props.getUserName() << " logged in");
    } else {
        LOG4CXX_INFO(logger, "User " << _props.getUserName()
                     << " logged in, message \"" << doneRec->reason() << '"');
        cerr << doneRec->reason() << endl;
    }
}

} // namespace
