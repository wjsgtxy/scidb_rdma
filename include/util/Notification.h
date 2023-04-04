/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2019 SciDB, Inc.
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
 * @file Notification.h
 *
 * @brief A publish-subscribe mechanism for receiving arbitrary messages
 *
 */

#ifndef NOTIFICATION_H_
#define NOTIFICATION_H_

#include <memory>

#include <log4cxx/logger.h>

#include <system/Utils.h> // For SCIDB_ASSERT
#include <util/Utility.h> // For demangle_cpp
#include <util/Mutex.h>

namespace scidb {
/**
 * @class Notification
 *
 * @brief A publish-subscribe mechanism for receiving arbitrary messages
 *
 * The Notification publish-subscribe mechanism provides a means to register a functor
 * (subscriber) to be executed at a later time when publish is called on the given
 * @c MessageType.
 *
 * When an object of interest of the given @c MessageType is "published," registered
 * functors will be called with a shared_ptr to the object passed as a parameter. The
 * functors are executed in the same thread as the caller to publish().
 *
 * Additionally, a callback (SubscribeListener) can be registered (separately from a
 * subscriber) that will execute every time a new subscriber functor is registered. The
 * SubscribeListener callback is a functor taking no arguments. These listener callbacks
 * are executed in the same thread where each subscribe() is called. Note, however, that
 * no functor is executed when a subscriber unsubscribes.
 *
 * @note
 *    No locks/mutexes can be held when invoking any of the methods of Notification.
 *
 * For Example:
 *
 * Subscriber:
 * @code{.cpp}
 *   using namespace std;
 *   class TestSubscriber : public enable_shared_from_this<TestSubscriber>
 *   {
 *   private:
 *      SubscriberID _id;
 *   public:
 *     void receiverFunctor(shared_ptr<SomeMessage> msg)
 *     {
 *       // A callback that executes based upon aspects of the msg.
 *     }
 *     void registerForLaterWork()
 *     {
 *       ...
 *       if (!_id) {
 *         _id = Notification<SomeMessage>::subscribe(
 *             bind(&TestSubscriber::receiverFunctor, shared_from_this(), placeholders::_1));
 *       }
 *       ...
 *     }
 *     ~TestSubscriber()
 *     {
 *       if (_id) {
 *         // remove the callback from the SubscriberMap
 *         Notification<SomeMessage>::unsubscribe(_id);
 *        }
 *     }
 *   };
 * @endcode
 *
 * Publisher:
 * @code{.cpp}
 *   using namespace std;
 *   shared_ptr<SomeMessage> msg = std::make_shared<SomeMessage>()
 *   // ... setup msg information
 *   Notification<SomeMessage> event(msg);
 *   event.publish();
 *   // NOTE: At this point, receiverFunctor(msg) will be called
 *   //       for each instance of TestSubscriber which has
 *   //       called registerForLaterWork().
 * @endcode
 *
 *
 * Subscription listener:
 * @code{.cpp}
 *   void getBusy()
 *   {
 *      // Some action to take when a new subscriber is added.
 *   }
 *
 *   SubscriberID id_;
 *   id_ = Notification<SomeMessage>::addSubscribeListener(&getBusy);
 *   ...
 *
 *   // Each time an instance of TestSubscriber makes an initial call to registerForLaterWork()
 *   // getBusy() will execute in that thread (not here!).
 *   ...
 *
 *   // Stop getBusy() from being executed in the subscriber's thread when the
 *   // subscriber to SomeMessage is registered.
 *   Notification<SomeMessage>::removeSubscribeListener(id_);
 *   ...
 * @endcode
 */
template <typename MessageType>
class Notification
{
public:
    /// Message pointer type
    typedef std::shared_ptr<const MessageType> MessageTypePtr;

    /// A functor to execute on published messages
    typedef std::function<void(MessageTypePtr)> Subscriber;

    /// A listener functor for "subscribe" events
    typedef std::function<void()> SubscribeListener;

    /// A registered subscriber id. The value of 0 corresponds to 'unsubscribed'.
    typedef uint64_t SubscriberID;

    /**
     * Notification constructor
     * @param msgPtr payload message pointer
     */
    Notification(const MessageTypePtr& msgPtr)
        : _msgPtr(msgPtr)
    {}

    /// Destructor
    virtual ~Notification() {}

    /**
     * Execute all the currently subscribed functors for this @c MessageType.
     *
     * @note
     *   All of the functors in the SubscriberMap are executed in the current thread.
     */
    void publish()
    {
        LOG4CXX_TRACE(_logger,
                      "Publishing [MessageType='" << _msgTypeName << "'] : " << _subscribers.size()
                                                  << " subscribers");
        notifyOnPublish(_msgPtr);
    }

    /**
     * Register a Subscriber functor to the SubscriberMap which will be executed when
     * publish() is called on the given @c MessageType.
     *
     * @note
     *    Subscribing will cause all registered SubscriberListener functors for
     *    the @c MessageType to execute at this point in the current thread.
     *
     * @param lsnr The Subscriber functor added to the SubscriberMap
     * @return key in the SubscriberMap for the @clsnr functor
     */
    static SubscriberID subscribe(Subscriber& lsnr) __attribute__((warn_unused_result))
    {
        assert(lsnr);
        SubscriberID id = 0;
        {
            ScopedMutexLock lock(_mutex, PTW_SML_NOTI);
            id = ++_nextSubscriberID;
            if (0 == id) {
                LOG4CXX_DEBUG(_logger,
                              "SubscriberIDs [" << _msgTypeName << "] have wrapped past 2^64");
                id = ++_nextSubscriberID;
            }
            auto ret = _subscribers.insert({id, lsnr});
            while (!ret.second || (0 == id)) {
                if (0 == id) {
                    LOG4CXX_WARN(_logger,
                                 "SubscriberIDs [" << _msgTypeName
                                                   << "] have wrapped past 2^64. Is map full?");
                }
                id = ++_nextSubscriberID;
                ret = _subscribers.insert({id, lsnr});
            }
            SCIDB_ASSERT(id == _nextSubscriberID);
        }
        LOG4CXX_TRACE(_logger,
                      "Subscribe [" << _msgTypeName << "] "
                                    << "(id = " << id << ")");

        notifyOnSubscribe();
        return id;
    }

    /**
     * Remove the Subscriber functor from the SubscriberMap assoicated with the provided
     * SubscriberID.
     *
     *
     * @param id SubscriberID key in the SubscriberMap
     *        @note
     *          The passed id is set to 0 as a result of unsubscribing.
     * @return true iff an entry was removed from the SubscriberMap.
     */
    static bool unsubscribe(SubscriberID& id)
    {
        if (0 == id) {
            // An ID of 0 implies the id was never assigned by a subscribe.
            LOG4CXX_WARN(_logger,
                         "Attempting to unsubscribe [" << _msgTypeName
                                                       << "]  no-subscriber sentinel (id = 0)")
            return false;
        }
        ScopedMutexLock lock(_mutex, PTW_SML_NOTI);
        auto search = _subscribers.find(id);
        bool found = (search != _subscribers.end());
        if (found) {
            LOG4CXX_TRACE(_logger,
                          "Unsubscribe [" << _msgTypeName << "]"
                                          << "(id = " << id << ")");
            _subscribers.erase(search);
        } else {
            LOG4CXX_TRACE(_logger,
                          "Unsubscribe [" << _msgTypeName << "]"
                                          << "(id = " << id << ") : Non-existent");
        }
        id = 0;
        return found;
    }

    /**
     * Add a "listener" functor that will be executed when any new subscribe functors are
     * added.
     *
     * @param lsnr the functor to execute
     *
     * @return the key in the SubscriberMap for the new registered SubscribeListener
     *         functor.
     */
    static SubscriberID addSubscribeListener(SubscribeListener& lsnr)
        __attribute__((warn_unused_result))
    {
        assert(lsnr);
        SubscriberID id = 0;
        {
            ScopedMutexLock lock(_mutex, PTW_SML_NOTI);
            id = ++_nextListenerID;
            if (0 == id) {
                LOG4CXX_DEBUG(_logger,
                              "Subscriber Listener IDs [" << _msgTypeName
                                                          << "] have wrapped past 2^64");
                id = ++_nextListenerID;
            }
            auto ret = _subscribeListeners.insert({id, lsnr});
            while (!ret.second || (0 == id)) {
                if (0 == id) {
                    LOG4CXX_WARN(_logger,
                                 "Subscriber Listener IDs ["
                                     << _msgTypeName << "] have wrapped past 2^64. Is map full?");
                }
                id = ++_nextListenerID;
                ret = _subscribeListeners.insert({id, lsnr});
            }
            SCIDB_ASSERT(id == _nextListenerID);
            LOG4CXX_TRACE(_logger,
                          "Adding Subscribe listener [" << _msgTypeName << "] "
                                                        << "(id = " << id << ")");
        }
        return id;
    }

    /**
     * Remove the subscription "listener".
     *
     * @param id SubscriberID of the subscription listener
     *        @note
     *          The passed id is set to 0 as a result of removing the listener.
     *
     * @return true iff the entry with given @c id existed and was successfully removed
     *         from the SubscriberListener map.
     */
    static bool removeSubscribeListener(SubscriberID& id)
    {
        if (0 == id) {
            // An ID of 0 implies the id was never assigned by a subscribe.
            LOG4CXX_WARN(_logger,
                         "Attempting to remove SubscriberListener ["
                             << _msgTypeName << "] with no-subscriber sentinel (id = 0)")
            return false;
        }
        ScopedMutexLock lock(_mutex, PTW_SML_NOTI);
        auto search = _subscribeListeners.find(id);
        bool found = (search != _subscribeListeners.end());
        if (found) {
            LOG4CXX_TRACE(_logger,
                          "Removing Subscribe listener [" << _msgTypeName << "]"
                                                          << "(id = " << id << ")");
            _subscribeListeners.erase(search);
        } else {
            LOG4CXX_TRACE(_logger,
                          "Removing Subscribe listener [" << _msgTypeName << "]"
                                                          << "(id = " << id << ") : Non-existent");
        }
        id = 0;
        return found;
    }

private:
    static void notifyOnPublish(const MessageTypePtr msgPtr)
    {
        SubscriberMap tmpSubscriberMap;
        {
            ScopedMutexLock lock(_mutex, PTW_SML_NOTI);
            tmpSubscriberMap.insert(_subscribers.begin(), _subscribers.end());
        }
        for (auto& _subscriberPair : tmpSubscriberMap) {
            LOG4CXX_TRACE(_logger, "  Notifying (id=" << _subscriberPair.first << ")");
            _subscriberPair.second(msgPtr);
        }
    }

    static void notifyOnSubscribe()
    {
        SubscribeListenerMap tmpListeners;
        {
            ScopedMutexLock lock(_mutex, PTW_SML_NOTI);
            tmpListeners.insert(_subscribeListeners.begin(), _subscribeListeners.end());
        }
        for (auto& listenerPair : tmpListeners) {
            listenerPair.second();
        }
    }

    MessageTypePtr _msgPtr;

    static Mutex _mutex;

    typedef std::map<SubscriberID, Subscriber> SubscriberMap;
    static SubscriberMap _subscribers;

    typedef std::map<SubscriberID, SubscribeListener> SubscribeListenerMap;
    static SubscribeListenerMap _subscribeListeners;

    static uint64_t _nextSubscriberID;
    static uint64_t _nextListenerID;
    static log4cxx::LoggerPtr _logger;
    static const std::string _msgTypeName;
};

/* Ugh, there should *never* be any linkage in header files. */
#ifndef NO_NOTIFICATION_LINKAGE

template <typename MessageType>
Mutex Notification<MessageType>::_mutex;

template <typename MessageType>
typename Notification<MessageType>::SubscriberMap Notification<MessageType>::_subscribers;

template <typename MessageType>
typename Notification<MessageType>::SubscribeListenerMap
    Notification<MessageType>::_subscribeListeners;

template <typename MessageType>
uint64_t Notification<MessageType>::_nextSubscriberID = 0;

template <typename MessageType>
uint64_t Notification<MessageType>::_nextListenerID = 0;

template <typename MessageType>
log4cxx::LoggerPtr Notification<MessageType>::_logger =
    log4cxx::Logger::getLogger("scidb.Notification." + demangle_cpp(typeid(MessageType).name()));

// TODO: This is used only in logging. The logger name also uses this string.
// Unfortunately, the ConversionPattern in the log4cxx.properties file does not contain
// logger name (know as a Category and specified via "%c" in the properties file).
template <typename MessageType>
const std::string
    Notification<MessageType>::_msgTypeName = demangle_cpp(typeid(MessageType).name());

#endif /* ! NO_NOTIFICATION_LINKAGE */

} // namespace scidb

#endif
