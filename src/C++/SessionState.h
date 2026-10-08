/* -*- C++ -*- */

/****************************************************************************
** Copyright (c) 2001-2014
**
** This file is part of the QuickFIX FIX Engine
**
** This file may be distributed under the terms of the quickfixengine.org
** license as defined by quickfixengine.org and appearing in the file
** LICENSE included in the packaging of this file.
**
** This file is provided AS IS with NO WARRANTY OF ANY KIND, INCLUDING THE
** WARRANTY OF DESIGN, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE.
**
** See http://www.quickfixengine.org/LICENSE for licensing information.
**
** Contact ask@quickfixengine.org if any conditions of this licensing are
** not clear to you.
**
****************************************************************************/

#ifndef FIX_SESSIONSTATE_H
#define FIX_SESSIONSTATE_H

#ifdef _MSC_VER
#pragma warning(disable : 4503 4355 4786 4290)
#endif

#include "FieldTypes.h"
#include "Log.h"
#include "MessageStore.h"
#include "Mutex.h"
#include <atomic>
#include <limits>
#include <mutex>

namespace FIX {
/// Maintains all of state for the Session class.
class SessionState : public MessageStore, public Log {
  typedef std::map<SEQNUM, Message> Messages;

public:
  SessionState(const UtcTimeStamp &now)
      : m_enabled(true),
        m_receivedLogon(false),
        m_sentLogout(false),
        m_sentLogon(false),
        m_sentReset(false),
        m_receivedReset(false),
        m_initiate(false),
        m_logonTimeout(10),
        m_logoutTimeout(2),
        m_testRequest(0),
        m_lastSentDate(now.m_date),
        m_lastSentNanos(now.m_time),
        m_lastReceivedTime(now),
        m_pStore(0),
        m_pLog(0) {}

  bool enabled() const { return m_enabled; }
  void enabled(bool value) { m_enabled = value; }

  bool receivedLogon() const { return m_receivedLogon; }
  void receivedLogon(bool value) { m_receivedLogon = value; }

  bool sentLogout() const { return m_sentLogout; }
  void sentLogout(bool value) { m_sentLogout = value; }

  bool sentLogon() const { return m_sentLogon; }
  void sentLogon(bool value) { m_sentLogon = value; }

  bool receivedReset() const { return m_receivedReset; }
  void receivedReset(bool value) { m_receivedReset = value; }

  bool sentReset() const { return m_sentReset; }
  void sentReset(bool value) { m_sentReset = value; }

  bool initiate() const { return m_initiate; }
  void initiate(bool value) { m_initiate = value; }

  int logonTimeout() const { return m_logonTimeout; }
  void logonTimeout(int value) { m_logonTimeout = value; }

  int logoutTimeout() const { return m_logoutTimeout; }
  void logoutTimeout(int value) { m_logoutTimeout = value; }

  int testRequest() const { return m_testRequest; }
  void testRequest(int value) { m_testRequest = value; }

  bool resendRequested() const { return !(m_resendRange.first == 0 && m_resendRange.second == 0); }

  typedef std::pair<SEQNUM, SEQNUM> ResendRange;

  ResendRange resendRange() const { return m_resendRange; }
  void resendRange(SEQNUM begin, SEQNUM end) { m_resendRange = std::make_pair(begin, end); }

  MessageStore *store() { return m_pStore; }
  void store(MessageStore *pValue) { m_pStore = pValue; }
  Log *log() { return m_pLog ? m_pLog : &m_nullLog; }
  void log(Log *pValue) { m_pLog = pValue; }

  void heartBtInt(const HeartBtInt &value) { m_heartBtInt = value; }
  HeartBtInt &heartBtInt() { return m_heartBtInt; }
  const HeartBtInt &heartBtInt() const { return m_heartBtInt; }

  void lastSentTime(const UtcTimeStamp &value) {
    std::lock_guard<std::mutex> lock(m_lastSentTimeMutex);
    const auto version = m_lastSentVersion.load(std::memory_order_relaxed);
    const auto exhausted = std::numeric_limits<uint64_t>::max();
    if (version != exhausted) {
      m_lastSentVersion.store(version + 1, std::memory_order_relaxed);
    }
    // Observing either new field makes the reader's acquire fence observe the odd version.
    std::atomic_thread_fence(std::memory_order_release);
    m_lastSentDate.store(value.m_date, std::memory_order_relaxed);
    m_lastSentNanos.store(value.m_time, std::memory_order_relaxed);
    // Saturation permanently selects the mutex path instead of reusing a version.
    if (version < exhausted - 1) {
      m_lastSentVersion.store(version + 2, std::memory_order_release);
    }
  }
  /// Returns an independent snapshot of the last sent timestamp.
  UtcTimeStamp lastSentTime() { return static_cast<const SessionState &>(*this).lastSentTime(); }
  /// Returns an independent snapshot of the last sent timestamp.
  UtcTimeStamp lastSentTime() const {
    const auto version = m_lastSentVersion.load(std::memory_order_acquire);
    if (!(version & 1)) {
      const auto date = m_lastSentDate.load(std::memory_order_relaxed);
      const auto nanos = m_lastSentNanos.load(std::memory_order_relaxed);
      std::atomic_thread_fence(std::memory_order_acquire);
      if (version == m_lastSentVersion.load(std::memory_order_relaxed)) {
        return UtcTimeStamp(DateTime(date, nanos));
      }
    }
    std::lock_guard<std::mutex> lock(m_lastSentTimeMutex);
    return UtcTimeStamp(
        DateTime(m_lastSentDate.load(std::memory_order_relaxed), m_lastSentNanos.load(std::memory_order_relaxed)));
  }

  void lastReceivedTime(const UtcTimeStamp &value) { m_lastReceivedTime = value; }
  UtcTimeStamp &lastReceivedTime() { return m_lastReceivedTime; }
  const UtcTimeStamp &lastReceivedTime() const { return m_lastReceivedTime; }

  bool shouldSendLogon() const { return initiate() && !sentLogon(); }
  bool alreadySentLogon() const { return initiate() && sentLogon(); }
  bool logonTimedOut(const UtcTimeStamp &now) const { return now - lastReceivedTime() >= logonTimeout(); }
  bool logoutTimedOut(const UtcTimeStamp &now) const {
    return sentLogout() && ((now - lastSentTime()) >= logoutTimeout());
  }
  bool withinHeartBeat(const UtcTimeStamp &now) const {
    return ((now - lastSentTime()) < heartBtInt()) && ((now - lastReceivedTime()) < heartBtInt());
  }
  bool timedOut(const UtcTimeStamp &now) const { return (now - lastReceivedTime()) >= (2.4 * (double)heartBtInt()); }
  bool needHeartbeat(const UtcTimeStamp &now) const {
    return ((now - lastSentTime()) >= heartBtInt()) && !testRequest();
  }
  bool needTestRequest(const UtcTimeStamp &now) const {
    return (now - lastReceivedTime()) >= ((1.2 * ((double)testRequest() + 1)) * (double)heartBtInt());
  }

  std::string logoutReason() const {
    Locker l(m_mutex);
    return m_logoutReason;
  }
  void logoutReason(const std::string &value) {
    Locker l(m_mutex);
    m_logoutReason = value;
  }

  void queue(SEQNUM msgSeqNum, const Message &message) {
    Locker l(m_mutex);
    m_queue[msgSeqNum] = message;
  }
  bool retrieve(SEQNUM msgSeqNum, Message &message) {
    Locker l(m_mutex);
    Messages::iterator i = m_queue.find(msgSeqNum);
    if (i != m_queue.end()) {
      message = i->second;
      m_queue.erase(i);
      return true;
    }
    return false;
  }
  void clearQueue() {
    Locker l(m_mutex);
    m_queue.clear();
  }
  void clearQueueUpTo(SEQNUM msgSeqNum) {
    Locker l(m_mutex);
    m_queue.erase(m_queue.begin(), m_queue.lower_bound(msgSeqNum));
  }

  bool set(SEQNUM s, const std::string &m) EXCEPT(IOException) {
    Locker l(m_mutex);
    return m_pStore->set(s, m);
  }
  void get(SEQNUM b, SEQNUM e, std::vector<std::string> &m) const EXCEPT(IOException) {
    Locker l(m_mutex);
    m_pStore->get(b, e, m);
  }
  SEQNUM getNextSenderMsgSeqNum() const EXCEPT(IOException) {
    Locker l(m_mutex);
    return m_pStore->getNextSenderMsgSeqNum();
  }
  SEQNUM getNextTargetMsgSeqNum() const EXCEPT(IOException) {
    Locker l(m_mutex);
    return m_pStore->getNextTargetMsgSeqNum();
  }
  void setNextSenderMsgSeqNum(SEQNUM n) EXCEPT(IOException) {
    Locker l(m_mutex);
    m_pStore->setNextSenderMsgSeqNum(n);
  }
  void setNextTargetMsgSeqNum(SEQNUM n) EXCEPT(IOException) {
    Locker l(m_mutex);
    m_pStore->setNextTargetMsgSeqNum(n);
  }
  void incrNextSenderMsgSeqNum() EXCEPT(IOException) {
    Locker l(m_mutex);
    m_pStore->incrNextSenderMsgSeqNum();
  }
  void incrNextTargetMsgSeqNum() EXCEPT(IOException) {
    Locker l(m_mutex);
    m_pStore->incrNextTargetMsgSeqNum();
  }
  UtcTimeStamp getCreationTime() const EXCEPT(IOException) {
    Locker l(m_mutex);
    return m_pStore->getCreationTime();
  }
  void reset(const UtcTimeStamp &now) EXCEPT(IOException) {
    Locker l(m_mutex);
    m_pStore->reset(now);
  }
  void refresh() EXCEPT(IOException) {
    Locker l(m_mutex);
    m_pStore->refresh();
  }

  void clear() {
    if (!m_pLog) {
      return;
    }
    Locker l(m_mutex);
    m_pLog->clear();
  }
  void backup() {
    if (!m_pLog) {
      return;
    }
    Locker l(m_mutex);
    m_pLog->backup();
  }
  void onIncoming(const std::string &string) {
    if (!m_pLog) {
      return;
    }
    Locker l(m_mutex);
    m_pLog->onIncoming(string);
  }
  void onOutgoing(const std::string &string) {
    if (!m_pLog) {
      return;
    }
    Locker l(m_mutex);
    m_pLog->onOutgoing(string);
  }
  void onEvent(const std::string &string) {
    if (!m_pLog) {
      return;
    }
    Locker l(m_mutex);
    m_pLog->onEvent(string);
  }

private:
  friend struct SessionStateTestAccess;

  bool m_enabled;
  bool m_receivedLogon;
  bool m_sentLogout;
  bool m_sentLogon;
  bool m_sentReset;
  bool m_receivedReset;
  bool m_initiate;
  int m_logonTimeout;
  int m_logoutTimeout;
  int m_testRequest;
  ResendRange m_resendRange;
  HeartBtInt m_heartBtInt;
  std::atomic<int> m_lastSentDate;
  std::atomic<int64_t> m_lastSentNanos;
  std::atomic<uint64_t> m_lastSentVersion{0};
  UtcTimeStamp m_lastReceivedTime;
  std::string m_logoutReason;
  Messages m_queue;
  MessageStore *m_pStore;
  Log *m_pLog;
  NullLog m_nullLog;
  mutable Mutex m_mutex;
  mutable std::mutex m_lastSentTimeMutex;
};
} // namespace FIX

#endif // FIX_SESSIONSTATE_H
