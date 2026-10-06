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

#ifdef _MSC_VER
#pragma warning(disable : 4503 4355 4786)
#include "stdafx.h"
#else
#include "config.h"
#endif

#include <Log.h>
#include <SessionState.h>
#include <atomic>
#include <sstream>
#include <string>
#include <thread>
#include <type_traits>

#include "catch_amalgamated.hpp"

using namespace FIX;

TEST_CASE("SessionStateTests") {
  class TestLog : public Log {
  public:
    void clear() { events = 0; }
    void backup() { eventsBackup = events; }
    void onIncoming(const std::string &) {}
    void onOutgoing(const std::string &) {}
    void onEvent(const std::string &) {}

    int events = 0;
    int eventsBackup = 0;
  };

  SECTION("ClearSessionLog_StateLogNotNull_LogCleared") {
    SessionSettings settings;
    TestLog log;
    log.events = 5;

    SessionState state(UtcTimeStamp::now());
    state.log(&log);

    state.clear();

    CHECK(0 == log.events);
  }

  SECTION("clearSessionLog_StateLogIsNull_LogNotCleared") {
    SessionSettings settings;
    TestLog log;
    log.events = 5;

    SessionState state(UtcTimeStamp::now());

    state.clear();

    CHECK(5 == log.events);
  }

  SECTION("backupSessionLog_StateLogNotNull_LogBackedUp") {
    SessionSettings settings;
    TestLog log;
    log.events = 5;

    SessionState state(UtcTimeStamp::now());
    state.log(&log);

    state.backup();

    CHECK(5 == log.eventsBackup);
  }

  SECTION("backupSessionLog_StateLogIsNull_LogBackedUp") {
    SessionSettings settings;
    TestLog log;
    log.events = 5;

    SessionState state(UtcTimeStamp::now());

    state.backup();

    CHECK(0 == log.eventsBackup);
  }
}

TEST_CASE("SessionState lastSentTime snapshots") {
  const UtcTimeStamp first(1, 2, 3, 123456789, 4, 5, 2024, 9);
  const UtcTimeStamp second(21, 22, 23, 987654321, 24, 11, 2025, 9);
  SessionState state(first);
  const SessionState &constState = state;

  SECTION("copies remain unchanged after an update") {
    CHECK((std::is_same<decltype(state.lastSentTime()), UtcTimeStamp>::value));
    CHECK((std::is_same<decltype(constState.lastSentTime()), UtcTimeStamp>::value));
    const auto &snapshot = state.lastSentTime();
    const auto &constSnapshot = constState.lastSentTime();
    state.lastSentTime(second);
    CHECK(snapshot == first);
    CHECK(constSnapshot == first);
    CHECK(state.lastSentTime() == second);
    CHECK(constState.lastSentTime() == second);
  }

  SECTION("concurrent reads copy a complete timestamp") {
    const UtcTimeStamp now(21, 23, 23, 987654321, 24, 11, 2025, 9);
    state.heartBtInt(30);
    state.lastReceivedTime(now);
    state.sentLogout(true);
    state.logoutTimeout(2);
    state.testRequest(0);
    constexpr int iterations = 100000;
    std::atomic<int> ready{0};
    std::atomic<bool> start{false};
    std::atomic<unsigned int> failures{0};
    auto awaitStart = [&] {
      ready.fetch_add(1);
      while (!start.load()) {
        std::this_thread::yield();
      }
    };
    std::thread writer([&] {
      awaitStart();
      for (int i = 0; i < iterations; ++i) {
        state.lastSentTime(i % 2 == 0 ? second : first);
      }
    });
    std::thread reader([&] {
      awaitStart();
      for (int i = 0; i < iterations; ++i) {
        const UtcTimeStamp snapshot = state.lastSentTime();
        const UtcTimeStamp constSnapshot = constState.lastSentTime();
        if ((snapshot != first && snapshot != second) || (constSnapshot != first && constSnapshot != second)) {
          failures.fetch_add(1, std::memory_order_relaxed);
        }
        if (constState.withinHeartBeat(now) || !constState.needHeartbeat(now) || !constState.logoutTimedOut(now)) {
          failures.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
    while (ready.load() != 2) {
      std::this_thread::yield();
    }
    start.store(true);
    writer.join();
    reader.join();
    CHECK(failures.load() == 0);
  }
}

TEST_CASE("SessionState heartbeat and logout boundaries") {
  const UtcTimeStamp sent(12, 0, 0, 123456789, 4, 5, 2024, 9);
  SessionState state(sent);
  state.heartBtInt(30);
  state.logoutTimeout(2);
  state.sentLogout(true);

  SECTION("heartbeat is due at the interval") {
    for (int elapsed : {29, 30, 31}) {
      UtcTimeStamp now = sent;
      now += elapsed;
      state.lastReceivedTime(now);
      CAPTURE(elapsed);
      CHECK(state.withinHeartBeat(now) == (elapsed < 30));
      CHECK(state.needHeartbeat(now) == (elapsed >= 30));
      state.testRequest(1);
      CHECK_FALSE(state.needHeartbeat(now));
      state.testRequest(0);
    }
  }

  SECTION("logout times out at the interval") {
    for (int elapsed : {1, 2, 3}) {
      UtcTimeStamp now = sent;
      now += elapsed;
      CAPTURE(elapsed);
      CHECK(state.logoutTimedOut(now) == (elapsed >= 2));
      state.sentLogout(false);
      CHECK_FALSE(state.logoutTimedOut(now));
      state.sentLogout(true);
    }
  }
}
