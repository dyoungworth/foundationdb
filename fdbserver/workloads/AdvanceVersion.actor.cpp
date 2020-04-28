/*
 * AdvanceVersion.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

struct AdvanceVersionWorkload : TestWorkload {
	double minAddVersion, maxAddVersion, jumpVersion, advanceAfter;
    bool ok = true;

	AdvanceVersionWorkload(WorkloadContext const& wcx)
		: TestWorkload(wcx), ok(true)
	{
        advanceAfter = getOption( options, LiteralStringRef("advanceAfter"), 0.0 );
		minAddVersion = getOption( options, LiteralStringRef("minVersionAdd"), 10.0 );
		maxAddVersion = getOption( options, LiteralStringRef("maxVersionAdd"), 10.0 );
		ASSERT(minAddVersion <= maxAddVersion);
        jumpVersion = deterministicRandom()->randomInt64(minAddVersion, maxAddVersion);
	}

	virtual std::string description() { return "AdvanceVersion"; }

	virtual Future<Void> setup( Database const& cx ) {
		return Void();
	}

	virtual Future<Void> start(Database const& cx) {
		if (clientId == 0) return advanceWorker(cx, this);
		return Void();
	}

	virtual Future<bool> check( Database const& cx ) {
		return ok;
	}

	virtual void getMetrics( vector<PerfMetric>& m ) {
	}

	ACTOR static Future<Void> advanceWorker( Database cx, AdvanceVersionWorkload* self ) {
        state Transaction tr(cx);
        state Version rvBefore;
		wait(delay(self->advanceAfter));
		Version rv = wait(tr.getReadVersion());
        rvBefore = rv;
        TraceEvent("AdvanceVersion").detail("By", self->jumpVersion).detail("CurrentVersion", rvBefore);
        wait(advanceVersion(cx, self->jumpVersion + rvBefore));
        tr.reset();
		Version rvAfter = wait(tr.getReadVersion());
        if (rvAfter < rvBefore + self->jumpVersion) {
            TraceEvent("TestFailure").detail("Reason", "version did not advance far enough").detail("CurrentVersion", rvAfter);
            self->ok = false;
        }
		return Void();
	};
};

WorkloadFactory<AdvanceVersionWorkload> AdvanceVersionWorkloadFactory("AdvanceVersion");
