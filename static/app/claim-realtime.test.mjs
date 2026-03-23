import test from "node:test";
import assert from "node:assert/strict";

import {
  applyClaimAcceptedState,
  applyClaimSnapshotState,
  buildQueueStatusFromRequest,
  createInitialClaimRealtimeState,
} from "./claim-realtime.js";

test("direct success only emits one terminal toast", () => {
  const initial = createInitialClaimRealtimeState();
  const accepted = applyClaimAcceptedState(initial, {
    request_id: "req-direct",
    status: "processing",
    queued: false,
    requested: 1,
  }, { tabId: "tab-a" });

  const result = applyClaimSnapshotState(accepted.state, {
    requests: [{
      request_id: "req-direct",
      status: "succeeded",
      requested: 1,
      granted: 1,
      remaining: 0,
      source: "self",
      origin_tab_id: "tab-a",
      terminal: true,
      updated_at_ts: 10,
    }],
  }, {
    tabId: "tab-a",
    activeRequestId: accepted.state.activeRequestId,
    emitToasts: true,
  });

  assert.equal(result.activeRequest?.status, "succeeded");
  assert.deepEqual(
    result.effects.filter((effect) => effect.type === "toast").map((effect) => effect.title),
    ["申请成功"]
  );

  const replay = applyClaimSnapshotState(result.state, {
    requests: [{
      request_id: "req-direct",
      status: "succeeded",
      requested: 1,
      granted: 1,
      remaining: 0,
      source: "self",
      origin_tab_id: "tab-a",
      terminal: true,
      updated_at_ts: 11,
    }],
  }, {
    tabId: "tab-a",
    activeRequestId: result.state.activeRequestId,
    emitToasts: true,
  });

  assert.equal(replay.effects.filter((effect) => effect.type === "toast").length, 0);
});

test("queued request keeps queue status then completes", () => {
  const initial = applyClaimAcceptedState(createInitialClaimRealtimeState(), {
    request_id: "req-queue",
    status: "queued",
    queued: true,
    queue_id: 9,
    queue_position: 2,
    queue_remaining: 3,
    requested: 3,
  }, { tabId: "tab-a" });

  let result = applyClaimSnapshotState(initial.state, {
    requests: [{
      request_id: "req-queue",
      status: "queued",
      queued: true,
      queue_id: 9,
      queue_position: 1,
      queue_total: 4,
      requested: 3,
      granted: 1,
      remaining: 2,
      source: "self",
      origin_tab_id: "tab-a",
      terminal: false,
      updated_at_ts: 20,
    }],
  }, {
    tabId: "tab-a",
    activeRequestId: initial.state.activeRequestId,
    emitToasts: true,
  });

  assert.equal(result.activeRequest?.status, "queued");
  assert.deepEqual(buildQueueStatusFromRequest(result.queueRequest), {
    queued: true,
    queue_id: 9,
    position: 1,
    total_queued: 4,
    requested: 3,
    remaining: 2,
    request_id: "req-queue",
  });

  result = applyClaimSnapshotState(result.state, {
    requests: [{
      request_id: "req-queue",
      status: "succeeded",
      requested: 3,
      granted: 3,
      remaining: 0,
      source: "self",
      origin_tab_id: "tab-a",
      terminal: true,
      updated_at_ts: 30,
    }],
  }, {
    tabId: "tab-a",
    activeRequestId: result.state.activeRequestId,
    emitToasts: true,
  });

  assert.equal(result.activeRequest?.status, "succeeded");
  assert.equal(result.queueRequest, null);
  assert.deepEqual(
    result.effects.filter((effect) => effect.type === "toast").map((effect) => effect.title),
    ["申请成功"]
  );
});

test("other session delivery emits explicit other session toast", () => {
  const result = applyClaimSnapshotState(createInitialClaimRealtimeState(), {
    requests: [{
      request_id: "req-other",
      status: "succeeded",
      requested: 1,
      granted: 1,
      remaining: 0,
      source: "other_session",
      origin_tab_id: "",
      terminal: true,
      updated_at_ts: 40,
    }],
  }, {
    tabId: "tab-a",
    emitToasts: true,
  });

  assert.deepEqual(
    result.effects.filter((effect) => effect.type === "toast").map((effect) => effect.title),
    ["账号已到账"]
  );
});

test("duplicate tab does not emit self toast for non-origin tab", () => {
  const accepted = applyClaimAcceptedState(createInitialClaimRealtimeState(), {
    request_id: "req-tab",
    status: "processing",
    queued: false,
    requested: 1,
  }, { tabId: "tab-origin" });

  const result = applyClaimSnapshotState(accepted.state, {
    requests: [{
      request_id: "req-tab",
      status: "succeeded",
      requested: 1,
      granted: 1,
      remaining: 0,
      source: "self",
      origin_tab_id: "tab-origin",
      terminal: true,
      updated_at_ts: 50,
    }],
  }, {
    tabId: "tab-follower",
    activeRequestId: accepted.state.activeRequestId,
    emitToasts: true,
  });

  assert.equal(result.effects.filter((effect) => effect.type === "toast").length, 0);
});

test("stream reconnect reuses terminal dedupe state", () => {
  const first = applyClaimSnapshotState(createInitialClaimRealtimeState(), {
    requests: [{
      request_id: "req-reconnect",
      status: "succeeded",
      requested: 1,
      granted: 1,
      remaining: 0,
      source: "self",
      origin_tab_id: "tab-a",
      terminal: true,
      updated_at_ts: 60,
    }],
  }, {
    tabId: "tab-a",
    activeRequestId: "req-reconnect",
    emitToasts: true,
  });

  assert.equal(first.effects.filter((effect) => effect.type === "toast").length, 1);

  const afterReconnect = applyClaimSnapshotState(first.state, {
    requests: [{
      request_id: "req-reconnect",
      status: "succeeded",
      requested: 1,
      granted: 1,
      remaining: 0,
      source: "self",
      origin_tab_id: "tab-a",
      terminal: true,
      updated_at_ts: 61,
    }],
  }, {
    tabId: "tab-a",
    activeRequestId: "req-reconnect",
    emitToasts: true,
  });

  assert.equal(afterReconnect.effects.filter((effect) => effect.type === "toast").length, 0);
});

test("same tab reload restores queued request and still emits one terminal toast", () => {
  const queued = applyClaimSnapshotState(createInitialClaimRealtimeState(), {
    requests: [{
      request_id: "req-reload",
      status: "queued",
      queued: true,
      queue_id: 12,
      queue_position: 2,
      queue_total: 5,
      requested: 2,
      granted: 0,
      remaining: 2,
      source: "self",
      origin_tab_id: "tab-persisted",
      terminal: false,
      updated_at_ts: 70,
    }],
  }, {
    tabId: "tab-persisted",
    activeRequestId: "",
    emitToasts: false,
  });

  assert.equal(queued.state.activeRequestId, "req-reload");
  assert.equal(queued.activeRequest?.status, "queued");

  const completed = applyClaimSnapshotState(queued.state, {
    requests: [{
      request_id: "req-reload",
      status: "succeeded",
      requested: 2,
      granted: 2,
      remaining: 0,
      source: "self",
      origin_tab_id: "tab-persisted",
      terminal: true,
      updated_at_ts: 71,
    }],
  }, {
    tabId: "tab-persisted",
    activeRequestId: queued.state.activeRequestId,
    emitToasts: true,
  });

  assert.deepEqual(
    completed.effects.filter((effect) => effect.type === "toast").map((effect) => effect.title),
    ["申请成功"]
  );
});
