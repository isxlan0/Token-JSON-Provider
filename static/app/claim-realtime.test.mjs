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
    status: "queued_waiting",
    queued: true,
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
    status: "queued_waiting",
    queued: true,
    queue_id: 9,
    queue_position: 2,
    queue_remaining: 3,
    requested: 3,
  }, { tabId: "tab-a" });

  let result = applyClaimSnapshotState(initial.state, {
    requests: [{
      request_id: "req-queue",
      status: "queued_waiting",
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

  assert.equal(result.activeRequest?.status, "queued_waiting");
  assert.deepEqual(buildQueueStatusFromRequest(result.queueRequest), {
    queued: true,
    status: "queued_waiting",
    queue_id: 9,
    position: 1,
    total_queued: 4,
    requested: 3,
    remaining: 2,
    request_id: "req-queue",
    block_reason: "",
    last_progress_at: "",
    next_retry_at: "",
    front_blocked: false,
    front_block_reason: "",
    front_next_retry_at: "",
  });
  assert.equal(result.effects.filter((effect) => effect.type === "toast").length, 0);

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

test("accepted queued request does not fake queue total from queue position", () => {
  const result = applyClaimAcceptedState(createInitialClaimRealtimeState(), {
    request_id: "req-accepted",
    status: "queued_waiting",
    queued: true,
    queue_id: 19,
    queue_position: 3,
    requested: 1,
  }, { tabId: "tab-a" });

  assert.equal(result.queueRequest?.queue_position, 3);
  assert.equal(result.queueRequest?.queue_total, 0);
  assert.deepEqual(buildQueueStatusFromRequest(result.queueRequest), {
    queued: true,
    status: "queued_waiting",
    queue_id: 19,
    position: 3,
    total_queued: 0,
    requested: 1,
    remaining: 1,
    request_id: "req-accepted",
    block_reason: "",
    last_progress_at: "",
    next_retry_at: "",
    front_blocked: false,
    front_block_reason: "",
    front_next_retry_at: "",
  });
});

test("accepted terminal request finishes immediately without waiting for stream", () => {
  const result = applyClaimAcceptedState(createInitialClaimRealtimeState(), {
    request_id: "req-direct-accepted",
    status: "succeeded",
    queued: false,
    requested: 1,
    granted: 1,
    remaining: 0,
    items: [{ token_id: 7, file_name: "direct.json" }],
    reason_message: "",
    terminal: true,
  }, { tabId: "tab-a", emitToasts: true });

  assert.equal(result.activeRequest?.status, "succeeded");
  assert.equal(result.activeRequest?.terminal, true);
  assert.equal(result.queueRequest, null);
  assert.equal(result.effects.filter((effect) => effect.type === "terminal").length, 1);
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
    status: "queued_waiting",
    queued: true,
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

test("startup reset terminal stays silent", () => {
  const result = applyClaimSnapshotState(createInitialClaimRealtimeState(), {
    requests: [{
      request_id: "req-startup-reset",
      status: "cancelled",
      requested: 1,
      granted: 0,
      remaining: 1,
      reason_code: "system_startup_reset",
      reason_message: "服务重启，未完成的排队请求已结束。",
      source: "self",
      origin_tab_id: "tab-reset",
      terminal: true,
      updated_at_ts: 90,
    }],
  }, {
    tabId: "tab-reset",
    activeRequestId: "req-startup-reset",
    emitToasts: true,
  });

  assert.equal(result.effects.filter((effect) => effect.type === "toast").length, 0);
  assert.equal(result.effects.filter((effect) => effect.type === "terminal").length, 1);
});

test("restored toast keys suppress duplicate terminal toast after refresh", () => {
  const restored = createInitialClaimRealtimeState({
    toastKeys: {
      "req-refresh:cancelled": true,
    },
  });

  const result = applyClaimSnapshotState(restored, {
    requests: [{
      request_id: "req-refresh",
      status: "cancelled",
      requested: 1,
      granted: 0,
      remaining: 1,
      reason_code: "admin:bug，继续排除中",
      reason_message: "领取请求已取消：admin:bug，继续排除中",
      source: "self",
      origin_tab_id: "tab-refresh",
      terminal: true,
      updated_at_ts: 91,
    }],
  }, {
    tabId: "tab-refresh",
    activeRequestId: "req-refresh",
    emitToasts: true,
  });

  assert.equal(result.effects.filter((effect) => effect.type === "toast").length, 0);
  assert.equal(result.effects.filter((effect) => effect.type === "terminal").length, 1);
});

test("same tab reload restores queued request and still emits one terminal toast", () => {
  const queued = applyClaimSnapshotState(createInitialClaimRealtimeState(), {
    requests: [{
      request_id: "req-reload",
      status: "queued_waiting",
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
  assert.equal(queued.activeRequest?.status, "queued_waiting");

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

test("blocked queued request stays nonterminal and carries block metadata", () => {
  const result = applyClaimSnapshotState(createInitialClaimRealtimeState(), {
    requests: [{
      request_id: "req-blocked",
      status: "queued_blocked",
      queued: true,
      queue_id: 15,
      queue_position: 3,
      queue_total: 6,
      requested: 2,
      granted: 0,
      remaining: 2,
      block_reason: "no_eligible_tokens",
      next_retry_at: "2026-03-24T16:59:08Z",
      source: "self",
      origin_tab_id: "tab-blocked",
      terminal: false,
      updated_at_ts: 80,
    }],
  }, {
    tabId: "tab-blocked",
    activeRequestId: "",
    emitToasts: true,
  });

  assert.equal(result.activeRequest?.status, "queued_blocked");
  assert.equal(result.activeRequest?.terminal, false);
  assert.equal(result.queueRequest?.block_reason, "no_eligible_tokens");
  assert.equal(result.effects.filter((effect) => effect.type === "toast").length, 0);
  assert.deepEqual(buildQueueStatusFromRequest(result.queueRequest), {
    queued: true,
    status: "queued_blocked",
    queue_id: 15,
    position: 3,
    total_queued: 6,
    requested: 2,
    remaining: 2,
    request_id: "req-blocked",
    block_reason: "no_eligible_tokens",
    last_progress_at: "",
    next_retry_at: "2026-03-24T16:59:08Z",
    front_blocked: false,
    front_block_reason: "",
    front_next_retry_at: "",
  });
});
