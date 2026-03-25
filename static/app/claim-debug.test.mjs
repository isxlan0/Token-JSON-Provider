import test from "node:test";
import assert from "node:assert/strict";

import {
  normalizeClaimDebugFlag,
  readClaimDebugFlagFromSearch,
  resolveClaimDebugMode,
  resolveServerClaimDebug,
  summarizeClaimAcceptedPayload,
  summarizeClaimRealtimePayload,
} from "./claim-debug.js";

test("normalizeClaimDebugFlag handles common values", () => {
  assert.equal(normalizeClaimDebugFlag("1"), true);
  assert.equal(normalizeClaimDebugFlag("debug"), true);
  assert.equal(normalizeClaimDebugFlag("false"), false);
  assert.equal(normalizeClaimDebugFlag("off"), false);
  assert.equal(normalizeClaimDebugFlag(""), null);
  assert.equal(normalizeClaimDebugFlag("maybe"), null);
});

test("readClaimDebugFlagFromSearch reads query flag", () => {
  assert.equal(readClaimDebugFlagFromSearch("?claim_debug=1"), true);
  assert.equal(readClaimDebugFlagFromSearch("?claim_debug=0"), false);
  assert.equal(readClaimDebugFlagFromSearch("?other=1"), null);
});

test("resolveClaimDebugMode keeps precedence query over storage over server", () => {
  assert.deepEqual(
    resolveClaimDebugMode({
      search: "?claim_debug=1",
      storageValue: "0",
      serverDebug: false,
    }),
    { enabled: true, source: "query" }
  );
  assert.deepEqual(
    resolveClaimDebugMode({
      search: "",
      storageValue: "true",
      serverDebug: false,
    }),
    { enabled: true, source: "storage" }
  );
  assert.deepEqual(
    resolveClaimDebugMode({
      search: "",
      storageValue: null,
      serverDebug: true,
    }),
    { enabled: true, source: "server" }
  );
});

test("resolveServerClaimDebug accepts claim trace and debug log level", () => {
  assert.equal(resolveServerClaimDebug({ claim_trace: true, log_level: "info" }), true);
  assert.equal(resolveServerClaimDebug({ log_level: "debug" }), true);
  assert.equal(resolveServerClaimDebug({ log_level: "info" }), false);
  assert.equal(resolveServerClaimDebug(null), null);
});

test("claim payload summaries stay small and stable", () => {
  assert.deepEqual(
    summarizeClaimAcceptedPayload({
      request_id: "req-1",
      status: "queued_waiting",
      queued: true,
      requested: 2,
      queue_total: 4,
    }),
    {
      request_id: "req-1",
      status: "queued_waiting",
      queued: true,
      requested: 2,
      granted: null,
      remaining: null,
      queue_id: null,
      queue_position: null,
      queue_total: 4,
      block_reason: "",
      terminal: false,
    }
  );

  assert.deepEqual(
    summarizeClaimRealtimePayload({
      stream_required: true,
      transport: "sse",
      requests: [{
        request_id: "req-2",
        status: "queued_blocked",
        queued: true,
        remaining: 1,
        queue_position: 3,
        queue_total: 9,
        block_reason: "inventory_unavailable",
        origin_tab_id: "tab-a",
      }],
    }),
    {
      request_count: 1,
      stream_required: true,
      transport: "sse",
      requests: [{
        request_id: "req-2",
        status: "queued_blocked",
        queued: true,
        terminal: false,
        requested: null,
        granted: null,
        remaining: 1,
        queue_position: 3,
        queue_total: 9,
        block_reason: "inventory_unavailable",
        origin_tab_id: "tab-a",
      }],
    }
  );
});
