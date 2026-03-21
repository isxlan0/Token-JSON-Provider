export function createLeaderSyncController(deps) {
  const {
    state,
    constants,
  } = deps;

  const {
    LEADER_HEARTBEAT_MS,
    LEADER_STALE_MS,
    LEADER_PROBE_WAIT_MS,
    LEADERSHIP_REASON_PRIORITY,
  } = constants;

  function hasCrossTabCoordination() {
    return state.crossTabBusMode === "broadcast";
  }

  function normalizeLeadershipPriority(value) {
    const priority = Number(value);
    if (!Number.isFinite(priority)) {
      return 0;
    }
    return Math.max(0, Math.floor(priority));
  }

  function getLeadershipPriority(reason = "background") {
    return normalizeLeadershipPriority(LEADERSHIP_REASON_PRIORITY[reason] ?? 0);
  }

  function buildLeadershipRequest(force = false, options = {}) {
    const reason = typeof options.reason === "string" && options.reason
      ? options.reason
      : (force ? "manual" : "background");
    return {
      reason,
      priority: normalizeLeadershipPriority(options.priority ?? getLeadershipPriority(reason)),
      allowTakeover: Boolean(force || options.allowTakeover || getLeadershipPriority(reason) > 0),
    };
  }

  function normalizeLeaderSignal(payload) {
    const tabId = typeof payload?.tab_id === "string" ? payload.tab_id : "";
    const termTs = Number(payload?.term_ts);
    if (!tabId || !Number.isFinite(termTs)) {
      return null;
    }
    return {
      tabId,
      termTs,
      priority: normalizeLeadershipPriority(payload?.priority),
      reason: typeof payload?.reason === "string" && payload.reason ? payload.reason : "background",
      lastSeenAt: Date.now(),
    };
  }

  function compareLeaderSignals(left, right) {
    if ((left?.termTs || 0) !== (right?.termTs || 0)) {
      return (left?.termTs || 0) - (right?.termTs || 0);
    }
    if ((left?.priority || 0) !== (right?.priority || 0)) {
      return (left?.priority || 0) - (right?.priority || 0);
    }
    return String(right?.tabId || "").localeCompare(String(left?.tabId || ""));
  }

  function isFreshLeaderSignal(signal) {
    return Boolean(signal && Date.now() - Number(signal.lastSeenAt || 0) <= LEADER_STALE_MS);
  }

  function rememberLeaderSignal(signal) {
    if (!signal) {
      return state.currentLeader;
    }
    const nextSignal = {
      tabId: signal.tabId,
      termTs: signal.termTs,
      priority: normalizeLeadershipPriority(signal.priority),
      reason: typeof signal.reason === "string" && signal.reason ? signal.reason : "background",
      lastSeenAt: Date.now(),
    };
    const current = state.currentLeader;
    if (!current || !isFreshLeaderSignal(current)) {
      state.currentLeader = nextSignal;
      return nextSignal;
    }
    if (current.tabId === nextSignal.tabId) {
      state.currentLeader = nextSignal;
      return nextSignal;
    }
    if (compareLeaderSignals(nextSignal, current) > 0) {
      state.currentLeader = nextSignal;
      return nextSignal;
    }
    return current;
  }

  function claimNextLeaderTerm(seedTs = Date.now()) {
    return Math.max(
      Number(seedTs) || Date.now(),
      Number(state.currentLeader?.termTs || 0) + 1
    );
  }

  function clearLeaderRetry() {
    if (state.leaderRetryTimer) {
      clearTimeout(state.leaderRetryTimer);
      state.leaderRetryTimer = null;
    }
  }

  function scheduleLeaderRetry(delayMs = LEADER_PROBE_WAIT_MS) {
    clearLeaderRetry();
    if (!hasCrossTabCoordination()) {
      return;
    }
    state.leaderRetryTimer = setTimeout(() => {
      state.leaderRetryTimer = null;
      if (!deps.shouldRunBackgroundRefresh?.() || state.leaderCandidate || isFreshLeaderSignal(state.currentLeader)) {
        return;
      }
      claimPollingLeadership();
    }, Math.max(0, delayMs));
  }

  function clearLeaderProbe() {
    if (state.leaderProbeTimer) {
      clearTimeout(state.leaderProbeTimer);
      state.leaderProbeTimer = null;
    }
    state.leaderCandidate = null;
    state.leaderCandidatePeers = new Map();
  }

  function buildLeaderProbePeer(candidate) {
    return {
      tabId: typeof candidate?.tabId === "string" ? candidate.tabId : state.tabId,
      priority: normalizeLeadershipPriority(candidate?.priority),
      reason: typeof candidate?.reason === "string" && candidate.reason ? candidate.reason : "background",
    };
  }

  function rememberLeaderProbePeer(candidate) {
    const peer = buildLeaderProbePeer(candidate);
    const current = state.leaderCandidatePeers.get(peer.tabId);
    if (
      current &&
      current.priority === peer.priority &&
      current.reason === peer.reason
    ) {
      return false;
    }
    state.leaderCandidatePeers.set(peer.tabId, peer);
    return true;
  }

  function normalizeLeaderProbeMessage(message) {
    const payload = message?.payload || {};
    const tabId = typeof payload?.candidate_tab_id === "string" && payload.candidate_tab_id
      ? payload.candidate_tab_id
      : (typeof message?.tabId === "string" ? message.tabId : "");
    if (!tabId) {
      return null;
    }
    const termTsRaw = payload?.term_ts;
    const termTs = Number.isFinite(Number(termTsRaw)) ? Number(termTsRaw) : null;
    return {
      tabId,
      priority: normalizeLeadershipPriority(payload?.priority),
      reason: typeof payload?.reason === "string" && payload.reason ? payload.reason : "background",
      termTs,
    };
  }

  function getLeaderProbeWinnerCandidate() {
    const candidates = Array.from(state.leaderCandidatePeers.values()).filter(Boolean).sort((left, right) => {
      if ((left?.priority || 0) !== (right?.priority || 0)) {
        return (right?.priority || 0) - (left?.priority || 0);
      }
      return String(left?.tabId || "").localeCompare(String(right?.tabId || ""));
    });
    return candidates[0] || buildLeaderProbePeer(state.leaderCandidate);
  }

  function startLeaderHeartbeat() {
    if (state.leaderHeartbeatTimer) {
      clearInterval(state.leaderHeartbeatTimer);
    }
    state.leaderHeartbeatTimer = setInterval(() => {
      if (!state.isPollingLeader || !deps.shouldRunBackgroundRefresh?.() || state.currentLeader?.tabId !== state.tabId) {
        return;
      }
      state.currentLeader = {
        ...state.currentLeader,
        lastSeenAt: Date.now(),
      };
      broadcastLeaderSignal("leader_heartbeat", state.currentLeader);
    }, LEADER_HEARTBEAT_MS);
  }

  function stopLeaderHeartbeat() {
    if (state.leaderHeartbeatTimer) {
      clearInterval(state.leaderHeartbeatTimer);
      state.leaderHeartbeatTimer = null;
    }
  }

  function setPollingLeader(isLeader) {
    const nextLeaderState = Boolean(isLeader);
    const wasLeader = state.isPollingLeader;
    state.isPollingLeader = nextLeaderState;
    if (nextLeaderState && !wasLeader) {
      startLeaderHeartbeat();
    } else if (!nextLeaderState && wasLeader) {
      stopLeaderHeartbeat();
    }
    deps.onLeaderStatusChange?.({
      isLeader: nextLeaderState,
      wasLeader,
      currentLeader: state.currentLeader,
    });
  }

  function finalizeLeaderProbe(candidate) {
    const sameCandidate = Boolean(
      state.leaderCandidate &&
      state.leaderCandidate.tabId === candidate.tabId &&
      state.leaderCandidate.startedAt === candidate.startedAt &&
      state.leaderCandidate.termTs === candidate.termTs
    );
    const currentLeader = isFreshLeaderSignal(state.currentLeader) ? state.currentLeader : null;
    const winnerCandidate = getLeaderProbeWinnerCandidate();
    clearLeaderProbe();
    if (!sameCandidate || !deps.shouldRunBackgroundRefresh?.()) {
      return false;
    }
    if (candidate.termTs != null && currentLeader && currentLeader.termTs >= candidate.termTs) {
      if (currentLeader.tabId === state.tabId) {
        if (state.isPollingLeader) {
          return true;
        }
        setPollingLeader(true);
        return true;
      }
      setPollingLeader(false);
      return false;
    }
    if (currentLeader?.tabId === state.tabId) {
      if (state.isPollingLeader) {
        return true;
      }
      setPollingLeader(true);
      return true;
    }
    if (currentLeader && candidate.termTs == null) {
      setPollingLeader(false);
      return false;
    }
    if (winnerCandidate?.tabId === state.tabId) {
      return promoteSelfToLeader(candidate);
    }
    setPollingLeader(false);
    scheduleLeaderRetry();
    return false;
  }

  function startLeaderProbe(request = {}, remoteCandidate = null) {
    if (!hasCrossTabCoordination()) {
      return false;
    }
    const currentLeader = isFreshLeaderSignal(state.currentLeader) ? state.currentLeader : null;
    const remoteTermTs = remoteCandidate && Number.isFinite(Number(remoteCandidate.termTs))
      ? Number(remoteCandidate.termTs)
      : null;
    const nextTermTs = Number.isFinite(Number(request.termTs)) ? Number(request.termTs) : remoteTermTs;
    if (currentLeader && currentLeader.tabId !== state.tabId && nextTermTs == null) {
      return false;
    }
    clearLeaderRetry();
    const normalizedRemoteCandidate = remoteCandidate ? buildLeaderProbePeer(remoteCandidate) : null;
    if (state.leaderCandidate) {
      if (nextTermTs != null && state.leaderCandidate.termTs !== nextTermTs) {
        clearLeaderProbe();
      } else {
        let changed = false;
        if (normalizeLeadershipPriority(request.priority) > normalizeLeadershipPriority(state.leaderCandidate.priority)) {
          state.leaderCandidate = {
            ...state.leaderCandidate,
            priority: normalizeLeadershipPriority(request.priority),
            reason: typeof request.reason === "string" && request.reason ? request.reason : state.leaderCandidate.reason,
          };
          changed = true;
        }
        changed = rememberLeaderProbePeer(state.leaderCandidate) || changed;
        if (normalizedRemoteCandidate) {
          changed = rememberLeaderProbePeer(normalizedRemoteCandidate) || changed;
        }
        if (changed) {
          broadcastMessage("leader_probe", {
            candidate_tab_id: state.tabId,
            priority: state.leaderCandidate.priority,
            reason: state.leaderCandidate.reason,
            term_ts: state.leaderCandidate.termTs,
            sentAt: Date.now(),
          });
        }
        return true;
      }
    }
    const candidate = {
      tabId: state.tabId,
      startedAt: Date.now(),
      priority: normalizeLeadershipPriority(request.priority),
      reason: typeof request.reason === "string" && request.reason ? request.reason : "background",
      termTs: nextTermTs,
    };
    state.leaderCandidate = candidate;
    state.leaderCandidatePeers = new Map();
    rememberLeaderProbePeer(candidate);
    if (normalizedRemoteCandidate) {
      rememberLeaderProbePeer(normalizedRemoteCandidate);
    }
    state.leaderProbeTimer = setTimeout(() => {
      finalizeLeaderProbe(candidate);
    }, LEADER_PROBE_WAIT_MS);
    broadcastMessage("leader_probe", {
      candidate_tab_id: state.tabId,
      priority: candidate.priority,
      reason: candidate.reason,
      term_ts: candidate.termTs,
      sentAt: Date.now(),
    });
    return true;
  }

  function broadcastLeaderSignal(type, signal = state.currentLeader) {
    if (!signal) {
      return;
    }
    broadcastMessage(type, {
      tab_id: signal.tabId,
      term_ts: signal.termTs,
      priority: normalizeLeadershipPriority(signal.priority),
      reason: typeof signal.reason === "string" && signal.reason ? signal.reason : "background",
    });
  }

  function promoteSelfToLeader(candidate = null) {
    clearLeaderRetry();
    clearLeaderProbe();
    const priority = normalizeLeadershipPriority(candidate?.priority);
    const reason = typeof candidate?.reason === "string" && candidate.reason ? candidate.reason : "background";
    const signal = {
      tabId: state.tabId,
      termTs: Number.isFinite(Number(candidate?.termTs)) ? Number(candidate.termTs) : claimNextLeaderTerm(),
      priority,
      reason,
      lastSeenAt: Date.now(),
    };
    state.currentLeader = signal;
    setPollingLeader(true);
    broadcastLeaderSignal("leader_announce", signal);
    return true;
  }

  function releasePollingLeadership(reason = "release") {
    clearLeaderRetry();
    clearLeaderProbe();
    if (hasCrossTabCoordination() && state.isPollingLeader && state.currentLeader?.tabId === state.tabId) {
      broadcastMessage("leader_release", {
        tab_id: state.tabId,
        term_ts: state.currentLeader.termTs,
        reason,
      });
    }
    if (state.currentLeader?.tabId === state.tabId) {
      state.currentLeader = null;
    }
    setPollingLeader(false);
  }

  function claimPollingLeadership(force = false, options = {}) {
    const request = buildLeadershipRequest(force, options);
    if (!deps.shouldRunBackgroundRefresh?.()) {
      clearLeaderRetry();
      clearLeaderProbe();
      setPollingLeader(false);
      return false;
    }
    if (!hasCrossTabCoordination()) {
      clearLeaderRetry();
      clearLeaderProbe();
      state.currentLeader = null;
      setPollingLeader(true);
      return true;
    }
    const currentLeader = isFreshLeaderSignal(state.currentLeader) ? state.currentLeader : null;
    if (!currentLeader) {
      state.currentLeader = null;
    }
    if (currentLeader?.tabId === state.tabId) {
      clearLeaderRetry();
      clearLeaderProbe();
      const nextPriority = Math.max(normalizeLeadershipPriority(currentLeader.priority), request.priority);
      if (nextPriority !== normalizeLeadershipPriority(currentLeader.priority) || currentLeader.reason !== request.reason) {
        state.currentLeader = {
          ...currentLeader,
          priority: nextPriority,
          reason: request.reason,
          lastSeenAt: Date.now(),
        };
        broadcastLeaderSignal("leader_announce", state.currentLeader);
      }
      if (state.isPollingLeader) {
        setPollingLeader(true);
        return true;
      }
      setPollingLeader(true);
      return true;
    }
    if (currentLeader && currentLeader.tabId !== state.tabId) {
      const currentPriority = normalizeLeadershipPriority(currentLeader.priority);
      if (!request.allowTakeover || request.priority <= currentPriority) {
        clearLeaderRetry();
        clearLeaderProbe();
        setPollingLeader(false);
        return false;
      }
    }
    setPollingLeader(false);
    startLeaderProbe({
      reason: request.reason,
      priority: request.priority,
      termTs: currentLeader ? claimNextLeaderTerm(currentLeader.termTs) : null,
    });
    return false;
  }

  function takePollingLeadership(reason = "manual") {
    return claimPollingLeadership(false, {
      reason,
      priority: getLeadershipPriority(reason),
      allowTakeover: true,
    });
  }

  function handleLeaderProbe(message) {
    const candidate = normalizeLeaderProbeMessage(message);
    if (!candidate || !deps.shouldRunBackgroundRefresh?.()) {
      return;
    }
    const currentLeader = isFreshLeaderSignal(state.currentLeader) ? state.currentLeader : null;
    if (currentLeader?.tabId === state.tabId) {
      const currentPriority = normalizeLeadershipPriority(currentLeader.priority);
      if ((candidate.termTs == null || candidate.termTs <= currentLeader.termTs) && candidate.priority <= currentPriority) {
        broadcastLeaderSignal("leader_announce", state.currentLeader);
        return;
      }
      startLeaderProbe(
        {
          reason: currentLeader.reason || "background",
          priority: currentPriority,
          termTs: candidate.termTs,
        },
        candidate
      );
      return;
    }
    if (!currentLeader) {
      startLeaderProbe({}, candidate);
    }
  }

  function handleLeaderSignal(payload) {
    const signal = normalizeLeaderSignal(payload);
    if (!signal) {
      return;
    }
    const previousLeader = state.currentLeader;
    const leader = rememberLeaderSignal(signal);
    if (!leader) {
      return;
    }
    const leaderChanged = !previousLeader
      || previousLeader.tabId !== leader.tabId
      || previousLeader.termTs !== leader.termTs;
    clearLeaderRetry();
    clearLeaderProbe();
    if (leaderChanged) {
      deps.onLeaderChanged?.({
        previousLeader,
        currentLeader: leader,
        leaderChanged,
      });
    }
    if (leader.tabId !== state.tabId) {
      if (state.isPollingLeader) {
        setPollingLeader(false);
      }
    }
  }

  function handleLeaderRelease(payload) {
    const signal = normalizeLeaderSignal(payload);
    if (!signal) {
      return;
    }
    const currentLeader = state.currentLeader;
    if (!currentLeader || currentLeader.tabId !== signal.tabId || currentLeader.termTs !== signal.termTs) {
      return;
    }
    if (signal.tabId === state.tabId) {
      return;
    }
    const previousLeader = state.currentLeader;
    state.currentLeader = null;
    clearLeaderRetry();
    clearLeaderProbe();
    deps.onLeaderChanged?.({
      previousLeader,
      currentLeader: null,
      leaderChanged: true,
    });
    if (!state.visibilityHidden) {
      claimPollingLeadership();
    }
  }

  function handleBroadcastMessage(message) {
    if (!message || message.tabId === state.tabId) {
      return;
    }
    if (message.id && state.lastCrossTabMessageId === message.id) {
      return;
    }
    state.lastCrossTabMessageId = message.id || null;
    if (message.type === "leader_probe") {
      handleLeaderProbe(message);
      return;
    }
    if (message.type === "leader_announce" || message.type === "leader_heartbeat") {
      handleLeaderSignal(message.payload || {});
      return;
    }
    if (message.type === "leader_release") {
      handleLeaderRelease(message.payload || {});
      return;
    }
    deps.onMessage?.(message);
  }

  function closeBroadcastChannel() {
    if (!state.broadcastChannel) {
      return;
    }
    try {
      state.broadcastChannel.close();
    } catch (error) {
      // Ignore channel close failures.
    }
    state.broadcastChannel = null;
  }

  function broadcastMessage(type, payload) {
    const message = {
      id: `${state.tabId}:${Date.now()}:${Math.random().toString(16).slice(2)}`,
      type,
      payload,
      tabId: state.tabId,
      sentAt: Date.now(),
    };
    if (hasCrossTabCoordination() && state.broadcastChannel) {
      try {
        state.broadcastChannel.postMessage(message);
        return true;
      } catch (error) {
        closeBroadcastChannel();
        state.crossTabBusMode = "none";
        state.currentLeader = null;
        clearLeaderRetry();
        clearLeaderProbe();
        setPollingLeader(false);
        deps.onTransportFailure?.({
          reason: "broadcast-channel-failed",
          error,
        });
        if (!state.visibilityHidden) {
          claimPollingLeadership();
        }
      }
    }
    return false;
  }

  function initBroadcastChannel() {
    closeBroadcastChannel();
    state.crossTabBusMode = "none";
    state.currentLeader = null;
    state.lastCrossTabMessageId = null;
    clearLeaderRetry();
    clearLeaderProbe();
    setPollingLeader(false);
    deps.onTransportReset?.({
      reason: "broadcast-channel-init",
    });
    if (!("BroadcastChannel" in window)) {
      return false;
    }
    try {
      const channel = new BroadcastChannel("token-atlas-refresh");
      channel.onmessage = (event) => handleBroadcastMessage(event.data || {});
      state.broadcastChannel = channel;
      state.crossTabBusMode = "broadcast";
      return true;
    } catch (error) {
      state.broadcastChannel = null;
      return false;
    }
  }

  function dispose() {
    clearLeaderRetry();
    clearLeaderProbe();
    stopLeaderHeartbeat();
    closeBroadcastChannel();
    state.crossTabBusMode = "none";
    state.currentLeader = null;
    state.isPollingLeader = false;
    state.lastCrossTabMessageId = null;
  }

  return {
    hasCrossTabCoordination,
    getLeadershipPriority,
    claimPollingLeadership,
    takePollingLeadership,
    releasePollingLeadership,
    broadcastMessage,
    initBroadcastChannel,
    dispose,
  };
}
