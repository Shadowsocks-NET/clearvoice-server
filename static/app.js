const FRAME_SIZE = 480;
const PING_INTERVAL_MS = 3000;
const PING_TIMEOUT_MS = 8000;
const PING_HISTORY_MAX = 40;
const PRESENCE_REFRESH_MS = 2500;

const serverUrlInput = document.getElementById("serverUrl");
const roomInput = document.getElementById("room");
const nameInput = document.getElementById("name");
const volumeInput = document.getElementById("volume");
const connectBtn = document.getElementById("connectBtn");
const disconnectBtn = document.getElementById("disconnectBtn");
const muteBtn = document.getElementById("muteBtn");
const statusEl = document.getElementById("status");
const logEl = document.getElementById("log");

const roomsListEl = document.getElementById("roomsList");
const usersListEl = document.getElementById("usersList");
const pingValueEl = document.getElementById("pingValue");
const lossValueEl = document.getElementById("lossValue");
const sentValueEl = document.getElementById("sentValue");
const recvValueEl = document.getElementById("recvValue");

serverUrlInput.value = `${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/ws`;

let ws = null;
let audioContext = null;
let stream = null;
let captureNode = null;
let playbackNode = null;
let playbackGain = null;
let fallbackCaptureSink = null;
let micMuted = false;
let usingWorklet = false;

let fallbackCaptureBuffer = new Float32Array(FRAME_SIZE);
let fallbackCaptureBuffered = 0;
let fallbackPlaybackQueue = [];
let fallbackPlaybackFrame = null;
let fallbackPlaybackOffset = 0;

let pingTimer = null;
let pingSeq = 0;
let pingSent = 0;
let pingRecv = 0;
let pingLastRtt = null;
const pingPending = new Map();
const pingHistory = [];

let presenceTimer = null;
let lastPresenceErrorAt = 0;

function log(message) {
  const now = new Date().toLocaleTimeString();
  logEl.textContent += `[${now}] ${message}\n`;
  logEl.scrollTop = logEl.scrollHeight;
}

function setStatus(text) {
  statusEl.textContent = `状态：${text}`;
}

function setConnectedUi(connected) {
  connectBtn.disabled = connected;
  disconnectBtn.disabled = !connected;
  muteBtn.disabled = !connected;
}

function sanitizeToken(raw, fallback) {
  const cleaned = (raw ?? "")
    .toString()
    .replace(/[^a-zA-Z0-9_-]/g, "")
    .slice(0, 32);
  return cleaned || fallback;
}

function currentRoom() {
  return sanitizeToken(roomInput.value, "main");
}

function currentName() {
  return sanitizeToken(nameInput.value, "anonymous");
}

function currentVolumePercent() {
  return Math.max(0, Math.min(100, Number(volumeInput.value) || 100));
}

function buildWsUrl() {
  const base = serverUrlInput.value.trim();
  const room = encodeURIComponent(currentRoom());
  const name = encodeURIComponent(currentName());
  return `${base}?room=${room}&name=${name}`;
}

function apiOriginFromWsUrl() {
  let url;
  try {
    url = new URL(serverUrlInput.value.trim(), location.href);
  } catch {
    url = new URL(location.href);
  }

  if (url.protocol === "wss:") {
    url.protocol = "https:";
  } else if (url.protocol === "ws:") {
    url.protocol = "http:";
  }

  return url.origin;
}

function renderList(listEl, items, renderItem) {
  listEl.innerHTML = "";
  if (!items || items.length === 0) {
    const li = document.createElement("li");
    li.className = "empty";
    li.textContent = "暂无数据";
    listEl.appendChild(li);
    return;
  }

  for (const item of items) {
    listEl.appendChild(renderItem(item));
  }
}

function renderRooms(rooms) {
  const sorted = [...rooms].sort((a, b) => a.name.localeCompare(b.name));
  renderList(roomsListEl, sorted, (room) => {
    const li = document.createElement("li");
    const name = document.createElement("span");
    name.textContent = room.name;
    const count = document.createElement("span");
    count.textContent = `${room.online}`;
    li.appendChild(name);
    li.appendChild(count);
    return li;
  });
}

function renderUsers(users) {
  const sorted = [...users].sort((a, b) => a.localeCompare(b));
  renderList(usersListEl, sorted, (user) => {
    const li = document.createElement("li");
    li.textContent = user;
    return li;
  });
}

function resetPingStats() {
  pingSeq = 0;
  pingSent = 0;
  pingRecv = 0;
  pingLastRtt = null;
  pingPending.clear();
  pingHistory.length = 0;
  updatePingStatsUi();
}

function updatePingStatsUi() {
  pingValueEl.textContent = pingLastRtt == null ? "-" : `${pingLastRtt} ms`;
  sentValueEl.textContent = String(pingSent);
  recvValueEl.textContent = String(pingRecv);

  let considered = 0;
  let lost = 0;
  for (const item of pingHistory) {
    if (item.status === "acked" || item.status === "lost") {
      considered += 1;
    }
    if (item.status === "lost") {
      lost += 1;
    }
  }

  const loss = considered === 0 ? 0 : (lost / considered) * 100;
  lossValueEl.textContent = `${loss.toFixed(1)}%`;
}

function markPingStatus(seq, status) {
  for (let i = pingHistory.length - 1; i >= 0; i -= 1) {
    if (pingHistory[i].seq === seq) {
      pingHistory[i].status = status;
      return;
    }
  }
}

function trimPingHistory() {
  if (pingHistory.length <= PING_HISTORY_MAX) {
    return;
  }
  pingHistory.splice(0, pingHistory.length - PING_HISTORY_MAX);
}

function sweepPingTimeouts() {
  const now = Date.now();
  for (const [seq, sentAt] of pingPending.entries()) {
    if (now - sentAt > PING_TIMEOUT_MS) {
      pingPending.delete(seq);
      markPingStatus(seq, "lost");
    }
  }
}

function sendPingProbe() {
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    return;
  }

  sweepPingTimeouts();

  const seq = ++pingSeq;
  const now = Date.now();
  pingSent += 1;
  pingPending.set(seq, now);
  pingHistory.push({ seq, status: "pending" });
  trimPingHistory();

  ws.send(JSON.stringify({ type: "ping", seq, ts: now }));
  updatePingStatsUi();
}

function onPongMessage(seq) {
  const sentAt = pingPending.get(seq);
  if (sentAt == null) {
    return;
  }

  pingPending.delete(seq);
  pingRecv += 1;
  pingLastRtt = Math.max(0, Date.now() - sentAt);
  markPingStatus(seq, "acked");
  updatePingStatsUi();
}

function startPingLoop() {
  stopPingLoop();
  sendPingProbe();
  pingTimer = setInterval(() => {
    sendPingProbe();
  }, PING_INTERVAL_MS);
}

function stopPingLoop() {
  if (pingTimer) {
    clearInterval(pingTimer);
    pingTimer = null;
  }
}

function handleSignalText(raw) {
  let data;
  try {
    data = JSON.parse(raw);
  } catch {
    return;
  }

  if (data.type === "pong" && Number.isInteger(data.seq)) {
    onPongMessage(data.seq);
    return;
  }

  if (data.type === "room_users" && typeof data.room === "string" && Array.isArray(data.users)) {
    if (data.room === currentRoom()) {
      renderUsers(data.users.filter((item) => typeof item === "string"));
    }
  }
}

async function fetchJson(url, timeoutMs = 1800) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      cache: "no-store",
      signal: controller.signal,
    });
    if (!res.ok) {
      throw new Error(`HTTP ${res.status}`);
    }
    return await res.json();
  } finally {
    clearTimeout(timeout);
  }
}

async function refreshPresence() {
  const origin = apiOriginFromWsUrl();
  const room = currentRoom();

  try {
    const [roomsData, usersData] = await Promise.all([
      fetchJson(`${origin}/api/rooms`),
      fetchJson(`${origin}/api/rooms/${encodeURIComponent(room)}/users`),
    ]);

    renderRooms(Array.isArray(roomsData.rooms) ? roomsData.rooms : []);
    renderUsers(Array.isArray(usersData.users) ? usersData.users : []);
  } catch (err) {
    const now = Date.now();
    if (now - lastPresenceErrorAt > 15000) {
      log(`拉取在线信息失败: ${err.message}`);
      lastPresenceErrorAt = now;
    }
  }
}

function startPresenceLoop() {
  if (presenceTimer) {
    clearInterval(presenceTimer);
  }
  refreshPresence().catch(() => {});
  presenceTimer = setInterval(() => {
    refreshPresence().catch(() => {});
  }, PRESENCE_REFRESH_MS);
}

async function ensureAudio() {
  const Ctx = window.AudioContext || window.webkitAudioContext;
  if (!Ctx) {
    throw new Error("当前浏览器不支持 Web Audio API");
  }
  audioContext = new Ctx({ sampleRate: 48000 });

  if (audioContext.sampleRate !== 48000) {
    throw new Error(`当前浏览器音频采样率为 ${audioContext.sampleRate}，请使用 48kHz 设备后重试`);
  }

  stream = await navigator.mediaDevices.getUserMedia({
    audio: {
      channelCount: 1,
      sampleRate: 48000,
      echoCancellation: false,
      noiseSuppression: false,
      autoGainControl: false,
    },
    video: false,
  });

  const micSource = audioContext.createMediaStreamSource(stream);
  usingWorklet = Boolean(audioContext.audioWorklet && typeof AudioWorkletNode !== "undefined");

  playbackGain = audioContext.createGain();
  playbackGain.gain.value = currentVolumePercent() / 100;

  if (usingWorklet) {
    await audioContext.audioWorklet.addModule("/worklets/capture-processor.js");
    await audioContext.audioWorklet.addModule("/worklets/playback-processor.js");

    captureNode = new AudioWorkletNode(audioContext, "capture-processor", {
      numberOfInputs: 1,
      numberOfOutputs: 0,
      channelCount: 1,
    });
    micSource.connect(captureNode);

    playbackNode = new AudioWorkletNode(audioContext, "playback-processor", {
      numberOfInputs: 0,
      numberOfOutputs: 1,
      outputChannelCount: [1],
    });
    playbackNode.connect(playbackGain).connect(audioContext.destination);

    captureNode.port.onmessage = (event) => {
      if (!ws || ws.readyState !== WebSocket.OPEN || micMuted) {
        return;
      }

      const frame = event.data;
      if (!(frame instanceof ArrayBuffer) || frame.byteLength !== FRAME_SIZE * 2) {
        return;
      }
      ws.send(frame);
    };
  } else {
    log("浏览器不支持 AudioWorklet，已切换兼容模式（ScriptProcessor）");

    captureNode = audioContext.createScriptProcessor(1024, 1, 1);
    micSource.connect(captureNode);

    fallbackCaptureSink = audioContext.createGain();
    fallbackCaptureSink.gain.value = 0;
    captureNode.connect(fallbackCaptureSink);
    fallbackCaptureSink.connect(audioContext.destination);

    captureNode.onaudioprocess = (event) => {
      if (!ws || ws.readyState !== WebSocket.OPEN || micMuted) {
        return;
      }

      const input = event.inputBuffer.getChannelData(0);
      for (let i = 0; i < input.length; i += 1) {
        fallbackCaptureBuffer[fallbackCaptureBuffered] = input[i];
        fallbackCaptureBuffered += 1;

        if (fallbackCaptureBuffered === FRAME_SIZE) {
          const pcm = new Int16Array(FRAME_SIZE);
          for (let j = 0; j < FRAME_SIZE; j += 1) {
            const s = Math.max(-1, Math.min(1, fallbackCaptureBuffer[j]));
            pcm[j] = s < 0 ? s * 32768 : s * 32767;
          }
          ws.send(pcm.buffer);
          fallbackCaptureBuffered = 0;
        }
      }
    };

    playbackNode = audioContext.createScriptProcessor(1024, 0, 1);
    playbackNode.onaudioprocess = (event) => {
      const output = event.outputBuffer.getChannelData(0);
      output.fill(0);

      let written = 0;
      while (written < output.length) {
        if (fallbackPlaybackFrame === null) {
          fallbackPlaybackFrame = fallbackPlaybackQueue.shift() ?? null;
          fallbackPlaybackOffset = 0;
          if (fallbackPlaybackFrame === null) {
            break;
          }
        }

        const remainOut = output.length - written;
        const remainFrame = fallbackPlaybackFrame.length - fallbackPlaybackOffset;
        const copyLen = Math.min(remainOut, remainFrame);

        output.set(
          fallbackPlaybackFrame.subarray(
            fallbackPlaybackOffset,
            fallbackPlaybackOffset + copyLen
          ),
          written
        );

        written += copyLen;
        fallbackPlaybackOffset += copyLen;

        if (fallbackPlaybackOffset >= fallbackPlaybackFrame.length) {
          fallbackPlaybackFrame = null;
          fallbackPlaybackOffset = 0;
        }
      }
    };
    playbackNode.connect(playbackGain).connect(audioContext.destination);
  }

  await audioContext.resume();
}

function bindWsEvents() {
  ws.binaryType = "arraybuffer";

  ws.onopen = () => {
    setStatus("已连接，语音中");
    setConnectedUi(true);
    startPingLoop();
    refreshPresence().catch(() => {});
    log("WebSocket 已连接");
  };

  ws.onclose = () => {
    setStatus("已断开");
    setConnectedUi(false);
    stopPingLoop();
    log("WebSocket 已断开");
    teardownAudio().catch((err) => log(`释放音频资源失败: ${err.message}`));
    refreshPresence().catch(() => {});
  };

  ws.onerror = () => {
    log("WebSocket 错误");
  };

  ws.onmessage = (event) => {
    if (typeof event.data === "string") {
      handleSignalText(event.data);
      return;
    }

    if (!(event.data instanceof ArrayBuffer)) {
      return;
    }

    if (usingWorklet) {
      playbackNode.port.postMessage(event.data, [event.data]);
    } else {
      const input = new Int16Array(event.data);
      const frame = new Float32Array(input.length);
      for (let i = 0; i < input.length; i += 1) {
        frame[i] = input[i] / 32768;
      }
      fallbackPlaybackQueue.push(frame);
      if (fallbackPlaybackQueue.length > 128) {
        fallbackPlaybackQueue.splice(0, fallbackPlaybackQueue.length - 128);
      }
    }
  };
}

async function teardownAudio() {
  if (stream) {
    for (const track of stream.getTracks()) {
      track.stop();
    }
    stream = null;
  }
  if (captureNode) {
    captureNode.disconnect();
    captureNode.onaudioprocess = null;
    captureNode = null;
  }
  if (playbackNode) {
    playbackNode.disconnect();
    playbackNode.onaudioprocess = null;
    playbackNode = null;
  }
  if (fallbackCaptureSink) {
    fallbackCaptureSink.disconnect();
    fallbackCaptureSink = null;
  }
  if (playbackGain) {
    playbackGain.disconnect();
    playbackGain = null;
  }
  fallbackCaptureBuffered = 0;
  fallbackPlaybackQueue = [];
  fallbackPlaybackFrame = null;
  fallbackPlaybackOffset = 0;
  usingWorklet = false;
  if (audioContext) {
    await audioContext.close();
    audioContext = null;
  }
}

async function connect() {
  if (ws && ws.readyState === WebSocket.OPEN) {
    return;
  }

  resetPingStats();
  setStatus("初始化音频...");
  await ensureAudio();
  ws = new WebSocket(buildWsUrl());
  bindWsEvents();
}

function disconnect() {
  if (ws) {
    ws.close();
    ws = null;
  }
}

connectBtn.addEventListener("click", async () => {
  try {
    await connect();
  } catch (err) {
    log(`连接失败: ${err.message}`);
    setStatus("连接失败");
    setConnectedUi(false);
    stopPingLoop();
    await teardownAudio().catch(() => {});
  }
});

disconnectBtn.addEventListener("click", () => {
  disconnect();
});

muteBtn.addEventListener("click", () => {
  micMuted = !micMuted;
  muteBtn.textContent = micMuted ? "取消静音上行" : "静音上行";
  log(micMuted ? "已静音上行音频" : "已恢复上行音频");
});

volumeInput.addEventListener("change", () => {
  const v = currentVolumePercent();
  volumeInput.value = String(v);
  if (playbackGain) {
    playbackGain.gain.value = v / 100;
  }
});

roomInput.addEventListener("change", () => {
  roomInput.value = currentRoom();
  refreshPresence().catch(() => {});
  if (ws && ws.readyState === WebSocket.OPEN) {
    log("房间已修改，需断开后重连才会切换会话房间");
  }
});

nameInput.addEventListener("change", () => {
  nameInput.value = currentName();
  if (ws && ws.readyState === WebSocket.OPEN) {
    log("昵称已修改，需断开后重连才会生效");
  }
});

serverUrlInput.addEventListener("change", () => {
  refreshPresence().catch(() => {});
});

renderRooms([]);
renderUsers([]);
resetPingStats();
startPresenceLoop();
