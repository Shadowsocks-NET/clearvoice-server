const FRAME_SIZE = 480;
const PING_INTERVAL_MS = 3000;
const PING_TIMEOUT_MS = 8000;
const PING_HISTORY_MAX = 40;
const PRESENCE_REFRESH_MS = 2500;
const PLAYBACK_QUEUE_MAX_FRAMES = 24;
const WS_MAX_BUFFERED_BYTES = 64 * 1024;
const BACKPRESSURE_LOG_INTERVAL_MS = 3000;
const BITRATE_SAMPLE_INTERVAL_MS = 1000;
const OPUS_FRAME_DURATION_US = 10_000;
const OPUS_MAX_ENCODE_QUEUE = 6;
const OPUS_BITRATE = 256_000;
const OPUS_MIN_SOFT_BITRATE = 200_000;
const AUDIO_PROC_ON = "on";
const AUDIO_PROC_OFF = "off";
const AUDIO_ECHO_CANCELLATION_DEFAULT = AUDIO_PROC_ON;
const AUDIO_NOISE_SUPPRESSION_DEFAULT = AUDIO_PROC_ON;
const AUDIO_AUTO_GAIN_CONTROL_DEFAULT = AUDIO_PROC_ON;
const CHANNEL_MODE_MONO = "mono";
const CHANNEL_MODE_STEREO = "stereo";

const AUDIO_PACKET_MAGIC = 0x43; // C
const AUDIO_PACKET_VERSION = 1;
const AUDIO_PACKET_HEADER_BYTES = 4;
const AUDIO_CODEC_PCM16 = 0;
const AUDIO_CODEC_OPUS = 1;
const TRANSPORT_WS_AUDIO = "ws-audio";
const TRANSPORT_WEBRTC = "webrtc";

const serverUrlInput = document.getElementById("serverUrl");
const roomInput = document.getElementById("room");
const nameInput = document.getElementById("name");
const volumeInput = document.getElementById("volume");
const channelModeInput = document.getElementById("channelMode");
const aecModeInput = document.getElementById("aecMode");
const nsModeInput = document.getElementById("nsMode");
const agcModeInput = document.getElementById("agcMode");
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
const txRateValueEl = document.getElementById("txRateValue");
const rxRateValueEl = document.getElementById("rxRateValue");

function defaultWsUrl() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  let path = location.pathname || "/";

  if (!path.endsWith("/")) {
    const idx = path.lastIndexOf("/");
    path = idx <= 0 ? "/" : path.slice(0, idx + 1);
  }

  const base = path === "/" ? "" : path.replace(/\/+$/, "");
  return `${proto}://${location.host}${base}/ws`;
}

serverUrlInput.value = defaultWsUrl();

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

let bitrateTimer = null;
let bitrateSampling = false;
let bitrateLastAtMs = 0;
let bitrateLastTxBytes = 0;
let bitrateLastRxBytes = 0;
let wsAudioTxBytesTotal = 0;
let wsAudioRxBytesTotal = 0;

let presenceTimer = null;
let lastPresenceErrorAt = 0;
let lastBackpressureLogAt = 0;

let codecMode = "pcm";
let audioEncoder = null;
let audioDecoder = null;
let opusCaptureTimestampUs = 0;
let opusDecodeTimestampUs = 0;
let transportMode = TRANSPORT_WS_AUDIO;

let selfPeerId = null;
let rtcLocalStream = null;
let latestRoomPeers = [];
const rtcPeers = new Map();
const rtcRemoteAudios = new Map();

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

function safeParseJson(text) {
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

function sanitizeToken(raw, fallback) {
  const normalized = (raw ?? "").toString().normalize("NFC").trim();
  const cleaned = Array.from(normalized)
    .filter((ch) => {
      const cp = ch.codePointAt(0) ?? 0;
      if (cp <= 0x1f || cp === 0x7f) {
        return false;
      }
      return ch !== "/" && ch !== "\\";
    })
    .slice(0, 32)
    .join("");
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

function currentChannelMode() {
  return channelModeInput?.value === CHANNEL_MODE_STEREO
    ? CHANNEL_MODE_STEREO
    : CHANNEL_MODE_MONO;
}

function currentChannelCount() {
  return currentChannelMode() === CHANNEL_MODE_STEREO ? 2 : 1;
}

function currentChannelLabel() {
  return currentChannelMode() === CHANNEL_MODE_STEREO ? "立体声" : "单声道";
}

function currentAecEnabled() {
  const mode = aecModeInput?.value ?? AUDIO_ECHO_CANCELLATION_DEFAULT;
  return mode !== AUDIO_PROC_OFF;
}

function currentNsEnabled() {
  const mode = nsModeInput?.value ?? AUDIO_NOISE_SUPPRESSION_DEFAULT;
  return mode !== AUDIO_PROC_OFF;
}

function currentAgcEnabled() {
  const mode = agcModeInput?.value ?? AUDIO_AUTO_GAIN_CONTROL_DEFAULT;
  return mode !== AUDIO_PROC_OFF;
}

function normalizeAudioProcessingUiValues() {
  if (aecModeInput) {
    aecModeInput.value = currentAecEnabled() ? AUDIO_PROC_ON : AUDIO_PROC_OFF;
  }
  if (nsModeInput) {
    nsModeInput.value = currentNsEnabled() ? AUDIO_PROC_ON : AUDIO_PROC_OFF;
  }
  if (agcModeInput) {
    agcModeInput.value = currentAgcEnabled() ? AUDIO_PROC_ON : AUDIO_PROC_OFF;
  }
}

function buildAudioProcessingConstraints() {
  return {
    echoCancellation: currentAecEnabled(),
    noiseSuppression: currentNsEnabled(),
    autoGainControl: currentAgcEnabled(),
  };
}

function currentAudioProcessingLabel() {
  return `AEC:${currentAecEnabled() ? "开" : "关"} NS:${currentNsEnabled() ? "开" : "关"} AGC:${currentAgcEnabled() ? "开" : "关"}`;
}

function supportsWebRtcAudio() {
  return (
    typeof RTCPeerConnection !== "undefined" &&
    navigator.mediaDevices &&
    typeof navigator.mediaDevices.getUserMedia === "function"
  );
}

function shouldUseWebRtcTransport() {
  return supportsWebRtcAudio();
}

function updateRtcAudioVolume() {
  const volume = currentVolumePercent() / 100;
  for (const audio of rtcRemoteAudios.values()) {
    audio.volume = volume;
  }
}

function applyRtcMuteState() {
  if (!rtcLocalStream) {
    return;
  }
  for (const track of rtcLocalStream.getAudioTracks()) {
    track.enabled = !micMuted;
  }
}

async function applyAudioProcessingSettingsRealtime() {
  const constraints = buildAudioProcessingConstraints();
  const tracks = [];

  if (rtcLocalStream) {
    tracks.push(...rtcLocalStream.getAudioTracks());
  }
  if (stream) {
    tracks.push(...stream.getAudioTracks());
  }

  const uniqueTracks = [...new Set(tracks)];
  if (uniqueTracks.length === 0) {
    return;
  }

  let ok = 0;
  let failed = 0;
  for (const track of uniqueTracks) {
    if (typeof track.applyConstraints !== "function") {
      failed += 1;
      continue;
    }
    try {
      await track.applyConstraints(constraints);
      ok += 1;
    } catch {
      failed += 1;
    }
  }

  if (ok > 0) {
    log(`音频处理已更新: ${currentAudioProcessingLabel()}`);
  }
  if (failed > 0) {
    log("部分浏览器轨道不支持实时切换 AEC/NS/AGC，必要时请断开重连");
  }
}

function handleRtcDataChannelMessage(peerId, entry, raw) {
  if (typeof raw !== "string") {
    return;
  }
  const data = safeParseJson(raw);
  if (!data || typeof data.type !== "string") {
    return;
  }

  if (data.type === "ping" && Number.isInteger(data.seq) && Number.isFinite(data.ts)) {
    if (entry.dc && entry.dc.readyState === "open") {
      entry.dc.send(JSON.stringify({ type: "pong", seq: data.seq, ts: data.ts }));
    }
    return;
  }

  if (data.type === "pong" && Number.isInteger(data.seq)) {
    onPongMessage(data.seq);
  }
}

function attachRtcDataChannel(peerId, entry, dc) {
  entry.dc = dc;
  dc.onmessage = (event) => {
    handleRtcDataChannelMessage(peerId, entry, event.data);
  };
  dc.onopen = () => {
    log(`WebRTC 数据通道已建立: ${peerId.slice(0, 8)}`);
  };
  dc.onclose = () => {
    if (entry.dc === dc) {
      entry.dc = null;
    }
  };
  dc.onerror = () => {};
}

function ensurePeerDataChannel(peerId, entry) {
  if (entry.dc) {
    return;
  }
  const dc = entry.pc.createDataChannel("ping", { ordered: true });
  attachRtcDataChannel(peerId, entry, dc);
}

function updateFmtpParam(line, key, value) {
  const prefixEnd = line.indexOf(" ");
  const prefix = prefixEnd >= 0 ? line.slice(0, prefixEnd + 1) : `${line} `;
  const paramsRaw = prefixEnd >= 0 ? line.slice(prefixEnd + 1) : "";
  const parts = paramsRaw
    .split(";")
    .map((item) => item.trim())
    .filter((item) => item.length > 0);

  let found = false;
  const updated = parts.map((item) => {
    const idx = item.indexOf("=");
    if (idx < 0) {
      return item;
    }
    const k = item.slice(0, idx).trim().toLowerCase();
    if (k !== key.toLowerCase()) {
      return item;
    }
    found = true;
    return `${key}=${value}`;
  });

  if (!found) {
    updated.push(`${key}=${value}`);
  }

  return `${prefix}${updated.join(";")}`;
}

function setSdpOpusStereo(sdp, stereo) {
  if (!sdp) {
    return sdp;
  }

  const lines = sdp.split("\r\n");
  const opusPayloads = new Set();

  for (const line of lines) {
    const m = /^a=rtpmap:(\d+)\s+opus\/48000(?:\/\d+)?$/i.exec(line);
    if (m) {
      opusPayloads.add(m[1]);
    }
  }

  if (opusPayloads.size === 0) {
    return sdp;
  }

  const stereoValue = stereo ? "1" : "0";
  for (let i = 0; i < lines.length; i += 1) {
    const m = /^a=fmtp:(\d+)\s+/i.exec(lines[i]);
    if (!m) {
      continue;
    }
    if (!opusPayloads.has(m[1])) {
      continue;
    }
    lines[i] = updateFmtpParam(lines[i], "stereo", stereoValue);
    lines[i] = updateFmtpParam(lines[i], "sprop-stereo", stereoValue);
    lines[i] = updateFmtpParam(lines[i], "cbr", "1");
    lines[i] = updateFmtpParam(lines[i], "usedtx", "0");
    lines[i] = updateFmtpParam(lines[i], "maxaveragebitrate", String(OPUS_BITRATE));
  }

  return lines.join("\r\n");
}

async function applyRtcSenderAudioParams(pc) {
  const senders = pc.getSenders().filter((sender) => sender.track?.kind === "audio");
  for (const sender of senders) {
    if (typeof sender.getParameters !== "function" || typeof sender.setParameters !== "function") {
      continue;
    }

    const params = sender.getParameters();
    if (!params.encodings || params.encodings.length === 0) {
      params.encodings = [{}];
    }
    params.encodings[0].maxBitrate = OPUS_BITRATE;
    if ("minBitrate" in params.encodings[0]) {
      params.encodings[0].minBitrate = OPUS_MIN_SOFT_BITRATE;
    }

    try {
      await sender.setParameters(params);
    } catch {}
  }
}

async function setLocalDescriptionWithOpusPrefs(pc, desc) {
  const patchedSdp = setSdpOpusStereo(desc.sdp, currentChannelMode() === CHANNEL_MODE_STEREO);
  await pc.setLocalDescription({
    type: desc.type,
    sdp: patchedSdp,
  });
  await applyRtcSenderAudioParams(pc);
}


function ensureRtcRemoteAudio(peerId) {
  let audio = rtcRemoteAudios.get(peerId);
  if (!audio) {
    audio = document.createElement("audio");
    audio.autoplay = true;
    audio.playsInline = true;
    audio.style.display = "none";
    audio.volume = currentVolumePercent() / 100;
    document.body.appendChild(audio);
    rtcRemoteAudios.set(peerId, audio);
  }
  return audio;
}

function removeRtcRemoteAudio(peerId) {
  const audio = rtcRemoteAudios.get(peerId);
  if (!audio) {
    return;
  }
  audio.srcObject = null;
  audio.remove();
  rtcRemoteAudios.delete(peerId);
}

async function ensureRtcAudioReady() {
  if (rtcLocalStream) {
    applyRtcMuteState();
    return;
  }

  rtcLocalStream = await navigator.mediaDevices.getUserMedia({
    audio: {
      channelCount: currentChannelCount(),
      ...buildAudioProcessingConstraints(),
    },
    video: false,
  });

  applyRtcMuteState();
}

function sendRtcSignal(to, data) {
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    return;
  }
  ws.send(
    JSON.stringify({
      type: "rtc_signal",
      to,
      data,
    })
  );
}

function getOrCreatePeerEntry(peerId) {
  let entry = rtcPeers.get(peerId);
  if (entry) {
    return entry;
  }

  const pc = new RTCPeerConnection({ iceServers: [] });
  entry = {
    pc,
    offerSent: false,
    pendingCandidates: [],
    dc: null,
  };
  rtcPeers.set(peerId, entry);

  if (rtcLocalStream) {
    for (const track of rtcLocalStream.getTracks()) {
      pc.addTrack(track, rtcLocalStream);
    }
    applyRtcSenderAudioParams(pc).catch(() => {});
  }

  pc.onicecandidate = (event) => {
    if (event.candidate) {
      sendRtcSignal(peerId, { candidate: event.candidate });
    }
  };

  pc.ontrack = (event) => {
    const audio = ensureRtcRemoteAudio(peerId);
    if (event.streams && event.streams[0]) {
      audio.srcObject = event.streams[0];
      return;
    }

    const stream = audio.srcObject instanceof MediaStream ? audio.srcObject : new MediaStream();
    stream.addTrack(event.track);
    audio.srcObject = stream;
  };

  pc.onconnectionstatechange = () => {
    if (pc.connectionState === "failed" || pc.connectionState === "closed") {
      closeRtcPeer(peerId);
    }
  };
  pc.ondatachannel = (event) => {
    const dc = event.channel;
    if (!dc) {
      return;
    }
    attachRtcDataChannel(peerId, entry, dc);
  };

  return entry;
}

function closeRtcPeer(peerId) {
  const entry = rtcPeers.get(peerId);
  if (entry) {
    if (entry.dc) {
      entry.dc.onmessage = null;
      entry.dc.onopen = null;
      entry.dc.onclose = null;
      entry.dc.onerror = null;
      entry.dc.close();
      entry.dc = null;
    }
    entry.pc.onicecandidate = null;
    entry.pc.ontrack = null;
    entry.pc.onconnectionstatechange = null;
    entry.pc.ondatachannel = null;
    entry.pc.close();
    rtcPeers.delete(peerId);
  }
  removeRtcRemoteAudio(peerId);
}

function shouldInitiateOffer(peerId) {
  return selfPeerId != null && selfPeerId < peerId;
}

async function ensureOfferToPeer(peerId) {
  const entry = getOrCreatePeerEntry(peerId);
  if (entry.offerSent) {
    return;
  }
  entry.offerSent = true;
  ensurePeerDataChannel(peerId, entry);

  const offer = await entry.pc.createOffer();
  await setLocalDescriptionWithOpusPrefs(entry.pc, offer);
  sendRtcSignal(peerId, { description: entry.pc.localDescription });
}

async function applyPendingIceCandidates(entry) {
  if (!entry.pc.remoteDescription) {
    return;
  }

  if (entry.pendingCandidates.length === 0) {
    return;
  }

  const pending = entry.pendingCandidates.splice(0, entry.pendingCandidates.length);
  for (const candidate of pending) {
    try {
      await entry.pc.addIceCandidate(candidate);
    } catch {}
  }
}

async function handleRtcSignalMessage(message) {
  if (transportMode !== TRANSPORT_WEBRTC) {
    return;
  }
  if (typeof message.from !== "string" || !message.data) {
    return;
  }
  if (typeof message.to === "string" && message.to !== selfPeerId) {
    return;
  }

  const peerId = message.from;
  const data = message.data;
  const entry = getOrCreatePeerEntry(peerId);

  if (data.description && typeof data.description.type === "string") {
    await entry.pc.setRemoteDescription(data.description);
    await applyPendingIceCandidates(entry);

    if (data.description.type === "offer") {
      const answer = await entry.pc.createAnswer();
      await setLocalDescriptionWithOpusPrefs(entry.pc, answer);
      sendRtcSignal(peerId, { description: entry.pc.localDescription });
    }
    return;
  }

  if (data.candidate) {
    try {
      await entry.pc.addIceCandidate(data.candidate);
    } catch {
      entry.pendingCandidates.push(data.candidate);
    }
  }
}

async function syncRtcPeers(peers) {
  if (transportMode !== TRANSPORT_WEBRTC) {
    return;
  }
  if (!selfPeerId) {
    return;
  }

  const wanted = new Map();
  for (const item of peers) {
    if (!item || typeof item.id !== "string" || typeof item.name !== "string") {
      continue;
    }
    if (item.id === selfPeerId) {
      continue;
    }
    wanted.set(item.id, item);
  }

  for (const peerId of rtcPeers.keys()) {
    if (!wanted.has(peerId)) {
      closeRtcPeer(peerId);
    }
  }

  for (const peerId of wanted.keys()) {
    getOrCreatePeerEntry(peerId);
    if (shouldInitiateOffer(peerId)) {
      try {
        await ensureOfferToPeer(peerId);
      } catch (err) {
        log(`发起 WebRTC 会话失败(${peerId.slice(0, 8)}): ${err.message ?? err}`);
      }
    }
  }
}

function buildAudioPacket(codec, payload) {
  const body = payload instanceof Uint8Array ? payload : new Uint8Array(payload);
  const out = new Uint8Array(AUDIO_PACKET_HEADER_BYTES + body.byteLength);
  out[0] = AUDIO_PACKET_MAGIC;
  out[1] = AUDIO_PACKET_VERSION;
  out[2] = codec;
  out[3] = 0;
  out.set(body, AUDIO_PACKET_HEADER_BYTES);
  return out.buffer;
}

function parseAudioPacket(buffer) {
  if (!(buffer instanceof ArrayBuffer)) {
    return null;
  }

  if (buffer.byteLength === FRAME_SIZE * 2) {
    return {
      codec: AUDIO_CODEC_PCM16,
      payload: buffer,
      legacy: true,
    };
  }

  if (buffer.byteLength < AUDIO_PACKET_HEADER_BYTES) {
    return null;
  }

  const view = new Uint8Array(buffer);
  if (view[0] !== AUDIO_PACKET_MAGIC || view[1] !== AUDIO_PACKET_VERSION) {
    return null;
  }

  const codec = view[2];
  if (codec !== AUDIO_CODEC_PCM16 && codec !== AUDIO_CODEC_OPUS) {
    return null;
  }

  const payload = view.slice(AUDIO_PACKET_HEADER_BYTES).buffer;
  if (codec === AUDIO_CODEC_PCM16 && payload.byteLength !== FRAME_SIZE * 2) {
    return null;
  }

  return {
    codec,
    payload,
    legacy: false,
  };
}

function pcm16ToFloat32(buffer) {
  const input = new Int16Array(buffer);
  const frame = new Float32Array(input.length);
  for (let i = 0; i < input.length; i += 1) {
    frame[i] = input[i] / 32768;
  }
  return frame;
}

function pushPlaybackFrame(frame) {
  if (!(frame instanceof Float32Array) || frame.length === 0) {
    return;
  }

  if (usingWorklet) {
    playbackNode.port.postMessage(frame, [frame.buffer]);
    return;
  }

  fallbackPlaybackQueue.push(frame);
  if (fallbackPlaybackQueue.length > PLAYBACK_QUEUE_MAX_FRAMES) {
    fallbackPlaybackQueue.splice(0, fallbackPlaybackQueue.length - PLAYBACK_QUEUE_MAX_FRAMES);
  }
}

function canSendAudioFrame() {
  if (!ws || ws.readyState !== WebSocket.OPEN || micMuted) {
    return false;
  }

  if (ws.bufferedAmount <= WS_MAX_BUFFERED_BYTES) {
    return true;
  }

  const now = Date.now();
  if (now - lastBackpressureLogAt >= BACKPRESSURE_LOG_INTERVAL_MS) {
    log(
      `上行拥塞，丢弃音频帧：buffered=${ws.bufferedAmount} bytes（阈值 ${WS_MAX_BUFFERED_BYTES}）`
    );
    lastBackpressureLogAt = now;
  }

  return false;
}

function resetCodecRuntime() {
  codecMode = "pcm";
  opusCaptureTimestampUs = 0;
  opusDecodeTimestampUs = 0;
}

function onOpusEncoded(chunk) {
  if (!canSendAudioFrame()) {
    return;
  }

  const payload = new Uint8Array(chunk.byteLength);
  chunk.copyTo(payload);
  const packet = buildAudioPacket(AUDIO_CODEC_OPUS, payload);
  ws.send(packet);
  markWsAudioSent(packet.byteLength);
}

function onOpusDecoded(audioData) {
  try {
    const frame = new Float32Array(audioData.numberOfFrames);
    audioData.copyTo(frame, { planeIndex: 0, format: "f32-planar" });
    pushPlaybackFrame(frame);
  } catch (err) {
    log(`Opus 解码输出失败: ${err.message ?? err}`);
  } finally {
    audioData.close();
  }
}

async function initPreferredCodec() {
  resetCodecRuntime();
  audioEncoder = null;
  audioDecoder = null;

  const hasWebCodecs =
    typeof AudioEncoder !== "undefined" &&
    typeof AudioDecoder !== "undefined" &&
    typeof AudioData !== "undefined" &&
    typeof EncodedAudioChunk !== "undefined";

  if (!hasWebCodecs) {
    log("浏览器不支持 WebCodecs，使用 PCM 传输");
    return;
  }

  if (currentChannelMode() === CHANNEL_MODE_STEREO) {
    log("WS 音频链路暂不支持立体声编码，已使用单声道 Opus");
  }

  const config = {
    codec: "opus",
    sampleRate: 48000,
    numberOfChannels: 1,
    bitrate: OPUS_BITRATE,
  };

  try {
    const [enc, dec] = await Promise.all([
      AudioEncoder.isConfigSupported(config),
      AudioDecoder.isConfigSupported(config),
    ]);

    if (!enc.supported || !dec.supported) {
      log("浏览器未声明支持 Opus 编解码，使用 PCM 传输");
      return;
    }

    audioDecoder = new AudioDecoder({
      output: onOpusDecoded,
      error: (err) => log(`Opus 解码器错误: ${err.message ?? err}`),
    });
    audioDecoder.configure(dec.config ?? config);

    audioEncoder = new AudioEncoder({
      output: onOpusEncoded,
      error: (err) => log(`Opus 编码器错误: ${err.message ?? err}`),
    });
    audioEncoder.configure(enc.config ?? config);

    codecMode = "opus";
    log(`已启用 Opus (${OPUS_BITRATE / 1000} kbps)`);
  } catch (err) {
    resetCodecRuntime();
    audioEncoder = null;
    audioDecoder = null;
    log(`启用 Opus 失败，回退 PCM: ${err.message}`);
  }
}

function sendCapturedPcmFrame(frameBuffer) {
  if (!(frameBuffer instanceof ArrayBuffer) || frameBuffer.byteLength !== FRAME_SIZE * 2) {
    return;
  }

  if (codecMode === "opus" && audioEncoder && audioEncoder.state === "configured") {
    if (!canSendAudioFrame()) {
      return;
    }

    if (audioEncoder.encodeQueueSize > OPUS_MAX_ENCODE_QUEUE) {
      return;
    }

    try {
      const audioData = new AudioData({
        format: "s16",
        sampleRate: 48000,
        numberOfFrames: FRAME_SIZE,
        numberOfChannels: 1,
        timestamp: opusCaptureTimestampUs,
        data: new Uint8Array(frameBuffer),
      });
      opusCaptureTimestampUs += OPUS_FRAME_DURATION_US;
      audioEncoder.encode(audioData);
      audioData.close();
      return;
    } catch (err) {
      log(`Opus 编码失败，降级 PCM: ${err.message}`);
      resetCodecRuntime();
      if (audioEncoder) {
        audioEncoder.close();
        audioEncoder = null;
      }
      if (audioDecoder) {
        audioDecoder.close();
        audioDecoder = null;
      }
    }
  }

  if (!canSendAudioFrame()) {
    return;
  }
  const packet = buildAudioPacket(AUDIO_CODEC_PCM16, frameBuffer);
  ws.send(packet);
  markWsAudioSent(packet.byteLength);
}

function handleIncomingAudio(buffer) {
  const packet = parseAudioPacket(buffer);
  if (!packet) {
    return;
  }

  if (packet.codec === AUDIO_CODEC_PCM16) {
    pushPlaybackFrame(pcm16ToFloat32(packet.payload));
    return;
  }

  if (!audioDecoder || audioDecoder.state !== "configured") {
    return;
  }

  try {
    const chunk = new EncodedAudioChunk({
      type: "key",
      timestamp: opusDecodeTimestampUs,
      data: new Uint8Array(packet.payload),
    });
    opusDecodeTimestampUs += OPUS_FRAME_DURATION_US;
    audioDecoder.decode(chunk);
  } catch (err) {
    log(`Opus 解码失败: ${err.message ?? err}`);
  }
}

function buildWsUrl() {
  const base = serverUrlInput.value.trim();
  const room = encodeURIComponent(currentRoom());
  const name = encodeURIComponent(currentName());
  return `${base}?room=${room}&name=${name}`;
}

function apiBaseFromWsUrl() {
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

  const parts = (url.pathname || "/").split("/").filter((item) => item.length > 0);
  if (parts.length > 0) {
    parts.pop();
  }
  const basePath = parts.length > 0 ? `/${parts.join("/")}` : "";
  return `${url.origin}${basePath}`;
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

function updateBitrateUi(txKbps, rxKbps) {
  if (txRateValueEl) {
    txRateValueEl.textContent = txKbps == null ? "-" : `${txKbps.toFixed(1)} kbps`;
  }
  if (rxRateValueEl) {
    rxRateValueEl.textContent = rxKbps == null ? "-" : `${rxKbps.toFixed(1)} kbps`;
  }
}

function resetBitrateState() {
  bitrateLastAtMs = 0;
  bitrateLastTxBytes = 0;
  bitrateLastRxBytes = 0;
  wsAudioTxBytesTotal = 0;
  wsAudioRxBytesTotal = 0;
  updateBitrateUi(null, null);
}

function markWsAudioSent(bytes) {
  if (transportMode !== TRANSPORT_WS_AUDIO || !Number.isFinite(bytes) || bytes <= 0) {
    return;
  }
  wsAudioTxBytesTotal += bytes;
}

function markWsAudioReceived(bytes) {
  if (transportMode !== TRANSPORT_WS_AUDIO || !Number.isFinite(bytes) || bytes <= 0) {
    return;
  }
  wsAudioRxBytesTotal += bytes;
}

async function readWebRtcAudioByteTotals() {
  let txBytes = 0;
  let rxBytes = 0;

  for (const { pc } of rtcPeers.values()) {
    let report;
    try {
      report = await pc.getStats();
    } catch {
      continue;
    }

    report.forEach((stat) => {
      const kind = stat.kind || stat.mediaType;
      if (kind !== "audio") {
        return;
      }

      if (stat.type === "outbound-rtp" && !stat.isRemote && Number.isFinite(stat.bytesSent)) {
        txBytes += stat.bytesSent;
      }

      if (stat.type === "inbound-rtp" && !stat.isRemote && Number.isFinite(stat.bytesReceived)) {
        rxBytes += stat.bytesReceived;
      }
    });
  }

  return { txBytes, rxBytes };
}

async function readCurrentAudioByteTotals() {
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    return { txBytes: 0, rxBytes: 0 };
  }

  if (transportMode === TRANSPORT_WEBRTC) {
    return readWebRtcAudioByteTotals();
  }

  return {
    txBytes: wsAudioTxBytesTotal,
    rxBytes: wsAudioRxBytesTotal,
  };
}

async function sampleBitrate() {
  if (bitrateSampling) {
    return;
  }
  bitrateSampling = true;

  try {
    const nowMs = Date.now();
    const totals = await readCurrentAudioByteTotals();

    if (bitrateLastAtMs === 0) {
      bitrateLastAtMs = nowMs;
      bitrateLastTxBytes = totals.txBytes;
      bitrateLastRxBytes = totals.rxBytes;
      updateBitrateUi(0, 0);
      return;
    }

    const elapsedSec = Math.max(0.001, (nowMs - bitrateLastAtMs) / 1000);
    const txDelta = Math.max(0, totals.txBytes - bitrateLastTxBytes);
    const rxDelta = Math.max(0, totals.rxBytes - bitrateLastRxBytes);
    const txKbps = (txDelta * 8) / 1000 / elapsedSec;
    const rxKbps = (rxDelta * 8) / 1000 / elapsedSec;

    updateBitrateUi(txKbps, rxKbps);
    bitrateLastAtMs = nowMs;
    bitrateLastTxBytes = totals.txBytes;
    bitrateLastRxBytes = totals.rxBytes;
  } finally {
    bitrateSampling = false;
  }
}

function startBitrateLoop() {
  stopBitrateLoop();
  sampleBitrate().catch(() => {});
  bitrateTimer = setInterval(() => {
    sampleBitrate().catch(() => {});
  }, BITRATE_SAMPLE_INTERVAL_MS);
}

function stopBitrateLoop() {
  if (bitrateTimer) {
    clearInterval(bitrateTimer);
    bitrateTimer = null;
  }
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

function sendWsPingProbe() {
  const seq = ++pingSeq;
  const now = Date.now();
  pingSent += 1;
  pingPending.set(seq, now);
  pingHistory.push({ seq, status: "pending" });
  trimPingHistory();

  ws.send(JSON.stringify({ type: "ping", seq, ts: now }));
}

function sendWebRtcPingProbe() {
  const channels = [];
  for (const entry of rtcPeers.values()) {
    if (entry.dc && entry.dc.readyState === "open") {
      channels.push(entry.dc);
    }
  }

  if (channels.length === 0) {
    updatePingStatsUi();
    return;
  }

  const now = Date.now();
  for (const dc of channels) {
    const seq = ++pingSeq;
    pingSent += 1;
    pingPending.set(seq, now);
    pingHistory.push({ seq, status: "pending" });
    dc.send(JSON.stringify({ type: "ping", seq, ts: now }));
  }
  trimPingHistory();
}

function sendPingProbe() {
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    return;
  }

  sweepPingTimeouts();
  if (transportMode === TRANSPORT_WEBRTC) {
    sendWebRtcPingProbe();
  } else {
    sendWsPingProbe();
  }
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
    return;
  }

  if (data.type === "welcome" && typeof data.client_id === "string") {
    selfPeerId = data.client_id;
    if (transportMode === TRANSPORT_WEBRTC) {
      syncRtcPeers(latestRoomPeers).catch((err) => {
        log(`同步 WebRTC 节点失败: ${err.message ?? err}`);
      });
    }
    return;
  }

  if (data.type === "room_peers" && typeof data.room === "string" && Array.isArray(data.peers)) {
    if (data.room !== currentRoom()) {
      return;
    }
    latestRoomPeers = data.peers
      .filter(
        (item) => item && typeof item.id === "string" && typeof item.name === "string"
      )
      .map((item) => ({ id: item.id, name: item.name }));

    if (transportMode === TRANSPORT_WEBRTC) {
      renderUsers(latestRoomPeers.map((item) => item.name));
      syncRtcPeers(latestRoomPeers).catch((err) => {
        log(`同步 WebRTC 房间失败: ${err.message ?? err}`);
      });
    }
    return;
  }

  if (data.type === "rtc_signal") {
    handleRtcSignalMessage(data).catch((err) => {
      log(`处理 WebRTC 信令失败: ${err.message ?? err}`);
    });
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
  const apiBase = apiBaseFromWsUrl();
  const room = currentRoom();

  try {
    const [roomsData, usersData] = await Promise.all([
      fetchJson(`${apiBase}/api/rooms`),
      fetchJson(`${apiBase}/api/rooms/${encodeURIComponent(room)}/users`),
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
      ...buildAudioProcessingConstraints(),
    },
    video: false,
  });

  const micSource = audioContext.createMediaStreamSource(stream);
  usingWorklet = Boolean(audioContext.audioWorklet && typeof AudioWorkletNode !== "undefined");

  playbackGain = audioContext.createGain();
  playbackGain.gain.value = currentVolumePercent() / 100;
  await initPreferredCodec();

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
      const frame = event.data;
      if (!(frame instanceof ArrayBuffer) || frame.byteLength !== FRAME_SIZE * 2) {
        return;
      }
      sendCapturedPcmFrame(frame);
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
          sendCapturedPcmFrame(pcm.buffer);
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

async function teardownRtc() {
  for (const peerId of [...rtcPeers.keys()]) {
    closeRtcPeer(peerId);
  }

  if (rtcLocalStream) {
    for (const track of rtcLocalStream.getTracks()) {
      track.stop();
    }
    rtcLocalStream = null;
  }

  selfPeerId = null;
  latestRoomPeers = [];
}

async function teardownMedia() {
  if (transportMode === TRANSPORT_WEBRTC) {
    await teardownRtc();
    return;
  }
  await teardownAudio();
}

function bindWsEvents() {
  ws.binaryType = "arraybuffer";

  ws.onopen = () => {
    setStatus("已连接，语音中");
    setConnectedUi(true);
    lastBackpressureLogAt = 0;
    startBitrateLoop();
    startPingLoop();
    refreshPresence().catch(() => {});
    if (transportMode === TRANSPORT_WEBRTC) {
      log(
        `WebSocket 已连接，音频通道: WebRTC (Opus 目标 ${OPUS_BITRATE / 1000} kbps, 软下限 ${OPUS_MIN_SOFT_BITRATE / 1000} kbps, ${currentChannelLabel()}, ${currentAudioProcessingLabel()})`
      );
      log("Ping 探测已切换为 WebRTC 数据通道真实链路 RTT");
    } else {
      log(`WebSocket 已连接，当前编码: ${codecMode.toUpperCase()}`);
    }
  };

  ws.onclose = () => {
    setStatus("已断开");
    setConnectedUi(false);
    stopBitrateLoop();
    resetBitrateState();
    stopPingLoop();
    log("WebSocket 已断开");
    teardownMedia().catch((err) => log(`释放音频资源失败: ${err.message}`));
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

    if (transportMode !== TRANSPORT_WS_AUDIO) {
      return;
    }
    markWsAudioReceived(event.data.byteLength);
    handleIncomingAudio(event.data);
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
  resetCodecRuntime();
  if (audioEncoder) {
    audioEncoder.close();
    audioEncoder = null;
  }
  if (audioDecoder) {
    audioDecoder.close();
    audioDecoder = null;
  }
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
  resetBitrateState();
  setStatus("初始化音频...");
  transportMode = shouldUseWebRtcTransport() ? TRANSPORT_WEBRTC : TRANSPORT_WS_AUDIO;
  if (transportMode === TRANSPORT_WEBRTC) {
    await ensureRtcAudioReady();
    log(
      `当前浏览器使用 WebRTC 音频通道（Opus 目标 ${OPUS_BITRATE / 1000} kbps, 软下限 ${OPUS_MIN_SOFT_BITRATE / 1000} kbps, ${currentChannelLabel()}, ${currentAudioProcessingLabel()}）`
    );
  } else {
    await ensureAudio();
  }
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
    stopBitrateLoop();
    resetBitrateState();
    stopPingLoop();
    await teardownMedia().catch(() => {});
  }
});

disconnectBtn.addEventListener("click", () => {
  disconnect();
});

muteBtn.addEventListener("click", () => {
  micMuted = !micMuted;
  applyRtcMuteState();
  muteBtn.textContent = micMuted ? "取消静音上行" : "静音上行";
  log(micMuted ? "已静音上行音频" : "已恢复上行音频");
});

volumeInput.addEventListener("change", () => {
  const v = currentVolumePercent();
  volumeInput.value = String(v);
  if (playbackGain) {
    playbackGain.gain.value = v / 100;
  }
  updateRtcAudioVolume();
});

if (channelModeInput) {
  channelModeInput.addEventListener("change", () => {
    const mode = currentChannelMode();
    channelModeInput.value = mode;
    if (ws && ws.readyState === WebSocket.OPEN) {
      log(`Opus 声道已切到${currentChannelLabel()}，请断开后重连使其生效`);
    }
  });
}

const onAudioProcessingModeChanged = async () => {
  normalizeAudioProcessingUiValues();
  if (ws && ws.readyState === WebSocket.OPEN) {
    await applyAudioProcessingSettingsRealtime();
    return;
  }
  log(`已设置音频处理参数: ${currentAudioProcessingLabel()}（下次采集生效）`);
};

if (aecModeInput) {
  aecModeInput.addEventListener("change", () => {
    onAudioProcessingModeChanged().catch((err) => {
      log(`更新 AEC 设置失败: ${err.message ?? err}`);
    });
  });
}

if (nsModeInput) {
  nsModeInput.addEventListener("change", () => {
    onAudioProcessingModeChanged().catch((err) => {
      log(`更新 NS 设置失败: ${err.message ?? err}`);
    });
  });
}

if (agcModeInput) {
  agcModeInput.addEventListener("change", () => {
    onAudioProcessingModeChanged().catch((err) => {
      log(`更新 AGC 设置失败: ${err.message ?? err}`);
    });
  });
}

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
resetBitrateState();
normalizeAudioProcessingUiValues();
startPresenceLoop();
