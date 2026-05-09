const FRAME_SIZE = 480;

const serverUrlInput = document.getElementById("serverUrl");
const roomInput = document.getElementById("room");
const nameInput = document.getElementById("name");
const volumeInput = document.getElementById("volume");
const connectBtn = document.getElementById("connectBtn");
const disconnectBtn = document.getElementById("disconnectBtn");
const muteBtn = document.getElementById("muteBtn");
const statusEl = document.getElementById("status");
const logEl = document.getElementById("log");

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

function buildWsUrl() {
  const base = serverUrlInput.value.trim();
  const room = encodeURIComponent(roomInput.value.trim() || "main");
  const name = encodeURIComponent(nameInput.value.trim() || "anonymous");
  return `${base}?room=${room}&name=${name}`;
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
      autoGainControl: false
    },
    video: false
  });

  const micSource = audioContext.createMediaStreamSource(stream);
  usingWorklet = Boolean(audioContext.audioWorklet && typeof AudioWorkletNode !== "undefined");

  playbackGain = audioContext.createGain();
  playbackGain.gain.value = Number(volumeInput.value) || 1;

  if (usingWorklet) {
    await audioContext.audioWorklet.addModule("/worklets/capture-processor.js");
    await audioContext.audioWorklet.addModule("/worklets/playback-processor.js");

    captureNode = new AudioWorkletNode(audioContext, "capture-processor", {
      numberOfInputs: 1,
      numberOfOutputs: 0,
      channelCount: 1
    });
    micSource.connect(captureNode);

    playbackNode = new AudioWorkletNode(audioContext, "playback-processor", {
      numberOfInputs: 0,
      numberOfOutputs: 1,
      outputChannelCount: [1]
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

    // 兼容模式下必须接到输出链路，处理回调才会持续触发。
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
    log("WebSocket 已连接");
  };

  ws.onclose = () => {
    setStatus("已断开");
    setConnectedUi(false);
    log("WebSocket 已断开");
    teardownAudio().catch((err) => log(`释放音频资源失败: ${err.message}`));
  };

  ws.onerror = () => {
    log("WebSocket 错误");
  };

  ws.onmessage = (event) => {
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
  const v = Math.max(0, Math.min(1, Number(volumeInput.value) || 1));
  volumeInput.value = String(v);
  if (playbackGain) {
    playbackGain.gain.value = v;
  }
});
