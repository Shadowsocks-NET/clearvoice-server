const FRAME_SIZE = 480;

class CaptureProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this.buffer = new Float32Array(FRAME_SIZE);
    this.buffered = 0;
  }

  process(inputs) {
    const channel = inputs[0]?.[0];
    if (!channel) {
      return true;
    }

    for (let i = 0; i < channel.length; i += 1) {
      this.buffer[this.buffered] = channel[i];
      this.buffered += 1;

      if (this.buffered === FRAME_SIZE) {
        const pcm = new Int16Array(FRAME_SIZE);
        for (let j = 0; j < FRAME_SIZE; j += 1) {
          const s = Math.max(-1, Math.min(1, this.buffer[j]));
          pcm[j] = s < 0 ? s * 32768 : s * 32767;
        }
        this.port.postMessage(pcm.buffer, [pcm.buffer]);
        this.buffered = 0;
      }
    }

    return true;
  }
}

registerProcessor("capture-processor", CaptureProcessor);
