const PLAYBACK_QUEUE_MAX_FRAMES = 24;

class PlaybackProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this.queue = [];
    this.currentFrame = null;
    this.currentOffset = 0;

    this.port.onmessage = (event) => {
      const payload = event.data;
      let frame = null;

      if (payload instanceof Float32Array) {
        frame = payload;
      } else if (payload instanceof ArrayBuffer) {
        const input = new Int16Array(payload);
        frame = new Float32Array(input.length);
        for (let i = 0; i < input.length; i += 1) {
          frame[i] = input[i] / 32768;
        }
      }

      if (!frame || frame.length === 0) {
        return;
      }

      this.queue.push(frame);
      if (this.queue.length > PLAYBACK_QUEUE_MAX_FRAMES) {
        this.queue.splice(0, this.queue.length - PLAYBACK_QUEUE_MAX_FRAMES);
      }
    };
  }

  process(_inputs, outputs) {
    const output = outputs[0]?.[0];
    if (!output) {
      return true;
    }

    output.fill(0);
    let written = 0;

    while (written < output.length) {
      if (this.currentFrame === null) {
        this.currentFrame = this.queue.shift() ?? null;
        this.currentOffset = 0;
        if (this.currentFrame === null) {
          break;
        }
      }

      const remainOut = output.length - written;
      const remainFrame = this.currentFrame.length - this.currentOffset;
      const copyLen = Math.min(remainOut, remainFrame);

      output.set(
        this.currentFrame.subarray(this.currentOffset, this.currentOffset + copyLen),
        written
      );

      written += copyLen;
      this.currentOffset += copyLen;

      if (this.currentOffset >= this.currentFrame.length) {
        this.currentFrame = null;
        this.currentOffset = 0;
      }
    }

    return true;
  }
}

registerProcessor("playback-processor", PlaybackProcessor);
