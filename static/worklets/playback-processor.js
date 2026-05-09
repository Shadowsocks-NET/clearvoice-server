class PlaybackProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this.queue = [];
    this.currentFrame = null;
    this.currentOffset = 0;

    this.port.onmessage = (event) => {
      const buffer = event.data;
      if (!(buffer instanceof ArrayBuffer)) {
        return;
      }

      const input = new Int16Array(buffer);
      const frame = new Float32Array(input.length);
      for (let i = 0; i < input.length; i += 1) {
        frame[i] = input[i] / 32768;
      }
      this.queue.push(frame);
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
