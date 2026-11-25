class DoorbellAudio {
    constructor(setStatus, volume) {
        this.setStatus = setStatus;
        this.volume = volume;
        this.context = null;
        this.gain = null;
        this.socket = null;
        this.sources = new Set();
        this.nextTime = 0;
    }

    async start() {
        if (this.socket) return;
        const AudioContext = window.AudioContext || window.webkitAudioContext;
        if (!AudioContext) {
            this.setStatus("This browser cannot play live audio.");
            return;
        }

        try {
            if (!this.context) {
                this.context = new AudioContext();
                this.gain = this.context.createGain();
                this.gain.gain.value = this.volume;
                this.gain.connect(this.context.destination);
            }
            const address = new URL("./audio", document.baseURI);
            address.protocol = address.protocol === "https:" ? "wss:" : "ws:";
            address.searchParams.set("token", document.querySelector('meta[name="valore-token"]').content);
            const socket = new WebSocket(address);
            this.socket = socket;
            socket.binaryType = "arraybuffer";
            let rate = 0;
            let playing = false;
            this.setStatus("Waiting for sound…");
            socket.onmessage = ({data}) => {
                if (this.socket !== socket) return;
                if (typeof data === "string") {
                    rate = JSON.parse(data).sample_rate;
                    return;
                }
                if (rate && data.byteLength >= 2 && this.context.state === "running") {
                    this.play(data, rate);
                    if (!playing) {
                        playing = true;
                        this.setStatus("Sound on");
                    }
                }
            };
            socket.onclose = () => {
                if (this.socket === socket) {
                    this.socket = null;
                    this.clearPlayback();
                    this.setStatus("Sound connection closed");
                }
            };
            await this.context.resume();
        } catch {
            this.stop();
            this.setStatus("Sound unavailable");
        }
    }

    play(bytes, rate) {
        const context = this.context;
        const samples = new DataView(bytes);
        const frames = Math.floor(bytes.byteLength / 2);
        const buffer = context.createBuffer(1, frames, rate);
        const channel = buffer.getChannelData(0);
        for (let index = 0; index < frames; index++) {
            channel[index] = samples.getInt16(index * 2, true) / 32768;
        }

        const now = context.currentTime;
        if (this.nextTime < now) this.nextTime = now + 0.07;
        if (this.nextTime > now + 0.35) {
            this.clearPlayback();
            this.nextTime = now + 0.07;
        }
        const source = context.createBufferSource();
        source.buffer = buffer;
        source.connect(this.gain);
        source.onended = () => {
            this.sources.delete(source);
            source.disconnect();
        };
        this.sources.add(source);
        source.start(this.nextTime);
        this.nextTime += buffer.duration;
    }

    setVolume(volume) {
        this.volume = volume;
        if (this.gain) {
            this.gain.gain.setTargetAtTime(volume, this.context.currentTime, 0.02);
        }
    }

    clearPlayback() {
        for (const source of this.sources) {
            source.onended = null;
            source.stop();
            source.disconnect();
        }
        this.sources.clear();
        this.nextTime = 0;
    }

    stop() {
        const socket = this.socket;
        this.socket = null;
        if (socket) socket.close();
        this.clearPlayback();
        this.setStatus("");
    }
}
