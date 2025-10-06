from concurrent.futures import ThreadPoolExecutor
import subprocess
import threading
import os
import socket
import time
import sys
import argparse

parser = argparse.ArgumentParser(description="Stream a playlist of files via FFmpeg + UDP relay")
parser.add_argument(
    "playlist",
    nargs="+",  # one or more arguments
    help="List of files to stream"
)
args = parser.parse_args()

playlist = args.playlist
print("[main] Playlist:", playlist)

# Optional environment variables, fallback to defaults
multicast_ip = os.environ.get("TARGET_MULTICAST", "239.100.0.1")
multicast_port = int(os.environ.get("TARGET_PORT", 1234))

TARGET = (multicast_ip, multicast_port)

print(f"[config] Using TARGET: {TARGET}")

PIPE_A = "/tmp/pipe_a"
PIPE_B = "/tmp/pipe_b"

PACKET_SIZE = 1316

CURRENT_PIPE = PIPE_A
NEXT_PIPE = PIPE_B

DATA_PIPES = []

# Pipe erstellen
for p in [PIPE_A, PIPE_B]:
    if not os.path.exists(p):
        os.mkfifo(p)

def relay_pipe(stop_event):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
    
    time.sleep(0.5)

    while not stop_event.is_set():
        if not DATA_PIPES:
            time.sleep(0.5)
            continue 
        pipe_path = DATA_PIPES.pop(0)
        
        print(f"[relay] Sending from {pipe_path} to {TARGET} ...")

        with open(pipe_path, "rb", buffering=0) as pipe:
            tries = 0
            max_tries = 10
            while not stop_event.is_set():
                data = pipe.read(PACKET_SIZE)
                if not data:
                    time.sleep(0.01)
                    tries += 1
                    if tries > max_tries:
                        break
                    continue
                sock.sendto(data, TARGET)

    print("[relay] Stopped")

def start_ffmpeg(input_file, output_pipe):
    output_pipe = "file:" + output_pipe
    cmd = [
    "ffmpeg",
    "-re",
    "-y",  # Overwrite pipe if needed
    "-i", input_file,
    "-vf", "scale=w=1920:h=1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2:black",
    "-c:v", "libx264",
    "-preset", "veryfast",
    "-tune", "zerolatency",
    "-c:a", "ac3",
    "-b:a", "192k",
    "-ac", "2",
    "-f", "mpegts",
    output_pipe
]
    print(f"[ffmpeg] Starting for {input_file} → {output_pipe}")
    return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def get_duration(input_file):
    """Return duration in seconds using ffprobe"""
    cmd = [
        "ffprobe",
        "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        input_file
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    try:
        return float(result.stdout.strip())
    except ValueError:
        print(f"[ffprobe] Could not get duration for {input_file}, default 10s")
        return 10.0

def stream_file(file_path, pipe_path):
    """Thread target: start ffmpeg for given file and pipe"""
    proc = start_ffmpeg(file_path, pipe_path)
    DATA_PIPES.append(pipe_path)
    proc.wait()
    print(f"[thread] Finished streaming: {file_path}")


def main():
    stop_event = threading.Event()
    relay_thread = threading.Thread(target=relay_pipe, args=(stop_event,))
    relay_thread.start()

    with ThreadPoolExecutor(max_workers=2) as executor:
        for next_file in playlist:
            global CURRENT_PIPE, NEXT_PIPE
            duration = get_duration(next_file)
            print(f"\n[main] Now streaming: {next_file} (duration {duration:.2f}s)")
            
            # Submit the ffmpeg streaming job to the threadpool
            executor.submit(stream_file, next_file, CURRENT_PIPE)
            
            CURRENT_PIPE, NEXT_PIPE = NEXT_PIPE, CURRENT_PIPE
            
            # Wait until 2 seconds before the video would end
            wait_time = max(0, duration - 2)
            print(f"[main] Waiting {wait_time:.2f}s before starting next stream")
            time.sleep(wait_time)
    
    print("[main] Playlist complete — stopping relay...")
    stop_event.set()
    relay_thread.join(timeout=2)
    print("[main] All done.")


if __name__ == "__main__":
    main()
