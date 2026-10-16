import subprocess
import re

VIDEO = "../../../sample-media/big_buck_bunny_1080p_h264.mov"
THRESHOLD = 0.4
WINDOW = 60        # Seconds around the target position
MIN_DURATION = 3600
TARGET_POSITIONS = [0.3, 0.5, 0.7]

def get_duration(video):
    cmd = ["ffprobe", "-v", "error", "-show_entries", "format=duration",
           "-of", "default=noprint_wrappers=1:nokey=1", video]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return float(result.stdout.strip())

def get_scene_changes_segment(video, start, length, threshold):
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "info",
        "-ss", str(start), "-t", str(length),
        "-i", video,
        "-vf", f"select='gt(scene,{threshold})',showinfo",
        "-f", "null", "-"
    ]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    times = []
    for line in result.stdout.splitlines():
        if "pts_time:" in line:
            m = re.search(r"pts_time:([0-9\.]+)", line)
            if m:
                times.append(start + float(m.group(1)))
    return times

def find_ads(video):
    duration = get_duration(video)
    print(f"🎬 Duration: {duration/60:.1f} min")

    if duration < MIN_DURATION:
        print("📺 Shorter than 60 min → no ads.")
        return []

    candidates = []
    for pos in TARGET_POSITIONS:
        center = duration * pos
        start = max(center - WINDOW, 0)
        cuts = get_scene_changes_segment(video, start, WINDOW * 2, THRESHOLD)
        if cuts:
            best = min(cuts, key=lambda c: abs(c - center))
            candidates.append(round(best, 2))
            print(f"🔍 Range {pos*100:.0f}% → {len(cuts)} cuts, best spot: {best:.2f}s")
        else:
            print(f"⚠️ Range {pos*100:.0f}%: no cuts found.")
    return candidates

if __name__ == "__main__":
    positions = find_ads(VIDEO)
    if positions:
        print("\n📺 Recommended ad breaks:")
        for c in positions:
            print(f" → {c:.2f}s")
        
        # 🔁 Optional preview mode using ffplay
        for c in positions:
            start = max(c - 1, 0)
            length = 2
            mark_time = c - start  # Marker position within the clip
            print(f"\n▶️  Preview around {c:.2f}s (marker at {mark_time:.2f}s)")

            cmd = [
                "ffplay", "-hide_banner", "-autoexit",
                "-ss", str(start), "-t", str(length),
                "-vf", f"drawbox=x=w/2-2:y=0:w=40:h=ih:color=red@0.8:enable='between(t,{mark_time},{mark_time+1})'",
                VIDEO
            ]
            subprocess.run(cmd)
