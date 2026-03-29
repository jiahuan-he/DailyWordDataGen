"""Compare multiple MiniMax TTS voices with test sentences."""

import os
import sys
import httpx

API_BASE = "https://api.minimax.io/v1"
API_KEY = os.environ.get("MINIMAX_API_KEY")

VOICE = "English_Wiselady"
SPEEDS = [1.0, 1.1, 1.2, 1.3]

SENTENCES = [
    "Children sense genuine affection; they cannot be fooled by hollow words.",
    "The investigation exposed an avaricious corporate culture where ethics yielded to quarterly profits.",
]

os.makedirs("test_output/voice_comparison", exist_ok=True)

for speed in SPEEDS:
    for i, sentence in enumerate(SENTENCES, 1):
        label = f"british_lady_wise_speed{speed}_{i}"
        output_path = f"test_output/voice_comparison/{label}.mp3"
        print(f"Generating: speed={speed} / sentence {i}...")

        resp = httpx.post(
            f"{API_BASE}/t2a_v2",
            headers={
                "Authorization": f"Bearer {API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": "speech-2.8-hd",
                "text": sentence,
                "voice_setting": {"voice_id": VOICE, "speed": speed},
                "audio_setting": {"format": "mp3", "sample_rate": 24000, "bitrate": 128000, "channel": 1},
                "language_boost": "en",
            },
            timeout=60,
        )
        data = resp.json()
        status = data.get("base_resp", {}).get("status_code", -1)
        if status != 0:
            print(f"  ERROR: {data.get('base_resp', {}).get('status_msg')}")
            continue

        audio = bytes.fromhex(data["data"]["audio"])
        with open(output_path, "wb") as f:
            f.write(audio)

        extra = data.get("extra_info", {})
        print(f"  Saved: {output_path} ({extra.get('audio_length')}ms, {len(audio)} bytes)")

print("\nDone! Files in test_output/voice_comparison/")
