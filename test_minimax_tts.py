"""Test script for MiniMax TTS API - generates pronunciation audio for DailyWord."""

import argparse
import glob
import json
import os
import sys

import httpx

API_BASE = "https://api.minimax.io/v1"


def get_api_key():
    key = os.environ.get("MINIMAX_API_KEY")
    if not key:
        print("Error: MINIMAX_API_KEY environment variable not set")
        sys.exit(1)
    return key


def list_voices(api_key):
    """List available system voices."""
    resp = httpx.post(
        f"{API_BASE}/get_voice",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={"voice_type": "system"},
    )
    resp.raise_for_status()
    data = resp.json()

    if data.get("base_resp", {}).get("status_code", -1) != 0:
        print(f"API error: {data.get('base_resp', {}).get('status_msg', 'unknown')}")
        sys.exit(1)

    voices = data.get("system_voice", [])
    print(f"Found {len(voices)} system voices:\n")
    for v in voices:
        vid = v.get("voice_id", "")
        name = v.get("voice_name", "")
        desc = v.get("description", "")
        print(f"  {vid}")
        if name:
            print(f"    name: {name}")
        if desc:
            print(f"    desc: {desc}")
        print()


def load_word_data(word):
    """Load word data from final_data_v4."""
    word_dir = os.path.join("final_data_v4", word)
    if not os.path.isdir(word_dir):
        print(f"Error: No data found for word '{word}' in final_data_v4/")
        sys.exit(1)

    json_files = sorted(glob.glob(os.path.join(word_dir, "*.json")))
    if not json_files:
        print(f"Error: No JSON files found in {word_dir}/")
        sys.exit(1)

    with open(json_files[-1]) as f:
        return json.load(f)


def generate_audio(api_key, voice_id, text, output_path):
    """Call MiniMax TTS API and save WAV audio."""
    payload = {
        "model": "speech-2.8-hd",
        "text": text,
        "voice_setting": {
            "voice_id": voice_id,
            "speed": 1.0,
        },
        "audio_setting": {
            "format": "wav",
            "sample_rate": 24000,
            "channel": 1,
        },
        "language_boost": "en",
    }

    print(f"Generating audio for: {text[:80]}{'...' if len(text) > 80 else ''}")
    print(f"Voice: {voice_id}, Model: speech-2.8-hd")

    resp = httpx.post(
        f"{API_BASE}/t2a_v2",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()

    status_code = data.get("base_resp", {}).get("status_code", -1)
    if status_code != 0:
        print(f"API error (code {status_code}): {data.get('base_resp', {}).get('status_msg', 'unknown')}")
        sys.exit(1)

    audio_hex = data.get("data", {}).get("audio")
    if not audio_hex:
        print("Error: No audio data in response")
        sys.exit(1)

    audio_bytes = bytes.fromhex(audio_hex)

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "wb") as f:
        f.write(audio_bytes)

    extra = data.get("extra_info", {})
    print(f"Saved: {output_path}")
    print(f"  Duration: {extra.get('audio_length', '?')}ms")
    print(f"  Size: {len(audio_bytes)} bytes")
    print(f"  Sample rate: {extra.get('audio_sample_rate', '?')}")
    print(f"  Characters used: {extra.get('usage_characters', '?')}")


def main():
    parser = argparse.ArgumentParser(description="Test MiniMax TTS API")
    parser.add_argument("--list-voices", action="store_true", help="List available system voices")
    parser.add_argument("--voice-id", type=str, help="Voice ID to use for generation")
    parser.add_argument("--text", type=str, help="Custom text to synthesize")
    parser.add_argument("--word", type=str, help="Word from final_data_v4 (generates word + first example sentence)")
    parser.add_argument("--output", type=str, help="Output file path (default: test_output/<word_or_text>.wav)")
    args = parser.parse_args()

    api_key = get_api_key()

    if args.list_voices:
        list_voices(api_key)
        return

    if not args.voice_id:
        print("Error: --voice-id is required for audio generation")
        print("Run with --list-voices to see available voices")
        sys.exit(1)

    if args.word:
        word_data = load_word_data(args.word)
        word = word_data["word"]
        sentence = word_data["examples"][0]["sentence"]
        text = f"{word}. {sentence}"
        output = args.output or f"test_output/{word}.wav"
    elif args.text:
        text = args.text
        output = args.output or "test_output/custom_tts.wav"
    else:
        print("Error: Provide --word or --text")
        sys.exit(1)

    generate_audio(api_key, args.voice_id, text, output)


if __name__ == "__main__":
    main()
