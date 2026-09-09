"""generate_note is one call per run: a cache write there is pure cost.

Every cadence composes exactly once, and the meeting-data block is rebuilt
from scratch each period, so a cache_control marker on it buys nothing and
charges 1.25x the plain input rate to write a cache nobody reads. The system
prompt is small and shared across runs, so its marker stays.
"""

import types

import generate_notes


class _Capture:
    def __init__(self):
        self.kwargs = None

    def create(self, **kwargs):
        self.kwargs = kwargs
        return types.SimpleNamespace(
            content=[types.SimpleNamespace(text="# note")],
            usage=types.SimpleNamespace(cache_read_input_tokens=0,
                                        cache_creation_input_tokens=0),
        )


def test_meeting_data_block_is_not_cache_written(monkeypatch):
    capture = _Capture()
    monkeypatch.setattr(generate_notes, "_get_client",
                        lambda: types.SimpleNamespace(messages=capture))

    generate_notes.generate_note("instructions\nMEETING DATA:\ncorpus", 100)

    blocks = capture.kwargs["messages"][0]["content"]
    assert [b["text"] for b in blocks] == ["instructions\nMEETING DATA:\n", "corpus"]
    assert not any("cache_control" in b for b in blocks)
