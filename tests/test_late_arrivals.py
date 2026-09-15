"""The behaviour this project exists to get right."""

from __future__ import annotations

from orders_stream.windowing import Outcome, WindowManager
from tests.factories import BASE_TIME, at, event


def test_an_in_order_event_is_on_time(windows: WindowManager):
    assert windows.add(event(occurred_at=at(10)), now=at(10)) is Outcome.ON_TIME


def test_out_of_order_but_ahead_of_the_watermark_is_still_on_time(windows: WindowManager):
    """Out-of-order is normal, not late. Only the watermark decides."""
    windows.add(event(occurred_at=at(100)), now=at(100))

    assert windows.add(event(occurred_at=at(60)), now=at(100)) is Outcome.ON_TIME


def test_behind_the_watermark_but_window_open_is_accepted_as_late(windows: WindowManager):
    windows.add(event(occurred_at=at(300)), now=at(300))  # watermark -> 12:03

    outcome = windows.add(event(occurred_at=at(170)), now=at(300))

    assert outcome is Outcome.LATE_ACCEPTED
    assert windows.late_count == 1


def test_a_late_event_restates_the_window_and_bumps_the_revision(windows: WindowManager):
    windows.add(event(occurred_at=at(170), amount_minor=1000), now=at(170))
    windows.add(event(occurred_at=at(300)), now=at(300))

    windows.add(event(occurred_at=at(175), amount_minor=500), now=at(300))

    aggregate = next(
        state.aggregate
        for key, state in windows.windows.items()
        if key.window_start == at(120)
    )
    assert aggregate.gross_amount_minor == 1500
    assert aggregate.late_events_applied == 1
    assert aggregate.revision == 2


def test_an_event_for_a_closed_window_is_too_late(windows: WindowManager):
    windows.add(event(occurred_at=at(10)), now=at(10))
    windows.add(event(occurred_at=at(300)), now=at(300))  # watermark -> 12:03

    outcome = windows.add(event(occurred_at=at(20)), now=at(300))

    assert outcome is Outcome.TOO_LATE
    assert windows.too_late_count == 1


def test_a_too_late_event_does_not_resurrect_the_window(windows: WindowManager):
    """Re-opening a closed window would silently change a number a dashboard
    has already shown, with no record that it happened."""
    windows.add(event(occurred_at=at(10)), now=at(10))
    windows.add(event(occurred_at=at(300)), now=at(300))
    windows.close_expired()
    open_before = windows.open_window_count

    windows.add(event(occurred_at=at(20)), now=at(300))

    assert windows.open_window_count == open_before
    assert not any(key.window_start == BASE_TIME for key in windows.windows)


def test_lateness_is_judged_against_the_watermark_before_the_event(windows: WindowManager):
    """An event must not be judged by a watermark it set itself - otherwise the
    newest event is always 'on time' and nothing is ever late."""
    windows.add(event(occurred_at=at(600)), now=at(600))

    assert windows.add(event(occurred_at=at(540)), now=at(600)) is Outcome.LATE_ACCEPTED


def test_allowed_lateness_is_the_only_knob_that_changes_the_verdict():
    generous = WindowManager(window_size_seconds=60, allowed_lateness_seconds=600)
    strict = WindowManager(window_size_seconds=60, allowed_lateness_seconds=60)

    for manager in (generous, strict):
        manager.add(event(occurred_at=at(10)), now=at(10))
        manager.add(event(occurred_at=at(300)), now=at(300))

    assert generous.add(event(occurred_at=at(20)), now=at(300)) is Outcome.LATE_ACCEPTED
    assert strict.add(event(occurred_at=at(20)), now=at(300)) is Outcome.TOO_LATE


def test_state_size_is_bounded_by_lateness_over_window_size():
    """With 120s lateness and 60s windows a merchant holds at most a handful of
    windows, however long the stream runs."""
    manager = WindowManager(window_size_seconds=60, allowed_lateness_seconds=120)

    for minute in range(120):
        manager.add(event(occurred_at=at(minute * 60)), now=at(minute * 60))
        manager.close_expired()

    assert manager.open_window_count <= 4


def test_replaying_the_same_events_in_a_different_order_gives_the_same_totals():
    forward = WindowManager(window_size_seconds=60, allowed_lateness_seconds=600)
    backward = WindowManager(window_size_seconds=60, allowed_lateness_seconds=600)
    events = [event(event_id=f"e{i}", occurred_at=at(i * 5), amount_minor=100 + i) for i in range(10)]

    for item in events:
        forward.add(item, now=at(60))
    for item in reversed(events):
        backward.add(item, now=at(60))

    def totals(manager):
        return sorted(
            (key.window_start, state.aggregate.gross_amount_minor, state.aggregate.events_total)
            for key, state in manager.windows.items()
        )

    assert totals(forward) == totals(backward)
