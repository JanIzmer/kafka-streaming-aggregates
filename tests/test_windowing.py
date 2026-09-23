from __future__ import annotations

from datetime import UTC, datetime, timedelta

from orders_stream.models import window_start_for
from orders_stream.windowing import Outcome, WindowManager
from tests.factories import BASE_TIME, at, event


def test_windows_are_aligned_to_the_epoch_not_to_the_first_event():
    """Two processors, and a replay, must agree on bucket boundaries."""
    start = window_start_for(datetime(2026, 9, 14, 12, 0, 37, tzinfo=UTC), 60)

    assert start == datetime(2026, 9, 14, 12, 0, 0, tzinfo=UTC)
    assert window_start_for(datetime(2026, 9, 14, 12, 0, 59, 999999, tzinfo=UTC), 60) == start
    assert window_start_for(datetime(2026, 9, 14, 12, 1, 0, tzinfo=UTC), 60) != start


def test_naive_timestamps_are_treated_as_utc():
    naive = datetime(2026, 9, 14, 12, 0, 30)

    assert window_start_for(naive, 60) == datetime(2026, 9, 14, 12, 0, tzinfo=UTC)


def test_events_in_the_same_minute_share_a_window(windows: WindowManager):
    windows.add(event(occurred_at=at(5)), now=at(5))
    windows.add(event(occurred_at=at(45)), now=at(45))

    assert windows.open_window_count == 1
    aggregate = windows.dirty_aggregates()[0]
    assert aggregate.events_total == 2
    assert aggregate.gross_amount_minor == 2000


def test_different_merchants_get_different_windows(windows: WindowManager):
    windows.add(event(merchant_id="m_a", occurred_at=at(5)), now=at(5))
    windows.add(event(merchant_id="m_b", occurred_at=at(5)), now=at(5))

    assert windows.open_window_count == 2


def test_watermark_trails_the_newest_event_by_allowed_lateness(windows: WindowManager):
    windows.add(event(occurred_at=at(300)), now=at(300))

    assert windows.watermark == at(300) - timedelta(seconds=120)


def test_watermark_never_moves_backwards(windows: WindowManager):
    windows.add(event(occurred_at=at(600)), now=at(600))
    high = windows.watermark

    windows.add(event(occurred_at=at(400)), now=at(600))

    assert windows.watermark == high


def test_event_types_map_to_the_right_measures(windows: WindowManager):
    for event_type, amount in (
        ("order_placed", None),
        ("order_paid", 1500),
        ("order_cancelled", None),
        ("order_refunded", 500),
    ):
        windows.add(
            event(event_type=event_type, amount_minor=amount, occurred_at=at(10)), now=at(10)
        )

    aggregate = windows.dirty_aggregates()[0]
    assert (aggregate.orders_placed, aggregate.orders_paid) == (1, 1)
    assert (aggregate.orders_cancelled, aggregate.orders_refunded) == (1, 1)
    assert aggregate.gross_amount_minor == 1500
    assert aggregate.refunded_amount_minor == 500
    assert aggregate.net_amount_minor == 1000


def test_distinct_users_are_counted_per_window(windows: WindowManager):
    for user in ("u1", "u2", "u1"):
        windows.add(event(user_id=user, occurred_at=at(10)), now=at(10))

    assert windows.dirty_aggregates()[0].distinct_users == 2


def test_window_closes_once_the_watermark_passes_its_end(windows: WindowManager):
    windows.add(event(occurred_at=at(10)), now=at(10))

    # Window [12:00, 12:01). Watermark reaches 12:01 when event time hits 12:03.
    windows.add(event(occurred_at=at(185)), now=at(185))
    closed = windows.close_expired()

    assert len(closed) == 1
    assert closed[0].key.window_start == BASE_TIME
    assert closed[0].is_closed is True


def test_closing_evicts_state_so_memory_stays_bounded(windows: WindowManager):
    windows.add(event(occurred_at=at(10)), now=at(10))
    windows.add(event(occurred_at=at(185)), now=at(185))

    windows.close_expired()

    assert windows.open_window_count == 1  # only the newer window remains
    assert windows.tracked_user_count == 1


def test_an_open_window_is_not_closed_early(windows: WindowManager):
    windows.add(event(occurred_at=at(10)), now=at(10))
    windows.add(event(occurred_at=at(90)), now=at(90))

    assert windows.close_expired() == []
    assert windows.open_window_count == 2


def test_idle_watermark_advances_when_the_stream_goes_quiet(windows: WindowManager):
    """A merchant that stops trading must not pin its window open forever."""
    windows.add(event(occurred_at=at(10)), now=at(10))
    assert windows.close_expired() == []

    advanced = windows.advance_on_idle(now=at(400))

    assert advanced is True
    assert len(windows.close_expired()) == 1


def test_idle_advance_never_rewinds_the_watermark(windows: WindowManager):
    windows.add(event(occurred_at=at(600)), now=at(600))
    high = windows.watermark

    assert windows.advance_on_idle(now=at(100)) is False
    assert windows.watermark == high


def test_future_skew_is_refused_and_does_not_move_the_watermark(windows: WindowManager):
    windows.add(event(occurred_at=at(10)), now=at(10))
    before = windows.watermark

    outcome = windows.add(event(occurred_at=at(3600)), now=at(10))

    assert outcome is Outcome.FUTURE_SKEW
    assert windows.watermark == before
    assert windows.future_skew_count == 1


def test_deltas_are_the_difference_since_the_last_flush(windows: WindowManager):
    windows.add(event(occurred_at=at(10)), now=at(10))
    first = windows.pending_deltas()
    assert first[0].events_total == 1

    windows.mark_flushed()
    windows.add(event(occurred_at=at(20)), now=at(20))
    second = windows.pending_deltas()

    assert second[0].events_total == 1  # the delta, not the running total of 2


def test_nothing_is_pending_right_after_a_flush(windows: WindowManager):
    windows.add(event(occurred_at=at(10)), now=at(10))
    windows.mark_flushed()

    assert windows.pending_deltas() == []


def test_a_failed_flush_keeps_its_changes_pending(windows: WindowManager):
    """mark_flushed is only called after the transaction commits, so an
    exception in between must leave the delta intact."""
    windows.add(event(occurred_at=at(10)), now=at(10))

    first = windows.pending_deltas()
    second = windows.pending_deltas()  # flush raised; nothing was snapshotted

    assert first[0].events_total == second[0].events_total == 1


def test_new_users_in_a_delta_are_only_the_ones_not_yet_written(windows: WindowManager):
    windows.add(event(user_id="u1", occurred_at=at(10)), now=at(10))
    windows.mark_flushed()
    windows.add(event(user_id="u1", occurred_at=at(20)), now=at(20))
    windows.add(event(user_id="u2", occurred_at=at(20)), now=at(20))

    assert windows.pending_deltas()[0].new_users == ("u2",)
