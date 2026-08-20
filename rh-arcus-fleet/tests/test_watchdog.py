from core.watchdog import should_crash_on_silence, silent_for_s


def test_veto_counts_as_liveness():
    now = 1_000.0
    # last send 90s ago, but a veto 5s ago — healthy (skipped toxic flow)
    silent = silent_for_s(now, last_send_ts=now - 90, last_veto_ts=now - 5)
    assert silent == 5
    assert not should_crash_on_silence(
        running=True,
        paused=False,
        has_active_workers=True,
        has_resting=False,
        silent_for=silent,
    )


def test_resting_quotes_are_not_a_dead_fleet():
    assert not should_crash_on_silence(
        running=True,
        paused=False,
        has_active_workers=True,
        has_resting=True,
        silent_for=120,
    )


def test_paused_or_no_workers_never_crash():
    assert not should_crash_on_silence(
        running=True, paused=True, has_active_workers=True, has_resting=False, silent_for=120
    )
    assert not should_crash_on_silence(
        running=True, paused=False, has_active_workers=False, has_resting=False, silent_for=120
    )


def test_true_silence_crashes():
    assert should_crash_on_silence(
        running=True,
        paused=False,
        has_active_workers=True,
        has_resting=False,
        silent_for=61,
    )
