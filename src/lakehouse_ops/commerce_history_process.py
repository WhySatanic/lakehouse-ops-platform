"""Bound the Linux history-profile controller group; Docker stages are separate."""
from __future__ import annotations

import os
import signal
import subprocess

_POSIX = os.name == "posix"
_KILL = getattr(signal, "SIGKILL", 9)


def run_profile_process(command, **options):
    """Use the experiment's file-backed output; retain the original timeout on failure."""
    if not _POSIX:
        return subprocess.run(command, **options)
    timeout = options.pop("timeout", None)
    check = options.pop("check", False)
    process = subprocess.Popen(command, start_new_session=True, **options)
    try:
        code = process.wait(timeout=timeout)
    except subprocess.TimeoutExpired as error:
        group = {"process_group_id": process.pid, "signal": "SIGKILL", "status": "unconfirmed",
                 "scope": "profile_controller_group_not_Docker_stages_or_commit_outcome"}
        signal_outcome = None
        # The timed-out leader is not reaped before signalling its newly created group.
        try:
            os.killpg(process.pid, _KILL)
            signal_outcome = "kill_requested"
        except ProcessLookupError:
            signal_outcome = "absent"
        except OSError as kill_error:
            group["signal_error_type"] = type(kill_error).__name__
        try:
            process.wait(timeout=10)
        except (OSError, subprocess.TimeoutExpired) as wait_error:
            group["wait_error_type"] = type(wait_error).__name__
        else:
            if signal_outcome:
                group["status"] = signal_outcome + "_leader_reaped"
        error.profile_process_group = group
        raise
    if check and code:
        raise subprocess.CalledProcessError(code, command)
    return subprocess.CompletedProcess(command, code)


def run_experiment_command(command, **options):
    if command[1:3] == ["-m", "lakehouse_ops.commerce_pipeline_profile"]:
        return run_profile_process(command, **options)
    return subprocess.run(command, **options)
