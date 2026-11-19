from .base import ProcessStatus


def determine_multi_process_status(
    *statuses: ProcessStatus,
) -> ProcessStatus:
    """Combine the statuses of several processes into a single status.

    `UNKNOWN` statuses are ignored. If all remaining statuses are the same,
    that status is returned. Otherwise, in order of precedence: any
    `ERRORED` gives `ERRORED`; any `STOPPING` or `STOPPED` gives
    `STOPPING`; any `STARTING` gives `STARTING`; a mix of only `RUNNING`
    and `WAITING` gives `RUNNING`; and a mix including `INITIALISED` gives
    `STARTING`.

    Args:
        *statuses: The statuses to combine.

    Returns:
        the combined status, or `UNKNOWN` if no known statuses are given.
    """
    status_set = set(statuses)
    status_set.discard(ProcessStatus.UNKNOWN)

    if len(status_set) == 0:
        return ProcessStatus.UNKNOWN

    if len(status_set) == 1:
        return status_set.pop()

    if any(status == ProcessStatus.ERRORED for status in status_set):
        return ProcessStatus.ERRORED

    if any(status == ProcessStatus.STOPPING for status in status_set):
        return ProcessStatus.STOPPING

    if any(status == ProcessStatus.STOPPED for status in status_set):
        return ProcessStatus.STOPPING

    if any(status == ProcessStatus.STARTING for status in status_set):
        return ProcessStatus.STARTING

    if all(
        status == ProcessStatus.RUNNING or status == ProcessStatus.WAITING
        for status in status_set
    ):
        return ProcessStatus.RUNNING

    if all(
        status == ProcessStatus.INITIALISED
        or status == ProcessStatus.WAITING
        or status == ProcessStatus.RUNNING
        for status in status_set
    ):
        return ProcessStatus.STARTING

    if any(status == ProcessStatus.RUNNING for status in status_set):
        return ProcessStatus.RUNNING

    if any(status == ProcessStatus.WAITING for status in status_set):
        return ProcessStatus.WAITING

    return ProcessStatus.INITIALISED
