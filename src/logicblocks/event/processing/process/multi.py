from .base import ProcessStatus


def determine_multi_process_status(
    *statuses: ProcessStatus,
) -> ProcessStatus:
    if any(status == ProcessStatus.ERRORED for status in statuses):
        return ProcessStatus.ERRORED

    if all(status == ProcessStatus.STOPPED for status in statuses):
        return ProcessStatus.STOPPED

    if any(status == ProcessStatus.STOPPING for status in statuses):
        return ProcessStatus.STOPPING

    if any(status == ProcessStatus.STOPPED for status in statuses):
        return ProcessStatus.STOPPING

    if any(status == ProcessStatus.STARTING for status in statuses):
        return ProcessStatus.STARTING

    if all(status == ProcessStatus.INITIALISED for status in statuses):
        return ProcessStatus.INITIALISED

    if all(status == ProcessStatus.WAITING for status in statuses):
        return ProcessStatus.WAITING

    if all(
        status == ProcessStatus.RUNNING or status == ProcessStatus.WAITING
        for status in statuses
    ):
        return ProcessStatus.RUNNING

    if all(
        status == ProcessStatus.INITIALISED
        or status == ProcessStatus.WAITING
        or status == ProcessStatus.RUNNING
        for status in statuses
    ):
        return ProcessStatus.STARTING

    if any(status == ProcessStatus.RUNNING for status in statuses):
        return ProcessStatus.RUNNING

    if any(status == ProcessStatus.WAITING for status in statuses):
        return ProcessStatus.WAITING

    return ProcessStatus.INITIALISED
