from .base import ProcessStatus


def determine_multi_process_status(
    *statuses: ProcessStatus,
) -> ProcessStatus:
    if any(status == ProcessStatus.ERRORED for status in statuses):
        return ProcessStatus.ERRORED

    if all(status == ProcessStatus.INITIALISED for status in statuses):
        return ProcessStatus.INITIALISED

    if all(status == ProcessStatus.STOPPED for status in statuses):
        return ProcessStatus.STOPPED

    if all(status == ProcessStatus.RUNNING for status in statuses):
        return ProcessStatus.RUNNING

    if all(
        status == ProcessStatus.INITIALISED or status == ProcessStatus.RUNNING
        for status in statuses
    ):
        return ProcessStatus.STARTING

    if any(status == ProcessStatus.STOPPED for status in statuses):
        return ProcessStatus.STOPPING

    if any(status == ProcessStatus.RUNNING for status in statuses):
        return ProcessStatus.RUNNING

    if any(status == ProcessStatus.STARTING for status in statuses):
        return ProcessStatus.STARTING

    return ProcessStatus.INITIALISED
