
import json
import os
import tempfile
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()


QUEUE_DATA_FILE = os.getenv("QUEUE_DATA_FILE", "queue.json")


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def load_queue():
    """Load jobs from disk, or initialize an empty queue."""
    if not os.path.exists(QUEUE_DATA_FILE):
        return {"jobs": {}}

    try:
        with open(QUEUE_DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, dict) or "jobs" not in data:
            raise ValueError("Invalid queue file structure")

        return data

    except (json.JSONDecodeError, ValueError) as e:
        print(f"Error loading queue: {e}")
        raise


def save_queue(queue_data):
    """Automically save queue data to disk."""
    directory = os.path.dirname(os.path.abspath(QUEUE_DATA_FILE))

    fd, temp_path = tempfile.mkstemp(
        dir=directory,
        prefix="queue_",
        suffix=".tmp"
    )

    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(queue_data, f, indent=2)

        os.replace(temp_path, QUEUE_DATA_FILE)

    except Exception:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise


def get_jobs():
    """Return all jobs."""
    return list(queue_data["jobs"].values())


def get_job(thread_id):
    """Return a job by its Discord forum thread ID."""
    return queue_data["jobs"].get(str(thread_id))


def add_job(job):
    """Add a job if it doesn't already exist."""
    thread_id = str(job["thread_id"])

    if thread_id in queue_data["jobs"]:
        return False

    queue_data["jobs"][thread_id] = job
    save_queue(queue_data)
    return True


def update_job(thread_id, **changes):
    """Update fields on an existing job."""
    job = get_job(thread_id)

    if job is None:
        return False

    job.update(changes)
    job["updated_at"] = now_iso()

    save_queue(queue_data)
    return True


def cancel_job(thread_id):
    """Cancel a queued job."""
    job = get_job(thread_id)

    if job is None:
        return False

    if job["status"] != "queued":
        return False

    update_job(
        thread_id,
        status="cancelled",
        cancelled_at=now_iso()
    )

    return True


def set_priority(thread_id, priority=True):
    """Set or remove priority on a queued job."""
    job = get_job(thread_id)

    if job is None or job["status"] != "queued":
        return False

    update_job(thread_id, priority=priority)
    return True


def get_active_jobs():
    """Return queued jobs in priority/FIFO order."""
    jobs = [
        job for job in get_jobs()
        if job["status"] == "queued"
    ]

    return sorted(
        jobs,
        key=lambda job: (
            not job.get("priority", False),
            job.get("submitted_at", "")
        )
    )


def get_queue_position(thread_id):
    """Return the 1-based queue position, or None."""
    jobs = get_active_jobs()

    for position, job in enumerate(jobs, start=1):
        if str(job["thread_id"]) == str(thread_id):
            return position

    return None


def get_queue_data():
    """Return queue data for other modules."""
    return queue_data


# Load persisted data when this module is imported.
queue_data = load_queue()
