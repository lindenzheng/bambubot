
import json
import os
import tempfile
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

VALID_STATUSES = {
    "queued",
    "cancelled",
    "printing",
    "completed",
    "failed",
}


def normalize_status(status):
    """Normalize job statuses for consistent comparisons."""
    if status is None:
        return None

    status = str(status).strip().lower()

    if status not in VALID_STATUSES:
        return status

    return status

# QUEUE_DATA_FILE = os.getenv("QUEUE_DATA_FILE", "queue.json")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

configured_queue_file = os.getenv("QUEUE_DATA_FILE", "queue.json")

if os.path.isabs(configured_queue_file):
    QUEUE_DATA_FILE = configured_queue_file
else:
    QUEUE_DATA_FILE = os.path.join(
        BASE_DIR,
        configured_queue_file,
    )

print(f"[QUEUE] Data file: {QUEUE_DATA_FILE}")


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def load_queue():
    """Load jobs from disk, or initialize an empty queue."""
    if not os.path.exists(QUEUE_DATA_FILE):
        return {"jobs": {}}

    try:
        with open(QUEUE_DATA_FILE, "r", encoding="utf-8") as f:
            contents = f.read()

        # Treat empty files as a new, empty queue.
        if not contents.strip():
            print("Queue file is empty. Initializing empty queue.")
            return {"jobs": {}}

        data = json.loads(contents)

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

def requeue_job(thread_id):
    """Requeue a cancelled job."""
    job = get_job(thread_id)

    if job is None:
        return False

    if normalize_status(job.get("status")) != "cancelled":
        return False

    return update_job(
        thread_id,
        status="queued",
        submitted_at=now_iso(),
        cancelled_at=None,
    )

def update_job(thread_id, **changes):
    """Update fields on an existing job."""
    job = get_job(thread_id)

    if job is None:
        return False

    if "status" in changes:
        changes["status"] = normalize_status(changes["status"])

    job.update(changes)
    job["updated_at"] = now_iso()

    save_queue(queue_data)
    return True


def cancel_job(thread_id):
    """Cancel a queued job."""
    job = get_job(thread_id)

    if job is None:
        return False

    if normalize_status(job.get("status")) != "queued":
        return False

    return update_job(
        thread_id,
        status="cancelled",
        cancelled_at=now_iso(),
    )

def delete_job(thread_id):
    """Permanently remove a job from the queue database."""
    thread_id = str(thread_id)

    if thread_id not in queue_data["jobs"]:
        return False

    del queue_data["jobs"][thread_id]
    save_queue(queue_data)

    return True

def set_priority(thread_id, priority=True):
    """Set or remove priority on a queued job."""
    job = get_job(thread_id)

    if job is None:
        return False

    if normalize_status(job.get("status")) != "queued":
        return False

    return update_job(
        thread_id,
        priority=bool(priority),
    )

def start_job(thread_id, printer_name):
    """Mark a queued job as started on a printer."""
    job = get_job(thread_id)

    if job is None:
        return False

    if normalize_status(job.get("status")) != "queued":
        return False

    return update_job(
        thread_id,
        status="printing",
        printer=str(printer_name),
        started_at=now_iso(),
        completed_at=None,
        priority=False,
    )

def complete_job(thread_id, printer_name=None):
    """Mark an assigned printing job as completed."""
    job = get_job(thread_id)

    if job is None:
        return False

    if normalize_status(job.get("status")) != "printing":
        return False

    if (
        printer_name is not None
        and str(job.get("printer", "")).casefold()
        != str(printer_name).casefold()
    ):
        return False

    return update_job(
        thread_id,
        status="completed",
        completed_at=now_iso(),
    )

def get_active_jobs():
    """Return queued jobs in priority/FIFO order."""
    jobs = [
        job for job in get_jobs()
        if normalize_status(job.get("status")) == "queued"
    ]

    return sorted(
        jobs,
        key=lambda job: (
            not bool(job.get("priority", False)),
            job.get("submitted_at", ""),
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

def subscribe_job(thread_id, user_id):
    """Subscribe a user to job notifications."""
    job = get_job(thread_id)

    if job is None:
        return False

    user_id = str(user_id)

    subscribers = job.setdefault("subscribers", [])

    if user_id in subscribers:
        return False

    subscribers.append(user_id)

    return update_job(
        thread_id,
        subscribers=subscribers,
    )


def unsubscribe_job(thread_id, user_id):
    """Unsubscribe a user from job notifications."""
    job = get_job(thread_id)

    if job is None:
        return False

    user_id = str(user_id)

    subscribers = job.get("subscribers", [])

    if user_id not in subscribers:
        return False

    subscribers.remove(user_id)

    return update_job(
        thread_id,
        subscribers=subscribers,
    )

# Load persisted data when this module is imported.
queue_data = load_queue()
print(
    f"[QUEUE] Loaded {len(queue_data['jobs'])} jobs "
    f"from {QUEUE_DATA_FILE}"
)