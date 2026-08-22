"""
Makerspace 3D Printer Discord Bot
Monitors Prusa (via PrusaLink HTTP), Bambu Lab (via MQTT), and
Moonraker/Klipper printers like Snapmaker U1 (via HTTP) and
displays real-time status in Discord.

Features:
    - Status message that auto-updates (interval configurable in printers.json)
    - Supports both Prusa and Bambu Lab printers from one config file
    - Notification messages when a print completes or fails, including print duration
    - Mentions the Discord user if their username is in the filename
      (format: filename_@username.gcode)
    - Custom status title and refresh interval configurable in printers.json

Requirements:
    pip install discord.py requests python-dotenv paho-mqtt

Setup:
    1. Create a .env file with your DISCORD_TOKEN and channel IDs
    2. Edit printers.json to add your printers (type: "prusa" or "bambu")
    3. Run: python3 3dprinter_discord_bot.py
"""

import os
import json
import asyncio
import traceback
import time as time_module
from datetime import datetime, timezone

import aiohttp
import discord
from discord.ext import tasks
import requests
from dotenv import load_dotenv

import ssl
import paho.mqtt.client as mqtt

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
STATUS_CHANNEL_ID = int(os.getenv("STATUS_CHANNEL_ID", 0))
NOTIFICATION_CHANNEL_ID = int(os.getenv("NOTIFICATION_CHANNEL_ID", 0))

# How often to poll printers (in seconds)
POLL_INTERVAL = 30

# Path to the printer config file
PRINTERS_CONFIG = "printers.json"


def load_config():
    """
    Load configuration from printers.json config file.
    Supports two formats:
      - Legacy: a plain list of printer dicts (for backwards compatibility)
      - New: a dict with top-level settings and a "printers" key

    Returns (printers, poll_interval, status_title).
    """
    try:
        with open(PRINTERS_CONFIG, "r") as f:
            data = json.load(f)

        # Support legacy format (plain list)
        if isinstance(data, list):
            printers = data
            poll_interval = 30
            status_title = "3D Printer Status"
        else:
            printers = data.get("printers", [])
            poll_interval = data.get("refresh_interval_seconds", 30)
            status_title = data.get("status_title", "3D Printer Status")

        print(f"Loaded {len(printers)} printer(s) from {PRINTERS_CONFIG}")
        print(f"Refresh interval: {poll_interval}s | Status title: \"{status_title}\"")
        return printers, poll_interval, status_title

    except FileNotFoundError:
        print(f"Error: {PRINTERS_CONFIG} not found. Create it with your printer info.")
        print('Example format:')
        print('{')
        print('  "status_title": "My Makerspace 3D Printer Status",')
        print('  "refresh_interval_seconds": 30,')
        print('  "printers": [')
        print('    {"name": "Printer Name", "type": "prusa/bambu", "ip": "YOUR_IP", "api_key": "YOUR_KEY"}')
        print('  ]')
        print('}')
        exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: {PRINTERS_CONFIG} contains invalid JSON: {e}")
        exit(1)


# ---------------------------------------------------------------------------
# Bambu Lab MQTT Client
# ---------------------------------------------------------------------------


class BambuMQTTClient:
    """
    Lightweight MQTT client for monitoring Bambu Lab printers.
    Connects via TLS, subscribes to status reports, and caches the latest data.
    Uses raw paho-mqtt — no bambulabs-api dependency.

    Only sends 'pushall' on connect (configurable). Does NOT send 'get_version'
    or 'get_history', which can trigger "MQTT Command verification failed"
    errors on newer firmware.
    """

    def __init__(self, ip, access_code, serial, pushall_on_connect=True):
        self.ip = ip
        self.serial = serial
        self.access_code = access_code
        self.pushall_on_connect = pushall_on_connect

        self.report_topic = f"device/{serial}/report"
        self.request_topic = f"device/{serial}/request"

        # Cached printer data — merged from incoming MQTT messages
        self._data = {}
        self._last_update = None

        self._client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            protocol=mqtt.MQTTv311,
        )
        self._client.username_pw_set("bblp", access_code)
        self._client.tls_set(tls_version=ssl.PROTOCOL_TLS, cert_reqs=ssl.CERT_NONE)
        self._client.tls_insecure_set(True)
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            client.subscribe(self.report_topic)
            if self.pushall_on_connect:
                client.publish(
                    self.request_topic,
                    json.dumps({"pushing": {"command": "pushall"}}),
                )

    def _on_message(self, client, userdata, msg):
        try:
            doc = json.loads(msg.payload)
            for k, v in doc.items():
                if k not in self._data:
                    self._data[k] = {}
                self._data[k].update(v)
            self._last_update = time_module.time()
        except (json.JSONDecodeError, AttributeError):
            pass

    def connect(self):
        self._client.connect(self.ip, 8883, 60)
        self._client.loop_start()

    def disconnect(self):
        self._client.loop_stop()
        self._client.disconnect()

    @property
    def has_data(self):
        return bool(self._data)

    @property
    def is_stale(self):
        """True if no MQTT message received in the last 60 seconds."""
        if self._last_update is None:
            return True
        return (time_module.time() - self._last_update) > 60

    def get_print_field(self, key, default=None):
        """Read a field from the cached 'print' status data."""
        return self._data.get("print", {}).get(key, default)

    def get_hms_errors(self):
        """
        Parse HMS (Health Management System) error codes from the cached data.
        Returns a list of dicts with 'code' (formatted string), 'url' (wiki link),
        and 'module' (human-readable subsystem name).

        HMS data comes as: [{"attr": <int>, "code": <int>}, ...]
        - attr is a 32-bit int encoding module_id, module_no, part_id, part_no
        - code is a 32-bit int encoding alert_level and error_code_id
        - Together they form the HMS code: XXXX-XXXX-XXXX-XXXX
        """
        # First two hex digits of the HMS code identify the subsystem
        MODULE_NAMES = {
            "03": "Heatbed/Motion",
            "05": "Main Board",
            "07": "AMS",
            "08": "Tool Head",
            "0C": "Camera/Lidar",
            "10": "Bambu Studio",
            "11": "Bambu Handy",
            "12": "AMS Lite",
            "18": "AMS HT",
        }

        hms_list = self._data.get("print", {}).get("hms", [])
        errors = []
        for item in hms_list:
            try:
                attr = item.get("attr", 0)
                code = item.get("code", 0)
                attr_hex = f"{attr:08X}"
                code_hex = f"{code:08X}"
                hms_code = f"{attr_hex[0:4]}-{attr_hex[4:8]}-{code_hex[0:4]}-{code_hex[4:8]}"
                wiki_url = f"https://wiki.bambulab.com/en/x1/troubleshooting/hmscode/{attr_hex[0:4]}_{attr_hex[4:8]}_{code_hex[0:4]}_{code_hex[4:8]}"
                module = MODULE_NAMES.get(attr_hex[0:2].upper(), "Printer")
                errors.append({"code": hms_code, "url": wiki_url, "module": module})
            except (ValueError, TypeError):
                continue
        return errors


# ---------------------------------------------------------------------------
# Bambu Lab Connections
# ---------------------------------------------------------------------------

# Stores connected Bambu printer instances: {"printer_name": BambuMQTTClient}
bambu_connections = {}


def connect_bambu_printers(printers):
    """
    Connect to all Bambu printers in the config.
    Each connection runs MQTT in the background, caching status updates.
    """
    for printer in printers:
        if printer.get("type") != "bambu":
            continue

        name = printer["name"]
        ip = printer["ip"]
        serial = printer["serial"]
        access_code = printer["access_code"]

        print(f"Connecting to Bambu printer: {name} at {ip}...")
        try:
            client = BambuMQTTClient(ip, access_code, serial)
            client.connect()
            bambu_connections[name] = client
            print(f"  Connected to {name}")
        except Exception as e:
            print(f"  Failed to connect to {name}: {e}")

    if bambu_connections:
        # Wait for initial MQTT data to arrive
        print("Waiting for Bambu MQTT data...")
        time_module.sleep(3)


def disconnect_bambu_printers():
    """Disconnect all Bambu printer MQTT connections."""
    for name, client in bambu_connections.items():
        try:
            client.disconnect()
            print(f"Disconnected from {name}")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# PrusaLink API Functions
# ---------------------------------------------------------------------------


def get_printer_status(ip, api_key, auth_type="api_key", username="maker"):
    """
    Query a printer's PrusaLink API for its current status.
    Returns a dictionary with the printer's state and job info.

    auth_type: "api_key" uses X-Api-Key header (Prusa Mini/Mini+)
               "digest"  uses HTTP Digest auth (MK3S with RPi Zero)
    username:  Username for digest auth (default: "maker")
    """
    base_url = f"http://{ip}"

    # Set up authentication based on printer type
    if auth_type == "digest":
        from requests.auth import HTTPDigestAuth
        auth = HTTPDigestAuth(username, api_key)
        headers = {}
    else:
        auth = None
        headers = {"X-Api-Key": api_key}

    try:
        # Get combined status (printer state + job progress)
        status_response = requests.get(
            f"{base_url}/api/v1/status", headers=headers, auth=auth, timeout=10
        )
        status_response.raise_for_status()
        status_data = status_response.json()

        printer_state = status_data.get("printer", {}).get("state", "UNKNOWN")
        job_data = status_data.get("job", {})

        result = {
            "state": printer_state,
            "progress": job_data.get("progress"),
            "time_remaining": job_data.get("time_remaining"),
            "time_printing": job_data.get("time_printing"),
        }

        # If printing, also grab the job endpoint for the file name
        if printer_state == "PRINTING":
            try:
                job_response = requests.get(
                    f"{base_url}/api/v1/job", headers=headers, auth=auth, timeout=10
                )
                job_response.raise_for_status()
                result["job_data"] = job_response.json()
            except Exception:
                result["job_data"] = None

        return result

    except requests.exceptions.ConnectionError:
        return {"state": "OFFLINE"}
    except requests.exceptions.Timeout:
        return {"state": "OFFLINE"}
    except Exception as e:
        print(f"Error polling {ip}: {e}")
        return {"state": "UNKNOWN"}


# ---------------------------------------------------------------------------
# Bambu Lab Status Function
# ---------------------------------------------------------------------------


def get_bambu_status(printer_name):
    """
    Read the cached status from a connected Bambu printer.
    Returns a dictionary in the same format as get_printer_status().
    """
    client = bambu_connections.get(printer_name)
    if client is None:
        return {"state": "OFFLINE"}

    try:
        if not client.has_data or client.is_stale:
            return {"state": "OFFLINE"}

        state_str = client.get_print_field("gcode_state", "UNKNOWN")
        percentage = client.get_print_field("mc_percent")
        time_remaining_min = client.get_print_field("mc_remaining_time")
        # subtask_name is preferred (X1 series returns project name here);
        # fall back to gcode_file (A1 series returns full filename here)
        file_name = client.get_print_field("subtask_name") or client.get_print_field("gcode_file", "")
        current_layer = int(client.get_print_field("layer_num", 0))
        total_layers = int(client.get_print_field("total_layer_num", 0))

        # Normalize Bambu states to match Prusa conventions
        state_map = {
            "IDLE": "IDLE",
            "RUNNING": "PRINTING",
            "PREPARE": "PRINTING",
            "PAUSE": "PAUSED",
            "FINISH": "IDLE",       # Treat FINISH as IDLE for display
            "FAILED": "ERROR",
            "UNKNOWN": "UNKNOWN",
        }

        normalized_state = state_map.get(state_str, state_str)

        # Check for FINISH -> send notification but display as IDLE
        # We pass the raw state so the notification logic can detect completion
        raw_state = state_str

        # Convert time from minutes to seconds for format_time()
        time_remaining_sec = (time_remaining_min * 60) if time_remaining_min is not None else None

        result = {
            "state": normalized_state,
            "raw_bambu_state": raw_state,
            "progress": percentage,
            "time_remaining": time_remaining_sec,
            "hms_errors": client.get_hms_errors(),
            "print_error": int(client.get_print_field("print_error", 0)),
        }

        # If printing, include file info in job_data format to match Prusa
        if normalized_state == "PRINTING" and file_name:
            result["job_data"] = {
                "file": {"display_name": file_name}
            }
            result["layers"] = f"{current_layer}/{total_layers}"

        return result

    except Exception as e:
        print(f"Error reading Bambu printer {printer_name}: {e}")
        return {"state": "UNKNOWN"}


# ---------------------------------------------------------------------------
# Moonraker (Klipper) Status Function — Snapmaker U1, etc.
# ---------------------------------------------------------------------------


def get_moonraker_status(ip):
    """
    Poll a Moonraker-based printer (Snapmaker U1, or any Klipper printer)
    via its HTTP API. Returns a dictionary in the same format as the other
    status functions.

    No authentication required by default.
    """
    try:
        response = requests.get(
            f"http://{ip}/printer/objects/query?print_stats&display_status&virtual_sdcard",
            timeout=5,
        )
        response.raise_for_status()
        data = response.json()

        status = data.get("result", {}).get("status", {})
        ps = status.get("print_stats", {})
        ds = status.get("display_status", {})
        vs = status.get("virtual_sdcard", {})

        state_str = ps.get("state", "standby")
        file_name = ps.get("filename", "")
        error_message = ps.get("message", "")
        # display_status.progress uses M73 from the slicer; some slicers
        # (including Snapmaker's) don't send M73, so fall back to
        # virtual_sdcard.progress which tracks file position
        progress_raw = ds.get("progress", 0) or vs.get("progress", 0)
        progress = progress_raw * 100  # 0.0-1.0 -> 0-100
        print_duration = ps.get("print_duration", 0)  # seconds, excludes paused time
        info = ps.get("info", {})
        current_layer = info.get("current_layer")
        total_layers = info.get("total_layer")

        # Normalize Moonraker/Klipper states to match our conventions
        state_map = {
            "standby": "IDLE",
            "printing": "PRINTING",
            "paused": "PAUSED",
            "complete": "IDLE",
            "cancelled": "IDLE",
            "error": "ERROR",
        }

        normalized_state = state_map.get(state_str, "UNKNOWN")
        raw_state = state_str

        result = {
            "state": normalized_state,
            "raw_moonraker_state": raw_state,
            "progress": progress,
            "time_remaining": None,  # Moonraker doesn't provide ETA natively
            "time_printing": print_duration,
            "error_message": error_message if normalized_state == "ERROR" else None,
        }

        # If printing, include file info in job_data format
        if normalized_state == "PRINTING" and file_name:
            result["job_data"] = {
                "file": {"display_name": file_name}
            }
            if current_layer is not None and total_layers is not None:
                result["layers"] = f"{current_layer}/{total_layers}"

            # Estimate time remaining from progress and elapsed time
            if progress > 0:
                estimated_total = print_duration / (progress / 100)
                result["time_remaining"] = estimated_total - print_duration

        return result

    except requests.exceptions.ConnectionError:
        return {"state": "OFFLINE"}
    except requests.exceptions.Timeout:
        return {"state": "OFFLINE"}
    except Exception as e:
        print(f"Error polling Moonraker printer at {ip}: {e}")
        return {"state": "UNKNOWN"}


# ---------------------------------------------------------------------------
# Unified Printer Polling
# ---------------------------------------------------------------------------


def poll_printer(printer):
    """
    Poll a single printer regardless of type.
    Returns a status dictionary in a common format.
    """
    printer_type = printer.get("type", "prusa")

    if printer_type == "bambu":
        return get_bambu_status(printer["name"])
    elif printer_type == "moonraker":
        return get_moonraker_status(printer["ip"])
    else:
        # Support both "api_key" and "password" fields in config
        key = printer.get("api_key") or printer.get("password", "")
        return get_printer_status(
            printer["ip"],
            key,
            auth_type=printer.get("auth_type", "api_key"),
            username=printer.get("username", "maker"),
        )


# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------


def format_time(seconds):
    """Convert seconds into a human-friendly time string."""
    if seconds is None:
        return "unknown"
    if seconds <= 0:
        return "0m"
    hours, remainder = divmod(int(seconds), 3600)
    minutes, _ = divmod(remainder, 60)
    if hours > 0:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def get_status_emoji(state):
    """Return an emoji representing the printer state."""
    state_emojis = {
        "IDLE": "🟢",
        "READY": "🟢",
        "PRINTING": "🟡",
        "BUSY": "🟡",
        "PAUSED": "🟠",
        "FINISHED": "🔵",
        "STOPPED": "🔴",
        "ERROR": "🔴",
        "ATTENTION": "🟠",
        "OFFLINE": "⚫",
        "UNKNOWN": "⚪",
    }
    return state_emojis.get(state, "⚪")


def get_status_label(state):
    """Return a human-friendly label for the printer state."""
    state_labels = {
        "IDLE": "Available",
        "READY": "Available",
        "PRINTING": "Printing",
        "BUSY": "Busy",
        "PAUSED": "Paused",
        "FINISHED": "Print Finished",
        "STOPPED": "Stopped",
        "ERROR": "Error",
        "ATTENTION": "Needs Attention",
        "OFFLINE": "Offline",
        "UNKNOWN": "Unknown",
    }
    return state_labels.get(state, state)


def get_file_name(status):
    """Extract the file name from job data."""
    if status.get("job_data") and isinstance(status["job_data"], dict):
        return (
            status["job_data"].get("file", {}).get("display_name")
            or status["job_data"].get("file", {}).get("name")
            or "Unknown file"
        )
    return "Unknown file"


def parse_username_from_filename(file_name):
    """
    Extract a Discord username from a filename.
    Looks for the pattern _@username before the file extension.
    Example: 'bracket_v3_@bob.smith.gcode' returns 'bob.smith'
    Returns None if no username is found.
    """
    if "_@" not in file_name:
        return None

    # Get everything after the last _@
    after_at = file_name.split("_@")[-1]

    # Remove the file extension (.gcode, .bgcode, etc.)
    # Find the last dot that's part of the extension
    # We need to be careful because usernames can have dots (e.g. riley.smith)
    # So we strip known gcode extensions from the end
    for ext in [".gcode.3mf", ".bgcode", ".gcode", ".gco", ".3mf"]:
        if after_at.lower().endswith(ext):
            return after_at[: -len(ext)]

    return after_at


async def find_member_by_username(guild, username):
    """
    Search guild members for a matching Discord username.
    Returns the Member object if found, None otherwise.
    Case-insensitive search.
    """
    if username is None:
        return None

    username_lower = username.lower()

    for member in guild.members:
        if member.name.lower() == username_lower:
            return member

    return None


# ---------------------------------------------------------------------------
# Discord Bot
# ---------------------------------------------------------------------------

intents = discord.Intents.default()
intents.message_content = True
intents.members = True  # Required to look up members by username
bot = discord.Client(intents=intents)

# Store the message ID so we can edit the same pinned message each time
status_message_id = None

# Track previous printer states to detect completed/failed prints
# Key: printer name, Value: dict with state and file info from last poll
previous_states = {}

# Track print duration for each printer (in-memory only).
# Excludes paused time to normalize with Prusa's time_printing behavior.
# Key: printer name, Value: dict with:
#   "accumulated_seconds": total printing seconds so far (frozen while paused)
#   "segment_start": datetime when current printing segment began (None if paused)
# If the bot restarts mid-print, this will be empty and duration will be omitted.
print_duration_trackers = {}

# Load printers and global config from config file
PRINTERS, POLL_INTERVAL, STATUS_TITLE = load_config()

# Connect to Bambu printers (MQTT background connections)
connect_bambu_printers(PRINTERS)


def build_status_embed(printer_statuses):
    """
    Build a Discord embed with all printer statuses.
    printer_statuses is a list of (printer, status, file_name, username) tuples.
    Returns a discord.Embed object.
    """
    embed = discord.Embed(
        title=STATUS_TITLE,
        color=0x4A90D9,
        timestamp=datetime.now(timezone.utc),
    )

    for printer, status, file_name, username in printer_statuses:
        emoji = get_status_emoji(status["state"])
        label = get_status_label(status["state"])

        if status["state"] == "PRINTING":
            progress = status.get("progress", 0) or 0
            time_left = format_time(status.get("time_remaining"))
            user_display = f"@{username}" if username else "Unknown User"

            value = (
                f"`{file_name}`\n"
                f"{user_display}\n"
                f"{progress:.0f}% complete | ~{time_left} remaining"
            )
        elif status["state"] == "OFFLINE":
            value = "Printer is not reachable"
        elif status["state"] in ("ERROR", "ATTENTION"):
            hms_errors = status.get("hms_errors", [])
            error_message = status.get("error_message")
            if hms_errors:
                hms = hms_errors[0]
                value = f"{hms['module']} error: `{hms['code']}`\n[Troubleshoot]({hms['url']})"
            elif error_message:
                value = f"Error: {error_message}"
            else:
                value = "Check printer"
        else:
            value = "\u200b"  # Invisible character so Discord doesn't complain about empty value

        embed.add_field(
            name=f"{emoji} {printer['name']} - {label}",
            value=value,
            inline=False,
        )

    embed.set_footer(text=f"Updates every {POLL_INTERVAL} seconds")

    return embed


@bot.event
async def on_ready():
    """Called when the bot connects to Discord."""
    print(f"Bot connected as {bot.user}")
    print(f"Monitoring {len(PRINTERS)} printer(s)")
    if not update_status.is_running():
        update_status.start()


@tasks.loop(seconds=POLL_INTERVAL)
async def update_status():
    """Periodically poll printers and update the status message in Discord."""
    global status_message_id, previous_states

    status_channel = bot.get_channel(STATUS_CHANNEL_ID)
    notification_channel = bot.get_channel(NOTIFICATION_CHANNEL_ID)

    if status_channel is None:
        print(f"Error: Could not find status channel with ID {STATUS_CHANNEL_ID}")
        return

    if notification_channel is None:
        print(f"Error: Could not find notification channel with ID {NOTIFICATION_CHANNEL_ID}")
        return

    # On first run, clear the status channel so it's clean
    if status_message_id is None:
        await status_channel.purge()

    guild = status_channel.guild

    # Poll all printers and collect statuses
    printer_statuses = []
    for printer in PRINTERS:
        status = poll_printer(printer)
        file_name = get_file_name(status) if status["state"] == "PRINTING" else None
        username = parse_username_from_filename(file_name) if file_name else None
        printer_statuses.append((printer, status, file_name, username))

    # --- Check for completed or failed prints (send to notification channel) ---
    for printer, status, file_name, username in printer_statuses:
        printer_name = printer["name"]
        prev = previous_states.get(printer_name)

        # Skip UNKNOWN states (Bambu printers may return this on first poll
        # before MQTT data arrives). Don't save it or check notifications.
        if status["state"] == "UNKNOWN":
            continue

        # --- Duration tracking (excludes paused time) ---
        # Start a new tracker when a printer begins printing (but not when resuming from pause)
        if status["state"] == "PRINTING" and (not prev or prev["state"] != "PRINTING"):
            if prev and prev["state"] == "PAUSED":
                # Resuming from pause — restart the segment timer, keep accumulated time
                tracker = print_duration_trackers.get(printer_name)
                if tracker:
                    tracker["segment_start"] = datetime.now()
            else:
                # New print job — fresh tracker
                print_duration_trackers[printer_name] = {
                    "accumulated_seconds": 0,
                    "segment_start": datetime.now(),
                }

        # When a printer pauses, freeze the current segment
        if status["state"] == "PAUSED" and prev and prev["state"] == "PRINTING":
            tracker = print_duration_trackers.get(printer_name)
            if tracker and tracker["segment_start"]:
                elapsed = (datetime.now() - tracker["segment_start"]).total_seconds()
                tracker["accumulated_seconds"] += elapsed
                tracker["segment_start"] = None  # Frozen while paused

        # --- Check for completed, cancelled, or failed prints ---
        if prev and prev["state"] in ("PRINTING", "PAUSED"):
            # For Bambu printers, raw_bambu_state == "FINISH" means completed
            bambu_finished = status.get("raw_bambu_state") == "FINISH"
            # For Moonraker printers, raw_moonraker_state == "complete" means completed
            moonraker_finished = status.get("raw_moonraker_state") == "complete"
            # For Moonraker, "cancelled" is an explicit state
            moonraker_cancelled = status.get("raw_moonraker_state") == "cancelled"

            print_ended = status["state"] in ("IDLE", "READY", "FINISHED") or bambu_finished or moonraker_finished
            print_failed = status["state"] in ("ERROR", "STOPPED")

            # Determine if a "failure" is actually a user-initiated cancel:
            # - Bambu: gcode_state goes to FAILED on cancel, but print_error == 0
            #   and no HMS errors. A real failure has HMS errors or print_error != 0.
            # - Moonraker: has an explicit "cancelled" state.
            is_cancel = False
            if print_failed:
                hms_errors = status.get("hms_errors", [])
                print_error = status.get("print_error", 0)
                if not hms_errors and print_error == 0:
                    is_cancel = True
            if moonraker_cancelled:
                is_cancel = True
                print_ended = False
                print_failed = False

            if print_ended or print_failed or is_cancel:
                # Calculate final duration:
                # 1. Use printer-reported time_printing if available (Prusa)
                # 2. Fall back to in-memory tracker (Bambu, or if API didn't report it)
                reported_time = prev.get("time_printing")
                tracker = print_duration_trackers.pop(printer_name, None)

                if reported_time:
                    duration_str = format_time(int(reported_time))
                elif tracker:
                    total = tracker["accumulated_seconds"]
                    if tracker["segment_start"]:
                        # Add time from the final printing segment
                        total += (datetime.now() - tracker["segment_start"]).total_seconds()
                    duration_str = format_time(int(total))
                else:
                    duration_str = None  # Bot restarted mid-print; omit duration

                member = await find_member_by_username(guild, prev.get("username"))
                mention = member.mention if member else (f"@{prev['username']}" if prev.get("username") else "Unknown user")
                prev_file = prev.get("file_name", "Unknown file")
                duration_line = f"Print time: {duration_str}\n" if duration_str else ""

                if print_ended:
                    await notification_channel.send(
                        f"**Print Complete** on **{printer_name}**\n"
                        f"`{prev_file}`\n"
                        f"{mention}\n"
                        f"{duration_line}"
                        f"Your print is ready for pickup!"
                    )
                elif is_cancel:
                    await notification_channel.send(
                        f"**Print Cancelled** on **{printer_name}**\n"
                        f"`{prev_file}`\n"
                        f"{mention}\n"
                        f"{duration_line}"
                    )
                else:
                    # Build error info for the notification
                    hms_line = ""
                    hms_errors = status.get("hms_errors", [])
                    error_message = status.get("error_message")
                    if hms_errors:
                        hms = hms_errors[0]
                        hms_line = f"{hms['module']} error: `{hms['code']}`\n[Troubleshoot on Bambu Wiki]({hms['url']})\n"
                    elif error_message:
                        hms_line = f"Error: {error_message}\n"

                    await notification_channel.send(
                        f"**Print Failed** on **{printer_name}**\n"
                        f"`{prev_file}`\n"
                        f"{mention}\n"
                        f"{duration_line}"
                        f"{hms_line}"
                        f"Please check the printer."
                    )

        # Save current state for next poll comparison
        previous_states[printer_name] = {
            "state": status["state"],
            "file_name": file_name,
            "username": username,
            "time_printing": status.get("time_printing"),
        }

    # --- Update the status embed (in status channel) ---
    embed = build_status_embed(printer_statuses)

    try:
        if status_message_id is not None:
            try:
                message = await status_channel.fetch_message(status_message_id)
                await message.edit(embed=embed)
            except discord.NotFound:
                # Message was deleted — post a fresh one and track it
                message = await status_channel.send(embed=embed)
                status_message_id = message.id
        else:
            message = await status_channel.send(embed=embed)
            status_message_id = message.id
    except (discord.HTTPException, aiohttp.ClientError, asyncio.TimeoutError) as e:
        # Transient Discord/network problem (rate limit, 5xx, dropped connection).
        # Skip this cycle rather than letting the exception kill the task loop.
        print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] Status update failed, "
              f"retrying next cycle: {e!r}")


@update_status.before_loop
async def before_update_status():
    """Wait until the bot is ready before starting the loop."""
    await bot.wait_until_ready()


@update_status.error
async def update_status_error(error):
    """
    Safety net: discord.py's tasks.loop dies silently if an exception escapes
    the loop body. The bot stays 'online' but the status message freezes.
    This logs the failure and restarts the loop instead.
    """
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] update_status loop crashed: {error!r}")
    traceback.print_exc()
    await asyncio.sleep(10)
    if not update_status.is_running():
        update_status.restart()


# ---------------------------------------------------------------------------
# Run the bot
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if not DISCORD_TOKEN:
        print("Error: DISCORD_TOKEN not found in .env file")
        print("Create a .env file with: DISCORD_TOKEN=your_token_here")
        exit(1)

    if STATUS_CHANNEL_ID == 0:
        print("Error: STATUS_CHANNEL_ID not found in .env file")
        print("Add to .env file: STATUS_CHANNEL_ID=your_channel_id_here")
        exit(1)

    if NOTIFICATION_CHANNEL_ID == 0:
        print("Error: NOTIFICATION_CHANNEL_ID not found in .env file")
        print("Add to .env file: NOTIFICATION_CHANNEL_ID=your_channel_id_here")
        exit(1)

    try:
        bot.run(DISCORD_TOKEN)
    finally:
        disconnect_bambu_printers()
