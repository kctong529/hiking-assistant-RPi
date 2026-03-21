import hike
import db
import bt

from datetime import datetime

hubdb = db.HubDatabase()
hubbt = bt.HubBluetooth()


def normalize_session_timestamps(
        session: hike.HikeSession,
        received_at: datetime | None = None
    ) -> hike.HikeSession:
    if received_at is None:
        received_at = datetime.now()

    # Keep the watch-provided timestamps.
    # They should now be mostly correct once the watch has been time-synced.
    # Only set created_at on the receiver side to record when the session
    # was received and stored by the Raspberry Pi.
    session.created_at = received_at.isoformat(timespec="seconds")
    return session


def process_sessions(sessions: list[hike.HikeSession]):
    """Callback function to process sessions.

    Saves the session into the database.

    Args:
        sessions: list of `hike.HikeSession` objects to process
    """
    for s in sessions:
        normalize_session_timestamps(s)
        hubdb.save(s)


def main():
    print("Starting Bluetooth receiver.")
    try:
        while True:
            hubbt.wait_for_connection()
            hubbt.synchronize(callback=process_sessions)

    except KeyboardInterrupt:
        print("CTRL+C Pressed. Shutting down the server...")

    except Exception as e:
        print("Unexpected shutdown...")
        print(f"ERROR: {e}")
        hubbt.sock.close()
        raise e


if __name__ == "__main__":
    main()
