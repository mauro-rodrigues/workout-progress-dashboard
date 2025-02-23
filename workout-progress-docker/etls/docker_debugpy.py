import os
import sys
import time
import signal
import debugpy
import logging
import threading


# configure logging
logging.basicConfig(level=logging.INFO)


def monitor_debugger_connection():
    while True:
        time.sleep(2) # check every 2 seconds
        if not debugpy.is_client_connected():
            logging.warning("Debugging client has disconnected. Terminating execution.")
            os.kill(os.getpid(), signal.SIGTERM) # terminate the main process


def start_debugger_monitor():
    monitor_thread = threading.Thread(target=monitor_debugger_connection, daemon=True)
    monitor_thread.start()


def init_debugger():
    if os.getenv("TESTING", "true") == "true":
        # this starts debugpy and makes it listen on port 5680
        debugpy.listen(("0.0.0.0", 5680))
        logging.info("Debugpy listening on 0.0.0.0:5680...")

        # makes the script wait for the debugger to connect before continuing
        debugpy.wait_for_client()

        debugpy.breakpoint()
        start_debugger_monitor()
    else:
        logging.info("Running without debugger...")


if __name__ == '__main__':

    init_debugger()

    script_to_run = sys.argv[1]
    sys.argv = sys.argv[1:] # adjust argv for the script

    with open(script_to_run) as script:
        code = compile(script.read(), script_to_run, "exec")
        exec(code)