# logger.py
import logging

# Pretty color-coded logger for terminal
logging.basicConfig(
    level=logging.INFO,
    format="\033[92m[%(levelname)s]\033[0m %(message)s"  # green tag + message
)

logger = logging.getLogger("pipeline")