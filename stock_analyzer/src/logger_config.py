import logging
import sys
from pathlib import Path

def setup_logger(name: str) -> logging.Logger:
    """
    Creates a standardized logger writing to both console and a local log file.
    """
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        # Structured, developer-friendly format: Timestamp | Level | Module | Message
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)-15s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        
        # Screen Output
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # File Output
        log_dir = Path(__file__).resolve().parent / "logs"
        log_dir.mkdir(exist_ok=True)
        file_handler = logging.FileHandler(log_dir / "fintech_dashboard.log")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        logger.propagate = False

    return logger