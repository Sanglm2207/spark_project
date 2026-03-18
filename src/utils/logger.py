"""
Simple colored console logger.
Usage:
    from src.utils.logger import get_logger
    log = get_logger(__name__)
    log.ok("Done!")
"""

from __future__ import annotations

GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
DIM    = "\033[2m"
RESET  = "\033[0m"


class AppLogger:
    def __init__(self, name: str) -> None:
        self._name = name

    def ok(self, msg: str) -> None:
        print(f"  {GREEN}✔{RESET}  {msg}")

    def warn(self, msg: str) -> None:
        print(f"  {YELLOW}⚠{RESET}  {msg}")

    def error(self, msg: str) -> None:
        print(f"  {RED}✘{RESET}  {msg}")

    def info(self, msg: str) -> None:
        print(f"  {CYAN}→{RESET}  {msg}")

    def header(self, msg: str) -> None:
        print(f"\n{BOLD}{msg}{RESET}")
        print(f"{DIM}{'─' * 52}{RESET}")


def get_logger(name: str) -> AppLogger:
    return AppLogger(name)
