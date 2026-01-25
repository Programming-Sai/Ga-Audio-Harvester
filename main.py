# main.py
from download_layer.logging_config import setup_logging
setup_logging()  # MUST be first

from input_layer.terminal_input import cli_dispatch

def main():
    """Entry point - delegate to CLI dispatcher"""
    cli_dispatch()

if __name__ == "__main__":
    main()
