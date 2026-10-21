#!/usr/bin/env python3
"""
Wrapper to ensure execution whether Render runs "python bb_telegram_bot.py" or "python bot.py".
Directly starts the Jio-Automation Bot engine.
"""
import sys
import os

if __name__ == "__main__":
    import bot
    bot.main()
