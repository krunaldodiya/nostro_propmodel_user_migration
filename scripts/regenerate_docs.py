#!/usr/bin/env python3
"""
Quick script to regenerate all export documentation

Usage:
    uv run python scripts/regenerate_docs.py

This script is READ-ONLY and will not modify any CSV files.
"""

import sys
import os

# Add scripts directory to path
sys.path.append(os.path.join(os.path.dirname(__file__)))

from generate_all_export_docs import generate_all_documentation

if __name__ == "__main__":
    print("🔄 Regenerating Export Documentation...")
    print("📖 This process is READ-ONLY - no CSV files will be modified")
    print()

    results = generate_all_documentation()

    print()
    print("✅ Documentation regeneration complete!")
    print("📁 Check docs/exports/ for all documentation files")
    print("📊 See EXPORT_SUMMARY.md for a quick overview")
