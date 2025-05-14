"""
PyTest configuration file.
"""
import os
import sys
from pathlib import Path

# Add the parent directory to sys.path
# This ensures that app can be imported in the tests
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import fixtures here if needed 