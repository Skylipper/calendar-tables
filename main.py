import os
from pathlib import Path

# Get the absolute path of the current file
current_file_path = Path(__file__).resolve()

print(f"The full path of the current file is: {current_file_path.parent}")