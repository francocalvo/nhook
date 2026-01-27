#!/bin/bash
# Auto-format Python files with ruff after Edit/Write operations
# Called as PostToolUse hook for Edit and Write tools

# Read JSON input from stdin
input=$(cat)

# Extract the file_path from JSON
file_path=$(echo "$input" | jq -r '.tool_input.file_path // empty')

# If no file_path, exit silently
if [ -z "$file_path" ]; then
    exit 0
fi

# Check if it's a Python file
if [[ "$file_path" != *.py ]]; then
    exit 0
fi

# Check if file exists
if [ ! -f "$file_path" ]; then
    exit 0
fi

# Check if ruff is available
if ! command -v ruff &> /dev/null; then
    echo "Note: ruff not found, skipping auto-format"
    exit 0
fi

# Run ruff format (quiet mode to reduce noise)
ruff format --quiet "$file_path" 2>/dev/null || true

# Run ruff check with auto-fix (quiet mode)
ruff check --fix --quiet "$file_path" 2>/dev/null || true

# Always exit 0 - formatting should not block operations
exit 0
