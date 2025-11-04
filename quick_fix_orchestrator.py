#!/usr/bin/env python3
"""Quick fix for orchestrator dataclass access patterns"""

from pathlib import Path
import re

file_path = Path("src/orchestrator.py")
content = file_path.read_text()

# Fix collapse_result dictionary access
content = re.sub(
    r"collapse_result\['overall_score'\]",
    "collapse_result.overall_score",
    content
)
content = re.sub(
    r"collapse_result\['collapse_detected'\]",
    "collapse_result.collapse_detected",
    content
)
content = re.sub(
    r"collapse_result\['dimensions'\]",
    "collapse_result.dimensions",
    content
)

# Fix diversity_result remaining occurrences
content = re.sub(
    r"diversity_result,",
    "{'overall_score': diversity_result.overall_score, 'dimension_scores': diversity_result.dimension_scores},",
    content
)

file_path.write_text(content)
print("✅ Fixed orchestrator.py dataclass access patterns")
