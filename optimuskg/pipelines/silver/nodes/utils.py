import logging
import re

logger = logging.getLogger(__name__)


def classify_age_type(age_str: str, qualifier_str: str) -> str:
    """Classify age and return type string"""
    if not age_str or age_str in ["", "null"]:
        return "null"

    age_clean = str(age_str).strip()
    qualifier_clean = str(qualifier_str).lower().strip() if qualifier_str else ""

    # Check patterns
    pattern_type = {r"[><=]+\s*\d+": "open_range", r"\d+\s*[-–]\s*\d+": "closed_range"}

    for pattern, type_ in pattern_type.items():
        if re.search(pattern, age_clean):
            return type_

    if re.search(r"\d+", age_clean):
        if "mean" in qualifier_clean:
            return "mean"
        if "median" in qualifier_clean:
            return "median"
        return "point"

    return "unparsed"


def extract_age_value(age_str: str, unit_str: str) -> float | None:
    """Extract numeric value from age, converted to years"""
    if not age_str or age_str in ["", "null"]:
        return None

    age_clean = str(age_str).strip()
    unit_clean = str(unit_str).lower().strip() if unit_str else "years"

    conversion_factor = 1.0 / 12 if unit_clean == "months" else 1.0

    # Extract first number found
    number_match = re.search(r"(\d+(?:\.\d+)?)", age_clean)
    if number_match:
        return float(number_match.group(1)) * conversion_factor
    return None
