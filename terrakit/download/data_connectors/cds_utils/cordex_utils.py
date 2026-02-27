# © Copyright IBM Corporation 2026
# SPDX-License-Identifier: Apache-2.0


import json
from pathlib import Path
from typing import Any, Dict, List

# Load CORDEX domains from JSON file
_CORDEX_DOMAINS_FILE = Path(__file__).parent / "cordex_domains.json"


def _load_cordex_domains() -> Dict[str, Any]:
    """Load CORDEX domains from JSON file."""
    with open(_CORDEX_DOMAINS_FILE, "r") as f:
        data: Dict[str, Any] = json.load(f)
    domains: Dict[str, Any] = data["domains"]
    return domains


# Cache the domains data
CORDEX_DOMAINS = _load_cordex_domains()


def get_domain_info(domain_code: str) -> Dict[str, Any]:
    """
    Get information for a specific CORDEX domain.

    Args:
        domain_code: CORDEX domain code (e.g., 'EUR-11', 'AFR-44')

    Returns:
        Dictionary with domain information including name, bbox, and resolution

    Raises:
        ValueError: If domain code is not found
    """
    if domain_code not in CORDEX_DOMAINS:
        available = list_available_domains()
        raise ValueError(
            f"Unknown CORDEX domain: {domain_code}. "
            f"Available domains: {', '.join(available)}"
        )
    domain_info: Dict[str, Any] = CORDEX_DOMAINS[domain_code]
    return domain_info


def list_available_domains() -> List[str]:
    """
    List all available CORDEX domain codes.

    Returns:
        List of domain codes (e.g., ['AFR-44', 'EUR-11', ...])
    """
    return sorted(CORDEX_DOMAINS.keys())


def get_domains_by_region(region_name: str) -> List[str]:
    """
    Get all domain codes for a specific region.

    Args:
        region_name: Region name (e.g., 'Europe', 'Africa')

    Returns:
        List of domain codes matching the region
    """
    return [
        code
        for code, info in CORDEX_DOMAINS.items()
        if region_name.lower() in info["name"].lower()
    ]


def get_domains_by_resolution(resolution_degrees: float) -> List[str]:
    """
    Get all domains with a specific resolution.

    Args:
        resolution_degrees: Resolution in degrees (e.g., 0.44, 0.22, 0.11)

    Returns:
        List of domain codes with matching resolution
    """
    return [
        code
        for code, info in CORDEX_DOMAINS.items()
        if info["resolution_degrees"] == resolution_degrees
    ]


def bbox_intersects(bbox1: List[float], bbox2: List[float]) -> bool:
    """
    Check if two bounding boxes intersect.

    Args:
        bbox1: [min_lon, min_lat, max_lon, max_lat]
        bbox2: [min_lon, min_lat, max_lon, max_lat]

    Returns:
        True if bboxes intersect, False otherwise
    """
    min_lon1, min_lat1, max_lon1, max_lat1 = bbox1
    min_lon2, min_lat2, max_lon2, max_lat2 = bbox2

    # Check if boxes DON'T intersect
    if (
        max_lon1 < min_lon2
        or min_lon1 > max_lon2
        or max_lat1 < min_lat2
        or min_lat1 > max_lat2
    ):
        return False

    return True


def find_matching_domains(bbox: List[float]) -> List[str]:
    """
    Find all CORDEX domains that intersect with the given bounding box.

    Args:
        bbox: User's bounding box [min_lon, min_lat, max_lon, max_lat]

    Returns:
        List of domain codes that intersect with the bbox
    """
    matching = []
    for domain_code, domain_info in CORDEX_DOMAINS.items():
        if bbox_intersects(bbox, domain_info["bbox"]):
            matching.append(domain_code)
    return matching
