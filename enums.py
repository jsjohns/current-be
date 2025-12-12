"""Shared enumerations for the portal backend."""
from typing import Literal

Utility = Literal["ELECTRIC", "GAS", "WATER", "SEWER", "TRASH"]
Reason = Literal["ACQUISITION", "DISPOSITION", "MOVE_OUT", "EVICTION", "ABANDONMENT", "ONBOARDING", "OTHER"]
SuborderStatus = Literal["TODO", "IN_PROGRESS", "BLOCKED_MANAGER", "BLOCKED_PROVIDER", "DONE", "CANCELED", "RETURNED"]

REASON_DISPLAY = {
    "ACQUISITION": "Acquisition",
    "DISPOSITION": "Disposition",
    "MOVE_OUT": "Move-Out",
    "EVICTION": "Eviction",
    "ABANDONMENT": "Abandonment",
    "ONBOARDING": "Onboarding",
    "OTHER": "Other",
}

# Maps single-letter abbreviations to full utility names (for parsing Linear titles)
UTILITY_ABBREV_MAP = {
    'E': 'ELECTRIC',
    'G': 'GAS',
    'W': 'WATER',
    'T': 'TRASH',
}


def get_suborder_status(state_name: str, label_names: list[str]) -> SuborderStatus:
    """Derive status from Linear state and labels. Terminal states take precedence."""
    # Terminal states first
    if state_name == 'Done':
        return 'DONE'
    if state_name == 'Canceled':
        # Check for Returned label
        if 'Returned' in label_names:
            return 'RETURNED'
        return 'CANCELED'

    # Blocked labels (only for non-terminal states)
    if 'Blocked - Manager' in label_names:
        return 'BLOCKED_MANAGER'
    if 'Blocked - Provider' in label_names:
        return 'BLOCKED_PROVIDER'

    # Normal states
    if state_name == 'Todo':
        return 'TODO'
    if state_name == 'In Progress':
        return 'IN_PROGRESS'

    return 'TODO'  # fallback
