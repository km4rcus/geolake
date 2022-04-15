from __future__ import annotations

import logging

class RoleManager:

    _LOG = logging.getLogger("RoleManager")

    @classmethod
    def is_role_eligible(cls, product_role: str | None = "public", current_role: str | None = "public") -> bool:
        # NOTE: extend when more roles are defined!
        if product_role == "public":
            return True
        if product_role == current_role:
            return True
        if product_role == "internal" and current_role == "admin":
            return True
        return False