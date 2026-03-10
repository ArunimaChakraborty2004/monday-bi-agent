"""
monday.com API client for fetching board data.

Fetches items from configured boards (e.g. Deals, Work Orders) with full
column values, with cursor-based pagination support.
"""

import os
import logging
from typing import Any

import requests

logger = logging.getLogger(__name__)

MONDAY_API_URL = "https://api.monday.com/v2"
DEFAULT_LIMIT = 500


class MondayAPIError(Exception):
    """Raised when the monday.com API returns an error."""

    pass


class MondayClient:
    """
    Client for the monday.com GraphQL API.

    Uses API token from environment variable MONDAY_API_KEY.
    Board IDs can be set via env (MONDAY_DEALS_BOARD_ID, MONDAY_WORK_ORDERS_BOARD_ID)
    or passed when fetching.
    """

    def __init__(
        self,
        api_key: str | None = None,
        deals_board_id: int | str | None = None,
        work_orders_board_id: int | str | None = None,
    ) -> None:
        self.api_key = api_key or os.environ.get("MONDAY_API_KEY")
        self.deals_board_id = deals_board_id or os.environ.get("MONDAY_DEALS_BOARD_ID")
        self.work_orders_board_id = work_orders_board_id or os.environ.get(
            "MONDAY_WORK_ORDERS_BOARD_ID"
        )
        if not self.api_key:
            raise ValueError(
                "Monday API key required. Set MONDAY_API_KEY or pass api_key=..."
            )

    def _request(self, query: str, variables: dict[str, Any] | None = None) -> dict:
        """Execute a GraphQL request and return the JSON response."""
        payload: dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables

        response = requests.post(
            MONDAY_API_URL,
            json=payload,
            headers={
                "Authorization": self.api_key,
                "Content-Type": "application/json",
            },
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()

        if "errors" in data and data["errors"]:
            messages = [e.get("message", str(e)) for e in data["errors"]]
            raise MondayAPIError("; ".join(messages))

        return data.get("data", {})

    def _item_to_row(self, item: dict) -> dict[str, Any]:
        """Convert an API item (with column_values) into a flat row dict."""
        row: dict[str, Any] = {
            "id": item.get("id"),
            "name": item.get("name"),
        }
        for cv in item.get("column_values") or []:
            column = cv.get("column") or {}
            title = column.get("title") or cv.get("id")
            if not title:
                continue
            # Prefer text for display/analysis; fall back to value for raw JSON
            text = cv.get("text")
            value = cv.get("value")
            if text is not None and str(text).strip() != "":
                row[title] = text
            elif value is not None:
                row[title] = value
            else:
                row[title] = None
        return row

    def _fetch_items_page(self, board_id: int | str) -> list[dict]:
        """Fetch one page of items for a board (up to 500)."""
        query = """
        query ($boardId: [ID!]!) {
            boards(ids: $boardId) {
                items_page(limit: 500) {
                    cursor
                    items {
                        id
                        name
                        column_values {
                            id
                            type
                            value
                            text
                            column {
                                title
                            }
                        }
                    }
                }
            }
        }
        """
        bid = str(board_id).strip()
        data = self._request(query, {"boardId": [bid]})
        boards = data.get("boards") or []
        if not boards:
            return [], None
        page = (boards[0] or {}).get("items_page") or {}
        items = page.get("items") or []
        cursor = page.get("cursor")
        return items, cursor

    def _fetch_next_page(self, cursor: str) -> tuple[list[dict], str | None]:
        """Fetch the next page of items using cursor."""
        query = """
        query ($cursor: String!) {
            next_items_page(cursor: $cursor) {
                cursor
                items {
                    id
                    name
                    column_values {
                        id
                        type
                        value
                        text
                        column {
                            title
                        }
                    }
                }
            }
        }
        """
        data = self._request(query, {"cursor": cursor})
        page = data.get("next_items_page") or {}
        items = page.get("items") or []
        next_cursor = page.get("cursor")
        return items, next_cursor

    def fetch_board_items(self, board_id: int | str) -> list[dict[str, Any]]:
        """
        Fetch all items from a board, paginating until done.

        Returns a list of flat dicts: id, name, and one key per column (by title).
        """
        all_rows: list[dict[str, Any]] = []
        items, cursor = self._fetch_items_page(board_id)
        for item in items:
            all_rows.append(self._item_to_row(item))

        while cursor:
            items, cursor = self._fetch_next_page(cursor)
            for item in items:
                all_rows.append(self._item_to_row(item))

        logger.info("Fetched %s items from board %s", len(all_rows), board_id)
        return all_rows

    def fetch_deals(self) -> list[dict[str, Any]]:
        """Fetch all items from the Deals board."""
        if not self.deals_board_id:
            raise ValueError(
                "Deals board ID not set. Set MONDAY_DEALS_BOARD_ID or pass deals_board_id=..."
            )
        return self.fetch_board_items(self.deals_board_id)

    def fetch_work_orders(self) -> list[dict[str, Any]]:
        """Fetch all items from the Work Orders board."""
        if not self.work_orders_board_id:
            raise ValueError(
                "Work Orders board ID not set. Set MONDAY_WORK_ORDERS_BOARD_ID or pass work_orders_board_id=..."
            )
        return self.fetch_board_items(self.work_orders_board_id)
