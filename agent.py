"""
BI agent that answers business intelligence questions using cleaned monday.com data.

Uses pandas for analysis and produces narrative insights, not just raw numbers.
"""

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


class BIAgent:
    """
    Answers natural-language BI questions over Deals and Work Orders data.

    Supports questions about pipeline by sector/quarter, total deal value,
    best-performing sector, and similar. Returns insights with context.
    """

    def __init__(
        self,
        deals_df: pd.DataFrame,
        work_orders_df: pd.DataFrame,
    ) -> None:
        self.deals = deals_df.copy() if deals_df is not None else pd.DataFrame()
        self.work_orders = (
            work_orders_df.copy() if work_orders_df is not None else pd.DataFrame()
        )

    def _current_quarter_bounds(self) -> tuple[pd.Timestamp, pd.Timestamp]:
        """Start and end of current quarter."""
        now = pd.Timestamp.now()
        quarter = (now.month - 1) // 3 + 1
        start = pd.Timestamp(year=now.year, month=(quarter - 1) * 3 + 1, day=1)
        if quarter == 4:
            end = pd.Timestamp(year=now.year, month=12, day=31)
        else:
            end = start + pd.offsets.QuarterEnd(0)
        return start, end

    def _parse_quarter_from_text(self, text: str) -> tuple[pd.Timestamp, pd.Timestamp] | None:
        """Infer quarter from phrases like 'this quarter', 'Q1', 'last quarter'."""
        text = text.lower().strip()
        now = pd.Timestamp.now()
        if "this quarter" in text or "current quarter" in text or "q" + str((now.month - 1) // 3 + 1) in text:
            return self._current_quarter_bounds()
        if "last quarter" in text or "previous quarter" in text:
            start, end = self._current_quarter_bounds()
            start = start - pd.offsets.QuarterEnd(0) - pd.offsets.MonthBegin(1)
            end = start + pd.offsets.QuarterEnd(0)
            return start, end
        for q in range(1, 5):
            if f"q{q}" in text or f"quarter {q}" in text:
                start = pd.Timestamp(year=now.year, month=(q - 1) * 3 + 1, day=1)
                end = start + pd.offsets.QuarterEnd(0)
                return start, end
        return None

    def _extract_sector(self, text: str) -> str | None:
        """Extract sector mention from question (e.g. 'energy sector' -> 'Energy')."""
        text = text.lower()
        sector_map = {
            "energy": "Energy",
            "tech": "Technology",
            "technology": "Technology",
            "healthcare": "Healthcare",
            "finance": "Financial Services",
            "financial": "Financial Services",
            "manufacturing": "Manufacturing",
            "retail": "Retail",
            "telecom": "Telecommunications",
            "construction": "Construction",
            "real estate": "Real Estate",
            "government": "Government",
            "education": "Education",
        }
        for key, canonical in sector_map.items():
            if key in text:
                return canonical
        return None

    def _get_deal_value_col(self) -> str | None:
        if "deal_value_numeric" in self.deals.columns:
            return "deal_value_numeric"
        return None

    def _get_date_col(self) -> str | None:
        for c in self.deals.columns:
            if "parsed" in c and "date" in c.lower():
                return c
        return None

    def _get_due_date_col(self) -> str | None:
        """
        Best-effort due date column selection.

        Prefers columns that look like a due/close/expected date and were parsed by the cleaner.
        """
        candidates = [
            "due date_parsed",
            "close date_parsed",
            "expected close_parsed",
            "expected close date_parsed",
        ]
        cols_lower = {str(c).lower(): c for c in self.deals.columns}
        for cand in candidates:
            if cand in cols_lower:
                return cols_lower[cand]

        # Fall back to any parsed date column
        for c in self.deals.columns:
            if str(c).lower().endswith("_parsed") and "date" in str(c).lower():
                return c
        return None

    def _get_status_col(self) -> str | None:
        cols_lower = {str(c).lower(): c for c in self.deals.columns}
        for cand in ["status", "stage", "deal stage"]:
            if cand in cols_lower:
                return cols_lower[cand]
        return None

    def _get_owner_col(self) -> str | None:
        cols_lower = {str(c).lower(): c for c in self.deals.columns}
        for cand in ["owner", "sales rep", "assignee", "person"]:
            if cand in cols_lower:
                return cols_lower[cand]
        return None

    def _is_closed_status(self, status: str) -> bool:
        s = str(status).strip().lower()
        return any(
            k in s
            for k in [
                "won",
                "lost",
                "closed",
                "done",
                "complete",
                "completed",
                "cancel",
                "canceled",
                "cancelled",
            ]
        )

    def prioritize_deals(
        self,
        top_n: int = 10,
        sector: str | None = None,
        quarter_start: pd.Timestamp | None = None,
        quarter_end: pd.Timestamp | None = None,
    ) -> dict[str, Any]:
        """
        Recommend which deals to prioritize.

        Heuristics (works even without deal value):
        - open deals first (based on Status)
        - overdue due dates first
        - then nearest upcoming due dates
        """
        df = self.deals.copy()
        if df.empty:
            return {"insight": "No deal data available.", "rows": []}

        due_col = self._get_due_date_col()
        status_col = self._get_status_col()
        owner_col = self._get_owner_col()

        if sector:
            df = df[df["sector"].str.lower() == sector.lower()]

        if due_col and quarter_start is not None and quarter_end is not None:
            df = df[(df[due_col] >= quarter_start) & (df[due_col] <= quarter_end)]

        if status_col:
            df["_is_closed"] = df[status_col].apply(self._is_closed_status)
        else:
            df["_is_closed"] = False

        df_open = df[~df["_is_closed"]].copy()
        if df_open.empty:
            return {
                "insight": "All deals in the selected scope look closed/completed (based on Status).",
                "rows": [],
            }

        now = pd.Timestamp.now().normalize()
        if due_col:
            due = pd.to_datetime(df_open[due_col], errors="coerce")
            df_open["_due_date"] = due
            df_open["_days_to_due"] = (due.dt.normalize() - now).dt.days
            df_open["_overdue"] = df_open["_days_to_due"] < 0
        else:
            df_open["_due_date"] = pd.NaT
            df_open["_days_to_due"] = pd.NA
            df_open["_overdue"] = False

        # Ranking: overdue first, then soonest due date, then missing due dates last
        def sort_key(row: pd.Series) -> tuple:
            missing_due = pd.isna(row.get("_due_date"))
            overdue = bool(row.get("_overdue", False))
            days = row.get("_days_to_due")
            days_val = int(days) if pd.notna(days) else 10_000
            return (0 if overdue else 1, 0 if not missing_due else 1, days_val)

        df_open = df_open.copy()
        df_open["_sort"] = df_open.apply(sort_key, axis=1)
        df_open = df_open.sort_values("_sort").head(top_n)

        rows: list[dict[str, Any]] = []
        for _, r in df_open.iterrows():
            rows.append(
                {
                    "Deal": r.get("name"),
                    "Sector": r.get("sector"),
                    "Status": r.get(status_col) if status_col else None,
                    "Owner": r.get(owner_col) if owner_col else None,
                    "Due": (
                        r.get("_due_date").date().isoformat()
                        if pd.notna(r.get("_due_date"))
                        else None
                    ),
                    "Urgency": (
                        "Overdue"
                        if bool(r.get("_overdue", False))
                        else (
                            f"Due in {int(r.get('_days_to_due'))}d"
                            if pd.notna(r.get("_days_to_due"))
                            else "No due date"
                        )
                    ),
                }
            )

        overdue_count = int(df_open["Urgency"].eq("Overdue").sum()) if rows else 0
        insight = (
            f"Here are **{len(rows)}** deals to prioritize next, ranked by **due date urgency** and **open status**. "
            + ("Overdue items are shown first. " if due_col else "Add a due/close date column to improve prioritization. ")
            + ("Deal value isn't available yet, so ranking is time/urgency-based." if self._get_deal_value_col() is None else "")
        )
        return {"insight": insight, "rows": rows}

    def deals_breakdown(self, group_by: str) -> dict[str, Any]:
        """Simple count breakdown by a deals column (sector/status/owner)."""
        df = self.deals
        if df.empty:
            return {"insight": "No deal data available.", "breakdown": {}}
        if group_by not in df.columns:
            return {"insight": f"I can't group by `{group_by}` because it isn't present in the Deals data.", "breakdown": {}}
        vc = df[group_by].fillna("Unknown").astype(str).value_counts()
        return {"breakdown": vc.head(15).to_dict(), "total": int(len(df))}

    def _as_markdown_table(self, rows: list[dict[str, Any]]) -> str:
        if not rows:
            return ""
        df = pd.DataFrame(rows)
        return df.to_markdown(index=False)

    def pipeline_by_sector_quarter(
        self, sector: str | None = None, quarter_start: pd.Timestamp | None = None, quarter_end: pd.Timestamp | None = None
    ) -> dict[str, Any]:
        """Pipeline view: deal count and value, optionally by sector and quarter."""
        df = self.deals
        if df.empty:
            return {"count": 0, "total_value": 0.0, "by_sector": {}, "insight": "No deal data available."}

        value_col = self._get_deal_value_col()
        date_col = self._get_date_col()

        if sector:
            df = df[df["sector"].str.lower() == sector.lower()]

        if date_col and quarter_start is not None and quarter_end is not None:
            df = df[(df[date_col] >= quarter_start) & (df[date_col] <= quarter_end)]

        total_value = float(df[value_col].sum()) if value_col else None
        count = len(df)

        by_sector = {}
        if "sector" in df.columns:
            agg = df.groupby("sector", dropna=False)
            by_sector = agg.size().to_dict()
            if value_col:
                by_sector_value = agg[value_col].sum().to_dict()
                by_sector = {s: {"count": by_sector[s], "value": float(by_sector_value.get(s, 0))} for s in by_sector}

        return {
            "count": count,
            "total_value": total_value,
            "by_sector": by_sector,
            "sector_filter": sector,
            "quarter_start": quarter_start,
            "quarter_end": quarter_end,
            "has_value": bool(value_col),
        }

    def total_deal_value(self) -> dict[str, Any]:
        """Total deal value across all deals."""
        df = self.deals
        value_col = self._get_deal_value_col()
        if df.empty or not value_col:
            return {"total_value": 0.0, "count": 0, "insight": "No deal value data available."}
        total = float(df[value_col].sum())
        count = len(df[df[value_col].notna() & (df[value_col] > 0)])
        return {"total_value": total, "count": count}

    def best_performing_sector(self) -> dict[str, Any]:
        """Sector with highest total deal value (and optionally count)."""
        df = self.deals
        value_col = self._get_deal_value_col()
        if df.empty or "sector" not in df.columns:
            return {"sector": None, "value": 0.0, "insight": "No sector data available."}
        if not value_col:
            # Fall back to count-based performance
            by_sector = df.groupby("sector", dropna=False).size()
            if by_sector.empty:
                return {"sector": None, "count": 0, "insight": "No sector breakdown available."}
            top = by_sector.idxmax()
            return {
                "sector": top,
                "count": int(by_sector.max()),
                "by_sector": by_sector.to_dict(),
                "value_unavailable": True,
            }
        by_sector = df.groupby("sector", dropna=False)[value_col].sum()
        if by_sector.empty:
            return {"sector": None, "value": 0.0, "insight": "No sector breakdown available."}
        top = by_sector.idxmax()
        return {
            "sector": top,
            "value": float(by_sector[top]),
            "count": int(len(df[df["sector"] == top])),
            "by_sector": {k: float(v) for k, v in by_sector.items()},
            "insight": None,
        }

    def _format_currency(self, x: float) -> str:
        if x >= 1_000_000:
            return f"₹{x/1_000_000:.1f}M"
        if x >= 1_000:
            return f"₹{x/1_000:.1f}K"
        return f"₹{x:,.0f}"

    def _build_insight(self, intent: str, result: dict[str, Any]) -> str:
        """Turn analysis result into a short narrative insight."""
        if result.get("insight"):
            return result["insight"]

        if intent == "total_deal_value":
            total = result.get("total_value", 0)
            count = result.get("count", 0)
            return (
                f"Your total deal value is **{self._format_currency(total)}** across **{count}** deals. "
                + ("This is a strong pipeline to focus on closing." if total > 0 else "Consider updating deal values in the Deals board.")
            )

        if intent == "best_sector":
            sector = result.get("sector")
            count = result.get("count", 0)
            if sector is None:
                return "There isn't enough sector data to identify a top performer. Make sure deals have a sector or industry set."
            if result.get("value_unavailable"):
                return (
                    f"**{sector}** is your top sector by **deal count** with **{count}** deals. "
                    "To rank by value, add a numeric/currency column (e.g. Deal Value) to the Deals board so I can total it."
                )
            value = result.get("value", 0)
            return (
                f"**{sector}** is your best-performing sector with **{self._format_currency(value)}** in deal value "
                f"across **{count}** deals. Double down on this segment while maintaining diversity in the pipeline."
            )

        if intent == "pipeline":
            count = result.get("count", 0)
            total = result.get("total_value")
            sector = result.get("sector_filter")
            qs, qe = result.get("quarter_start"), result.get("quarter_end")
            period = "this quarter" if (qs and qe) else "overall"
            sector_phrase = f" for **{sector}**" if sector else ""
            if total is None:
                line = f"Your pipeline{sector_phrase} {period} shows **{count}** deals. "
                line += (
                    "I can’t total deal value yet because no numeric/currency deal value column was detected in the Deals board. "
                    "Add one (e.g. “Deal Value”) and reload."
                )
            else:
                line = (
                    f"Your pipeline{sector_phrase} {period} shows **{count}** deals worth **{self._format_currency(float(total))}** in total. "
                )
            by_sector = result.get("by_sector", {})
            if isinstance(by_sector, dict) and by_sector and not sector:
                parts = []
                for s, v in list(by_sector.items())[:5]:
                    if isinstance(v, dict):
                        parts.append(f"{s}: {v.get('count', 0)} deals ({self._format_currency(v.get('value', 0))})")
                    else:
                        parts.append(f"{s}: {v}")
                line += "Breakdown: " + "; ".join(parts) + "."
            elif sector:
                line += "Focus on moving these opportunities through the funnel."
            return line

        return "Here are the numbers; consider asking for a specific metric (e.g. total deal value, best sector, pipeline by sector/quarter)."

    def ask(self, question: str) -> str:
        """
        Answer a natural-language BI question.

        Returns a markdown string with insight and, where relevant, numbers.
        """
        q = question.strip().lower()
        if not q:
            return "Please ask a question about your deals or work orders (e.g. total deal value, pipeline by sector, best-performing sector)."

        # Intent detection
        intent = None
        if any(x in q for x in ["prioritize", "priority", "what should we do first", "focus on", "focus"]):
            intent = "prioritize_deals"
        if any(
            x in q
            for x in [
                "pipeline",
                "pipeline looking",
                "how is our pipeline",
                "deals this quarter",
                "pipeline for",
            ]
        ):
            intent = "pipeline"
        elif any(
            x in q for x in ["total deal value", "total value", "sum of deals", "deal value"]
        ):
            intent = "total_deal_value"
        elif any(
            x in q
            for x in [
                "best sector",
                "sector performing best",
                "top sector",
                "which sector",
                "performing best",
            ]
        ):
            intent = "best_sector"
        elif any(x in q for x in ["breakdown", "by status", "status breakdown", "by owner", "by sector", "top sectors"]):
            intent = "breakdown"

        if not intent:
            # Broader fallback: suggest examples, but avoid hard failure
            return (
                "I can help with things like:\n\n"
                "- **Prioritization**: “Which deals should we prioritize?”\n"
                "- **Pipeline**: “Show pipeline by sector” / “Pipeline for Energy this quarter”\n"
                "- **Breakdowns**: “Breakdown by status” / “Breakdown by owner”\n"
                "- **Performance**: “Which sector is performing best?”\n\n"
                "Try one of those phrasings (you can include a sector like Energy)."
            )

        sector = self._extract_sector(question)
        quarter_bounds = self._parse_quarter_from_text(question)
        q_start, q_end = (quarter_bounds if quarter_bounds else (None, None))

        result: dict[str, Any] = {}
        if intent == "prioritize_deals":
            result = self.prioritize_deals(sector=sector, quarter_start=q_start, quarter_end=q_end)
            insight = result.get("insight", "")
            table = self._as_markdown_table(result.get("rows", []))
            if table:
                return insight + "\n\n" + table
            return insight
        if intent == "pipeline":
            result = self.pipeline_by_sector_quarter(
                sector=sector, quarter_start=q_start, quarter_end=q_end
            )
        elif intent == "total_deal_value":
            result = self.total_deal_value()
        elif intent == "best_sector":
            result = self.best_performing_sector()
        elif intent == "breakdown":
            # Choose grouping based on question wording
            status_col = self._get_status_col()
            owner_col = self._get_owner_col()
            if "owner" in q and owner_col:
                bd = self.deals_breakdown(owner_col)
                return f"Deals by **Owner** (top 15 out of {bd.get('total', 0)}):\n\n{pd.Series(bd['breakdown']).to_markdown()}"
            if "status" in q and status_col:
                bd = self.deals_breakdown(status_col)
                return f"Deals by **Status** (top 15 out of {bd.get('total', 0)}):\n\n{pd.Series(bd['breakdown']).to_markdown()}"
            # default to sector
            bd = self.deals_breakdown("sector")
            return f"Deals by **Sector** (top 15 out of {bd.get('total', 0)}):\n\n{pd.Series(bd['breakdown']).to_markdown()}"

        return self._build_insight(intent, result)
