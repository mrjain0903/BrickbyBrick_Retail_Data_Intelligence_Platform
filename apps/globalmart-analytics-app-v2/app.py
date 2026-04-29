import os
import traceback
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import time
import json
from datetime import datetime

from database import fetch_kpi_data, run_query

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="GlobalMart Analytics Hub",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS ───────────────────────────────────────────────────────────────────────
def load_css():
    try:
        base_dir = os.path.dirname(__file__)
    except NameError:
        base_dir = os.getcwd()
    css_path = os.path.join(base_dir, "assets", "style.css")
    if os.path.exists(css_path):
        with open(css_path) as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

load_css()

# ── Extra inline CSS for Tabs + Genie Rich Response ──────────────────────────
st.markdown("""
<style>
/* ── Reduce top whitespace ── */
.stMainBlockContainer { padding-top: 1rem !important; }
section[data-testid="stMainBlockContainer"] > div { gap: 0.4rem !important; }

/* Big Box Tabs - Inline Override */
.stTabs [data-baseweb="tab-list"] {
    gap: 10px;
    padding: 8px;
    background: rgba(15, 23, 42, 0.85);
    border-radius: 18px;
    border: 1px solid rgba(255, 255, 255, 0.1);
    margin-bottom: 10px;
}
.stTabs [data-baseweb="tab"] {
    background: rgba(30, 41, 59, 0.7);
    border: 2px solid rgba(255, 255, 255, 0.1);
    border-radius: 14px;
    padding: 10px 20px;
    height: auto;
    min-height: 42px;
    white-space: nowrap;
}
.stTabs [data-baseweb="tab"] span {
    font-size: 1.15rem !important;
    font-weight: 700 !important;
    color: #94A3B8 !important;
}
.stTabs [data-baseweb="tab"]:hover {
    background: rgba(59, 130, 246, 0.15);
    border-color: rgba(59, 130, 246, 0.4);
}
.stTabs [data-baseweb="tab"]:hover span {
    color: #E2E8F0 !important;
}
.stTabs [data-baseweb="tab"][aria-selected="true"] {
    background: linear-gradient(135deg, rgba(59, 130, 246, 0.35), rgba(45, 212, 191, 0.2));
    border-color: rgba(59, 130, 246, 0.6);
    box-shadow: 0 6px 24px rgba(59, 130, 246, 0.25);
}
.stTabs [data-baseweb="tab"][aria-selected="true"] span {
    color: #FFFFFF !important;
}
.stTabs [data-baseweb="tab-highlight"] { display: none !important; }
.stTabs [data-baseweb="tab-border"]    { display: none !important; }

/* ── Genie Rich Response Components ── */
.genie-chart-box {
    background: rgba(30, 41, 59, 0.5);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 16px;
    padding: 20px 16px 8px;
    margin: 16px 0;
}
.genie-chart-title {
    color: #F8FAFC; font-size: 1.08rem; font-weight: 600;
    margin-bottom: 4px; padding-left: 4px;
}
.genie-source {
    color: #64748B; font-size: 0.72rem; text-align: right;
    padding: 4px 8px 0; margin-top: -4px;
}
.genie-narrative {
    background: rgba(30, 41, 59, 0.35);
    border-left: 3px solid #3B82F6;
    border-radius: 0 12px 12px 0;
    padding: 14px 18px; margin: 10px 0 14px;
    color: #E2E8F0; line-height: 1.7;
}
.genie-narrative ul, .genie-narrative ol { padding-left: 20px; margin: 6px 0; }
.genie-narrative li { margin-bottom: 4px; }
.genie-narrative strong, .genie-narrative b { color: #F8FAFC; }
.genie-stats-row {
    display: flex; gap: 12px; flex-wrap: wrap; margin: 10px 0;
}
.genie-stat-chip {
    background: rgba(59,130,246,0.12);
    border: 1px solid rgba(59,130,246,0.25);
    border-radius: 10px; padding: 10px 14px;
    font-size: 0.82rem; color: #CBD5E1;
    flex: 1; min-width: 140px; text-align: center;
}
.genie-stat-chip b { color: #F8FAFC; }

/* ── Follow-up suggestion pills ── */
.genie-followup-row {
    display: flex; gap: 8px; flex-wrap: wrap; margin: 12px 0 4px;
}
.genie-followup-pill {
    background: rgba(59,130,246,0.10);
    border: 1px solid rgba(59,130,246,0.25);
    border-radius: 20px; padding: 6px 16px;
    font-size: 0.82rem; color: #93C5FD;
    cursor: pointer; transition: all 0.2s;
}
.genie-followup-pill:hover {
    background: rgba(59,130,246,0.25);
    border-color: rgba(59,130,246,0.5);
    color: #DBEAFE;
}
/* ── Feedback row ── */
.genie-feedback-row {
    display: flex; align-items: center; gap: 8px;
    margin: 8px 0 4px; padding: 4px 0;
    border-top: 1px solid rgba(255,255,255,0.04);
}
.genie-feedback-row span {
    color: #64748B; font-size: 0.78rem;
}
/* ── Thinking section ── */
.genie-thinking {
    background: rgba(30,41,59,0.25);
    border: 1px solid rgba(255,255,255,0.05);
    border-radius: 10px; padding: 10px 14px;
    margin: 6px 0; font-size: 0.8rem; color: #64748B;
}
.genie-thinking b { color: #94A3B8; }
/* ── Pin badge ── */
.genie-pin-badge {
    display: inline-block; background: rgba(245,158,11,0.15);
    border: 1px solid rgba(245,158,11,0.3);
    border-radius: 6px; padding: 2px 8px;
    font-size: 0.72rem; color: #FBBF24;
    margin-left: 8px;
}
/* ── History item ── */
.genie-history-item {
    background: rgba(30,41,59,0.5);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 10px; padding: 10px 12px;
    margin: 4px 0; cursor: pointer;
    transition: all 0.15s;
}
.genie-history-item:hover {
    background: rgba(59,130,246,0.12);
    border-color: rgba(59,130,246,0.3);
}
.genie-history-item .title { color: #E2E8F0; font-size: 0.85rem; }
.genie-history-item .meta { color: #64748B; font-size: 0.7rem; margin-top: 2px; }
/* ── Compare columns ── */
.genie-compare-divider {
    border-left: 2px solid rgba(59,130,246,0.3);
    min-height: 200px;
}
</style>
""", unsafe_allow_html=True)

# ── Plotly dark config ────────────────────────────────────────────────────────
PLOT_LAYOUT = dict(
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    font_color="#CBD5E1",
    margin=dict(l=50, r=50, t=56, b=70),
    xaxis=dict(gridcolor="rgba(255,255,255,0.04)", zerolinecolor="rgba(255,255,255,0.06)"),
    yaxis=dict(gridcolor="rgba(255,255,255,0.04)", zerolinecolor="rgba(255,255,255,0.06)"),
)

CHART_HEIGHT = 480

# ══════════════════════════════════════════════════════════════════════════════
# ENHANCED GENIE — Rich multi-chart responses with auto-detection
# ══════════════════════════════════════════════════════════════════════════════
GENIE_SPACE_ID = "01f13d4f62c01f7ab3b42f2632c5acba"

_FORECAST_KEYWORDS = (
    "forecast", "predict", "projection", "projected", "project",
    "future", "next month", "next quarter", "next year", "next 6",
    "next 3", "next 12", "upcoming", "estimate", "outlook",
    "trend forward", "growth rate", "expected", "will be",
    "anticipate", "extrapolat",
)

_NON_FORECAST_MSG = (
    "\u26a0\ufe0f This tab is dedicated to **forecasting** questions only "
    "(predictions, projections, future trends).\n\n"
    "Please switch to the **\u2728 Ask Genie** tab for general data questions!"
)


def _is_forecast_question(prompt: str) -> bool:
    """Return True if the prompt is related to forecasting / predictions."""
    low = prompt.lower()
    return any(kw in low for kw in _FORECAST_KEYWORDS)


@st.cache_resource(show_spinner=False)
def _genie_client():
    """Cached WorkspaceClient — reused across reruns, avoids re-auth overhead."""
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient()


def genie_ask(prompt, conversation_id=None):
    """Ask Genie and return structured multi-part response.
    Automatically retries polling if the query result isn't ready yet."""
    try:
        w = _genie_client()

        # ── Enhance prompt for better accuracy on first message ──
        if conversation_id is None:
            enhanced = (
                f"{prompt}\n\n"
                "(Please provide the SQL query and result data. "
                "Use the gold layer tables for accurate results.)"
            )
            msg = w.genie.start_conversation_and_wait(
                space_id=GENIE_SPACE_ID, content=enhanced)
            cid = msg.conversation_id
        else:
            msg = w.genie.create_message_and_wait(
                space_id=GENIE_SPACE_ID, conversation_id=conversation_id,
                content=prompt)
            cid = conversation_id

        msg_id = getattr(msg, "id", None)

        # ── Retry loop: re-poll the message if query results aren't ready ──
        max_polls = 15
        for attempt in range(max_polls):
            parts = []
            query_pending = False
            attachments = getattr(msg, "attachments", None) or []
            print(f"[Genie] Attempt {attempt+1}: msg_id={msg_id}, cid={cid}, {len(attachments)} attachment(s)")

            for i, att in enumerate(attachments):
                # Minimal logging (verbose attrs dump removed for performance)

                # ── Text attachment ──
                t = getattr(att, "text", None)
                if t and getattr(t, "content", None):
                    parts.append({"type": "text", "content": str(t.content)})

                # ── Query attachment ──
                q = getattr(att, "query", None)
                if q:
                    sql_query = (getattr(q, "query", None)
                                 or getattr(q, "statement", None)
                                 or getattr(q, "sql", None)
                                 or getattr(q, "content", None))
                    description = (getattr(q, "description", None)
                                   or getattr(q, "title", None) or "")
                    df = None
                    att_id = (getattr(att, "id", None)
                              or getattr(att, "attachment_id", None))

                    if att_id and msg_id:
                        try:
                            result = w.genie.get_message_attachment_query_result(
                                space_id=GENIE_SPACE_ID,
                                conversation_id=cid,
                                message_id=msg_id,
                                attachment_id=att_id,
                            )
                            sr = result.statement_response
                            cols = [c.name for c in sr.manifest.schema.columns]
                            rows = sr.result.data_array or []
                            df = pd.DataFrame(rows, columns=cols)
                            for c in df.columns:
                                try:
                                    df[c] = pd.to_numeric(df[c])
                                except (ValueError, TypeError):
                                    pass
                            print(f"[Genie] API result: {len(df)} rows, cols={list(df.columns)}")
                        except Exception as api_err:
                            query_pending = True  # Query exists but result not ready yet
                            print(f"[Genie] Query not ready yet (attempt {attempt+1})")

                    if df is None and sql_query and not query_pending:
                        try:
                            df = run_query(sql_query)
                            for c in df.columns:
                                try:
                                    df[c] = pd.to_numeric(df[c])
                                except (ValueError, TypeError):
                                    pass
                        except Exception as sql_err:
                            print(f"[Genie] SQL fallback failed: {sql_err}")

                    if df is not None and not df.empty:
                        parts.append({
                            "type": "query", "sql": sql_query or "",
                            "df": df, "title": str(description),
                        })

            # Check if we got query results
            has_query = any(p["type"] == "query" for p in parts)
            if has_query:
                print(f"[Genie] Got data on attempt {attempt+1}")
                break
            # If no pending query and we've tried a few times, give up (text-only response)
            if not query_pending and attempt >= 3:
                print(f"[Genie] Text-only response, stopping retry")
                break
            if attempt == max_polls - 1:
                break

            # Query still executing — wait and re-poll
            print(f"[Genie] Query pending, retrying in 4s... (attempt {attempt+1}/{max_polls})")
            time.sleep(4)
            try:
                msg = w.genie.get_message(
                    space_id=GENIE_SPACE_ID,
                    conversation_id=cid,
                    message_id=msg_id)
            except Exception as poll_err:
                print(f"[Genie] Re-poll failed: {poll_err}")
                break

        if not parts:
            raw = getattr(msg, "content", None) or "_(No response from Genie)_"
            parts = [{"type": "text", "content": str(raw)}]
        return parts, cid

    except Exception as e:
        print(f"[Genie] Top-level error: {type(e).__name__}: {e}")
        print(traceback.format_exc())
        return [{"type": "text", "content": f"Genie error: {e}"}], conversation_id


# ══════════════════════════════════════════════════════════════════════════════
# SMART CHART AUTO-DETECTION  (fixes "index" x-axis bug)
# ══════════════════════════════════════════════════════════════════════════════

COLORS = ["#3B82F6", "#10B981", "#F59E0B", "#A78BFA", "#EF4444",
          "#2DD4BF", "#EC4899", "#8B5CF6", "#F97316", "#06B6D4"]

ALL_CHART_OPTIONS = [
    ("bar",     "📊 Bar"),
    ("stacked", "📊 Stacked"),
    ("hbar",    "📊 H-Bar"),
    ("line",    "📈 Line"),
    ("area",    "📉 Area"),
    ("combo",   "📊 Combo"),
    ("pie",     "🍩 Pie"),
    ("scatter", "🔵 Scatter"),
]

_ID_HINTS = ("id", "key", "code", "customer", "name", "vendor", "product",
             "order", "region", "segment", "category", "status", "reason")


def _detect_date_col(df):
    for c in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[c]):
            return c
        clow = c.lower()
        if any(kw in clow for kw in ("date", "month", "year_month", "year",
                                      "time", "day", "week", "quarter", "period")):
            return c
    return None


def _detect_label_col(df):
    """Find the best label/category column — prefers string cols, then ID-like numeric cols.
    Uses a cached attribute on the DataFrame to avoid repeated mutation."""
    # Return cached result if already computed for this DataFrame
    cached = getattr(df, "_label_col_cache", None)
    if cached is not None:
        return cached if cached != "__none__" else None

    str_cols = df.select_dtypes(exclude="number").columns.tolist()
    if str_cols:
        result = str_cols[0]
    else:
        result = None
        # Fallback: look for ID-like numeric columns and convert to string
        for c in df.columns:
            clow = c.lower()
            if any(hint in clow for hint in _ID_HINTS):
                df[c] = df[c].astype(str)
                result = c
                break
        # Last resort: if first column has few unique values, treat as label
        if result is None:
            first = df.columns[0]
            if df[first].nunique() <= 30:
                df[first] = df[first].astype(str)
                result = first

    # Cache on the DataFrame object
    try:
        df._label_col_cache = result if result is not None else "__none__"
    except Exception:
        pass
    return result



def _ensure_date_readable(df, col):
    """Convert raw epoch timestamps to proper datetime in-place."""
    if col is None or col not in df.columns:
        return
    # Already datetime — nothing to do
    if pd.api.types.is_datetime64_any_dtype(df[col]):
        return
    # Numeric column with large values → likely epoch timestamp
    if pd.api.types.is_numeric_dtype(df[col]):
        sample = df[col].dropna()
        if sample.empty:
            return
        val = abs(float(sample.iloc[0]))
        try:
            if val > 1e17:          # nanoseconds
                df[col] = pd.to_datetime(df[col], unit="ns", errors="coerce")
            elif val > 1e14:        # microseconds
                df[col] = pd.to_datetime(df[col], unit="us", errors="coerce")
            elif val > 1e11:        # milliseconds
                df[col] = pd.to_datetime(df[col], unit="ms", errors="coerce")
            elif val > 1e8:         # seconds
                df[col] = pd.to_datetime(df[col], unit="s", errors="coerce")
        except Exception:
            pass
        # Format as readable string for chart labels
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%b %Y")
    # String that looks like a timestamp number → try conversion
    elif df[col].dtype == object:
        try:
            numeric = pd.to_numeric(df[col], errors="coerce")
            if numeric.notna().any():
                val = abs(float(numeric.dropna().iloc[0]))
                if val > 1e8:
                    df[col] = numeric
                    _ensure_date_readable(df, col)  # recurse with numeric col
        except Exception:
            pass

def _auto_chart_type(df):
    if df is None or df.empty or len(df) < 2:
        return None
    num_cols = df.select_dtypes(include="number").columns.tolist()
    if not num_cols:
        return None
    date_col = _detect_date_col(df)
    if date_col:
        _ensure_date_readable(df, date_col)
        return "line"
    label_col = _detect_label_col(df)
    num_cols = df.select_dtypes(include="number").columns.tolist()  # refresh after conversion
    if label_col and len(num_cols) == 1 and 2 <= len(df) <= 6:
        return "pie"
    if label_col and len(df) > 10:
        return "hbar"
    if label_col and len(num_cols) >= 2 and len(df) <= 15:
        return "combo"
    if label_col and num_cols:
        return "bar"
    if len(num_cols) >= 2:
        return "scatter"
    return "bar"


def _build_smart_figure(df, chart_type="bar"):
    """Build Plotly figure. Handles all-numeric DataFrames by detecting label cols."""
    num_cols = df.select_dtypes(include="number").columns.tolist()
    label_col = _detect_label_col(df)
    date_col = _detect_date_col(df)
    x_col = date_col or label_col
    # Convert raw timestamps to readable dates
    if date_col:
        _ensure_date_readable(df, date_col)
    # Refresh num_cols after potential conversion
    num_cols = df.select_dtypes(include="number").columns.tolist()
    pretty = lambda s: s.replace("_", " ").title() if s else ""

    base_layout = dict(
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        font_color="#CBD5E1", margin=dict(l=50, r=30, t=48, b=60),
    )

    fig = None

    # ── Bar ──
    if chart_type == "bar":
        if x_col:
            fig = px.bar(df, x=x_col, y=num_cols[:4], barmode="group",
                         template="plotly_dark", color_discrete_sequence=COLORS,
                         labels={x_col: pretty(x_col)})
        elif num_cols:
            fig = px.bar(df, x=df.index.astype(str), y=num_cols[0],
                         template="plotly_dark", color_discrete_sequence=COLORS,
                         labels={"x": "Row", num_cols[0]: pretty(num_cols[0])})

    # ── Stacked Bar ──
    elif chart_type == "stacked":
        if x_col and len(num_cols) >= 2:
            fig = px.bar(df, x=x_col, y=num_cols[:6], barmode="stack",
                         template="plotly_dark", color_discrete_sequence=COLORS,
                         labels={x_col: pretty(x_col)})
        elif x_col:
            fig = px.bar(df, x=x_col, y=num_cols[0], template="plotly_dark",
                         color_discrete_sequence=COLORS, labels={x_col: pretty(x_col)})
        elif num_cols:
            fig = px.bar(df, y=num_cols[0], template="plotly_dark",
                         color_discrete_sequence=COLORS)

    # ── Horizontal Bar ──
    elif chart_type == "hbar":
        display_df = df.head(15)
        if x_col and num_cols:
            display_df = display_df.sort_values(num_cols[0], ascending=True)
            fig = px.bar(display_df, y=x_col, x=num_cols[0], orientation="h",
                         template="plotly_dark", color=num_cols[0],
                         color_continuous_scale=[[0,"#1E3A5F"],[1,"#3B82F6"]],
                         labels={x_col: pretty(x_col), num_cols[0]: pretty(num_cols[0])})
            fig.update_layout(coloraxis_showscale=False)
            fig.update_traces(texttemplate="%{x:,.0f}", textposition="outside",
                              textfont=dict(size=10, color="#94A3B8"))
        elif num_cols:
            fig = px.bar(df.head(15), y=num_cols[0], template="plotly_dark",
                         color_discrete_sequence=COLORS)

    # ── Line ──
    elif chart_type == "line":
        if x_col:
            fig = px.line(df, x=x_col, y=num_cols[:4], template="plotly_dark",
                          markers=True, color_discrete_sequence=COLORS,
                          labels={x_col: pretty(x_col)})
            fig.update_traces(line=dict(width=2.5))
        elif num_cols:
            fig = px.line(df, y=num_cols[:4], template="plotly_dark",
                          markers=True, color_discrete_sequence=COLORS)

    # ── Area ──
    elif chart_type == "area":
        if x_col:
            fig = px.area(df, x=x_col, y=num_cols[:4], template="plotly_dark",
                          color_discrete_sequence=COLORS, labels={x_col: pretty(x_col)})
        elif num_cols:
            fig = px.area(df, y=num_cols[:4], template="plotly_dark",
                          color_discrete_sequence=COLORS)

    # ── Combo (Bar + Line dual axis) ──
    elif chart_type == "combo":
        if x_col and len(num_cols) >= 2:
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(
                go.Bar(x=df[x_col], y=df[num_cols[0]], name=pretty(num_cols[0]),
                       marker_color=COLORS[0], opacity=0.85),
                secondary_y=False)
            fig.add_trace(
                go.Scatter(x=df[x_col], y=df[num_cols[1]], name=pretty(num_cols[1]),
                           mode="lines+markers", line=dict(color=COLORS[1], width=2.5),
                           marker=dict(size=7)),
                secondary_y=True)
            if len(num_cols) >= 3:
                fig.add_trace(
                    go.Scatter(x=df[x_col], y=df[num_cols[2]], name=pretty(num_cols[2]),
                               mode="lines+markers",
                               line=dict(color=COLORS[2], width=2, dash="dot"),
                               marker=dict(size=5)),
                    secondary_y=True)
            fig.update_yaxes(title_text=pretty(num_cols[0]), secondary_y=False,
                             gridcolor="rgba(255,255,255,0.04)")
            fig.update_yaxes(title_text=pretty(num_cols[1]), secondary_y=True,
                             gridcolor="rgba(0,0,0,0)")
            fig.update_xaxes(title_text=pretty(x_col))
            fig.update_layout(template="plotly_dark",
                              legend=dict(bgcolor="rgba(0,0,0,0)", orientation="h", y=1.08))
        elif x_col:
            fig = px.bar(df, x=x_col, y=num_cols[0], template="plotly_dark",
                         color_discrete_sequence=COLORS)
        elif num_cols:
            fig = px.bar(df, y=num_cols[0], template="plotly_dark",
                         color_discrete_sequence=COLORS)

    # ── Pie ──
    elif chart_type == "pie":
        if label_col and num_cols:
            fig = px.pie(df, names=label_col, values=num_cols[0], hole=0.45,
                         template="plotly_dark", color_discrete_sequence=COLORS)
            fig.update_traces(textinfo="label+percent+value",
                              textfont=dict(color="#CBD5E1", size=11))
            fig.update_layout(margin=dict(l=20, r=20, t=48, b=20))
        elif num_cols:
            fig = px.bar(df, y=num_cols[0], template="plotly_dark",
                         color_discrete_sequence=COLORS)

    # ── Scatter ──
    elif chart_type == "scatter":
        if len(num_cols) >= 2:
            size_col = num_cols[2] if len(num_cols) > 2 else None
            fig = px.scatter(df, x=num_cols[0], y=num_cols[1],
                             size=size_col,
                             color=label_col if label_col else None,
                             template="plotly_dark", color_discrete_sequence=COLORS,
                             labels={num_cols[0]: pretty(num_cols[0]),
                                     num_cols[1]: pretty(num_cols[1])})
        elif num_cols and x_col:
            fig = px.scatter(df, x=x_col, y=num_cols[0],
                             template="plotly_dark", color_discrete_sequence=COLORS,
                             labels={x_col: pretty(x_col), num_cols[0]: pretty(num_cols[0])})
        elif num_cols:
            fig = px.scatter(df, x=df.index, y=num_cols[0],
                             template="plotly_dark", color_discrete_sequence=COLORS,
                             labels={"x": "Index", num_cols[0]: pretty(num_cols[0])})

    # ── Fallback ──
    if fig is None:
        col = num_cols[0] if num_cols else df.columns[0]
        fig = px.bar(df, y=col, template="plotly_dark", color_discrete_sequence=COLORS)

    fig.update_layout(height=420, **base_layout)
    fig.update_layout(legend=dict(bgcolor="rgba(0,0,0,0)", orientation="h", y=1.08))
    return fig


# ── Response helpers ──────────────────────────────────────────────────────────

def _generate_summary(df):
    num_cols = df.select_dtypes(include="number").columns.tolist()
    lines = []
    for nc in num_cols[:3]:
        val = df[nc]
        lines.append(f"{nc.replace('_', ' ').title()} — "
                     f"Total: {val.sum():,.2f} · Avg: {val.mean():,.2f} · "
                     f"Min: {val.min():,.2f} · Max: {val.max():,.2f}")
    return "\n".join(lines) if lines else ""


def _generate_key_findings(df):
    num_cols = df.select_dtypes(include="number").columns.tolist()
    str_cols = df.select_dtypes(exclude="number").columns.tolist()
    if not num_cols or df.empty:
        return ""
    findings = []
    primary = num_cols[0]
    pretty = lambda s: s.replace("_", " ").title() if s else ""
    if str_cols:
        cat = str_cols[0]
        top_i = df[primary].idxmax()
        bot_i = df[primary].idxmin()
        findings.append(f"Highest {pretty(primary)}: {df.loc[top_i, cat]} ({df.loc[top_i, primary]:,.2f})")
        if len(df) > 1:
            findings.append(f"Lowest {pretty(primary)}: {df.loc[bot_i, cat]} ({df.loc[bot_i, primary]:,.2f})")
    total = df[primary].sum()
    avg = df[primary].mean()
    findings.append(f"Total: {total:,.2f}  ·  Average: {avg:,.2f}")

    return "\n\n".join(findings)


def _build_stat_chips_html(df):
    num_cols = df.select_dtypes(include="number").columns.tolist()
    if not num_cols:
        return ""
    chips = []
    for nc in num_cols[:4]:
        val = df[nc]
        label = nc.replace("_", " ").title()
        chips.append(
            f'<div class="genie-stat-chip"><b>{label}</b><br>'
            f'Total: {val.sum():,.0f} \u00b7 Avg: {val.mean():,.1f}</div>')
    return f'<div class="genie-stats-row">{"".join(chips)}</div>'




def _generate_followups(df, title=""):
    """Auto-generate 2-3 clickable follow-up questions based on the data."""
    suggestions = []
    str_cols = df.select_dtypes(exclude="number").columns.tolist()
    num_cols = df.select_dtypes(include="number").columns.tolist()
    date_col = _detect_date_col(df)
    pretty = lambda s: s.replace("_", " ").title() if s else ""

    # Drill into top performer
    if str_cols and num_cols:
        cat = str_cols[0]
        primary = num_cols[0]
        top_val = df.loc[df[primary].idxmax(), cat]
        suggestions.append(f"Drill into {top_val} details")
    # Time-based follow-up
    if date_col:
        suggestions.append("Compare with previous year trends")
    # Breakdown suggestion
    if num_cols and not date_col:
        suggestions.append(f"Show {pretty(num_cols[0])} breakdown by region")
    # Top/bottom
    if len(df) > 5:
        suggestions.append("Show only top 5 performers")
    # Profitability
    if any("sale" in c.lower() or "revenue" in c.lower() for c in num_cols):
        suggestions.append("What is the profit margin analysis?")
    # Return-related
    if any("return" in c.lower() or "refund" in c.lower() for c in df.columns):
        suggestions.append("Which customers have the most returns?")

    return suggestions[:3]


def _render_thinking_section(df, chart_type, title, elapsed_secs=0):
    """Render a 'Thinking complete' collapsible section."""
    num_cols = df.select_dtypes(include="number").columns.tolist()
    str_cols = df.select_dtypes(exclude="number").columns.tolist()
    lines = [
        f"Analyzed {len(df)} rows x {len(df.columns)} columns",
        f"Detected columns: {', '.join(df.columns[:8])}{'...' if len(df.columns) > 8 else ''}",
        f"Numeric fields: {', '.join(num_cols[:5])}",
        f"Category fields: {', '.join(str_cols[:5]) if str_cols else 'None'}",
        f"Auto-selected chart: {chart_type.upper()}",
    ]
    if elapsed_secs > 0:
        lines.append(f"Response time: {elapsed_secs:.1f}s")
    html = '<div class="genie-thinking">' + "<br>".join(f"<b>›</b> {l}" for l in lines) + "</div>"
    return html


def _export_conversation_html(messages):
    """Generate an HTML export of the full conversation."""
    html_parts = [
        "<!DOCTYPE html><html><head><meta charset='utf-8'>",
        "<title>Genie Conversation Export</title>",
        "<style>body{font-family:system-ui;background:#0F172A;color:#E2E8F0;padding:40px;max-width:900px;margin:auto}",
        ".msg{margin:20px 0;padding:16px;border-radius:12px}",
        ".user{background:rgba(59,130,246,0.15);border-left:3px solid #3B82F6}",
        ".assistant{background:rgba(30,41,59,0.5);border-left:3px solid #10B981}",
        ".role{font-size:0.75rem;color:#94A3B8;margin-bottom:6px;text-transform:uppercase}",
        "table{border-collapse:collapse;width:100%;margin:12px 0}th,td{border:1px solid #334155;padding:8px;text-align:left}",
        "th{background:#1E293B}pre{background:#1E293B;padding:12px;border-radius:8px;overflow-x:auto}",
        ".meta{color:#64748B;font-size:0.8rem;text-align:center;margin-top:30px}</style></head><body>",
        f"<h1>Genie Conversation</h1><p style='color:#64748B'>Exported: {datetime.now().strftime('%Y-%m-%d %H:%M')}</p><hr>",
    ]
    for msg in messages:
        role = msg["role"]
        css_class = "user" if role == "user" else "assistant"
        html_parts.append(f'<div class="msg {css_class}"><div class="role">{role}</div>')
        for part in msg.get("parts", []):
            if part["type"] == "text":
                text = part["content"].replace("**", "").replace("\n", "<br>")
                html_parts.append(f"<p>{text}</p>")
            elif part["type"] == "query":
                title = part.get("title", "")
                if title:
                    html_parts.append(f"<h4>{title}</h4>")
                df = part.get("df")
                if df is not None and not df.empty:
                    html_parts.append(df.head(20).to_html(index=False, classes="data-table"))
                sql = part.get("sql", "")
                if sql:
                    html_parts.append(f"<pre>{sql}</pre>")
        html_parts.append("</div>")
    html_parts.append(f'<div class="meta">GlobalMart Analytics Hub - Genie Export</div></body></html>')
    return "\n".join(html_parts)


# ══════════════════════════════════════════════════════════════════════════════
# RENDER RICH RESPONSE — Genie-like interleaved text, charts, findings
# ══════════════════════════════════════════════════════════════════════════════

def render_rich_response(parts, msg_idx):
    """Render Genie response with charts, findings, feedback, thinking, follow-ups, pins."""
    chart_counter = 0
    last_df = None
    last_title = ""

    for part in parts:
        # ── Text block ──
        if part["type"] == "text":
            txt = part["content"].replace("**", "")
            if any(ch in txt for ch in ("\n", "- ", "* ", "# ", "|")):
                st.markdown(f'<div class="genie-narrative">{txt}</div>',
                            unsafe_allow_html=True)
            else:
                st.markdown(txt)

        # ── Query result with auto-chart ──
        elif part["type"] == "query":
            df = part["df"]
            sql = part.get("sql", "")
            title = part.get("title", "")
            last_df = df
            last_title = title

            auto_type = _auto_chart_type(df)
            if auto_type:
                chart_key = f"genie_chart_{msg_idx}_{chart_counter}"
                if chart_key not in st.session_state:
                    st.session_state[chart_key] = auto_type

                st.markdown('<div class="genie-chart-box">', unsafe_allow_html=True)
                if title:
                    st.markdown(f'<div class="genie-chart-title">{title}</div>',
                                unsafe_allow_html=True)

                toggle_cols = st.columns(len(ALL_CHART_OPTIONS) + 1)
                for i, (key, label) in enumerate(ALL_CHART_OPTIONS):
                    with toggle_cols[i]:
                        is_active = st.session_state[chart_key] == key
                        if st.button(label, key=f"ct_{msg_idx}_{chart_counter}_{i}",
                                     use_container_width=True,
                                     type="primary" if is_active else "secondary"):
                            st.session_state[chart_key] = key
                            st.rerun()

                fig = _build_smart_figure(df, st.session_state[chart_key])
                st.plotly_chart(fig, use_container_width=True,
                                key=f"gc_{msg_idx}_{chart_counter}")

                source_label = title if title else "Query result"
                st.markdown(f'<div class="genie-source">Source: \U0001f4ca {source_label}</div>',
                            unsafe_allow_html=True)
                st.markdown('</div>', unsafe_allow_html=True)

                # ── Thinking indicator ──
                with st.expander("\U0001f9e0 Thinking complete", expanded=False):
                    thinking_html = _render_thinking_section(
                        df, st.session_state[chart_key], title,
                        st.session_state.get(f"genie_time_{msg_idx}", 0))
                    st.markdown(thinking_html, unsafe_allow_html=True)

                # ── Chart annotation ──
                ann_key = f"ann_{msg_idx}_{chart_counter}"
                note = st.text_input("\U0001f4dd Add note to this chart",
                                     value=st.session_state.get(ann_key, ""),
                                     key=f"ann_input_{msg_idx}_{chart_counter}",
                                     placeholder="Type a note...")
                if note:
                    st.session_state[ann_key] = note

            elif title:
                st.caption(title)

            # ── Stat chips ──
            chips = _build_stat_chips_html(df)
            if chips:
                st.markdown(chips, unsafe_allow_html=True)

            # ── Key findings ──
            findings = _generate_key_findings(df)
            if findings:
                with st.expander("\U0001f4a1 Key findings", expanded=True):
                    st.markdown(findings)

            # ── Data table + CSV ──
            with st.expander(f"\U0001f4cb View data table ({len(df)} rows)"):
                st.dataframe(df, use_container_width=True, hide_index=True)
                csv_data = df.to_csv(index=False)
                st.download_button("\u2b07\ufe0f Download CSV", csv_data,
                                   file_name=f"genie_{msg_idx}_{chart_counter}.csv",
                                   mime="text/csv",
                                   key=f"dl_{msg_idx}_{chart_counter}")

            # ── SQL ──
            if sql:
                with st.expander("\U0001f50d View SQL query"):
                    st.code(sql, language="sql")

            chart_counter += 1

    # ═══ After all parts: Feedback + Follow-ups + Pin (assistant msgs only) ═══
    if msg_idx > 0 and any(p["type"] == "query" for p in parts):
        st.markdown('<div class="genie-feedback-row">', unsafe_allow_html=True)
        fb_key = f"fb_{msg_idx}"
        c1, c2, c3, c4, c5 = st.columns([1, 1, 1, 1, 6])
        with c1:
            if st.button("\U0001f44d", key=f"fb_up_{msg_idx}",
                         type="primary" if st.session_state.get(fb_key) == "up" else "secondary"):
                st.session_state[fb_key] = "up"
                st.rerun()
        with c2:
            if st.button("\U0001f44e", key=f"fb_dn_{msg_idx}",
                         type="primary" if st.session_state.get(fb_key) == "down" else "secondary"):
                st.session_state[fb_key] = "down"
                st.rerun()
        with c3:
            is_pinned = msg_idx in st.session_state.get("genie_pins", [])
            pin_label = "\U0001f4cc Pinned" if is_pinned else "\U0001f4cc Pin"
            if st.button(pin_label, key=f"pin_{msg_idx}",
                         type="primary" if is_pinned else "secondary"):
                pins = st.session_state.get("genie_pins", [])
                if msg_idx in pins:
                    pins.remove(msg_idx)
                else:
                    pins.append(msg_idx)
                st.session_state["genie_pins"] = pins
                st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)




# ══════════════════════════════════════════════════════════════════════════════
# DASHBOARD COMPONENTS (Overview, Returns, Vendors)
# ══════════════════════════════════════════════════════════════════════════════

def kpi_card(icon, title, value, sub="", accent=""):
    st.markdown(
        f'<div class="kpi-card">'
        f'<span class="kpi-icon">{icon}</span>'
        f'<div class="kpi-title">{title}</div>'
        f'<div class="kpi-value {accent}">{value}</div>'
        f'{"<div class=kpi-sub>" + sub + "</div>" if sub else ""}'
        f'</div>',
        unsafe_allow_html=True)


def chart_region(df):
    fig = px.bar(df, x="region_name", y="revenue", color="revenue",
                 color_continuous_scale=[[0,"#1E3A5F"],[0.5,"#3B82F6"],[1,"#2DD4BF"]],
                 template="plotly_dark",
                 labels={"region_name":"Region","revenue":"Revenue ($)"})
    fig.add_scatter(x=df["region_name"], y=df["profit"],
                    mode="lines+markers", name="Profit",
                    line=dict(color="#A78BFA", width=2), marker=dict(size=8))
    fig.update_layout(title="Revenue & Profit by Region",
                      coloraxis_showscale=False, height=CHART_HEIGHT, **PLOT_LAYOUT)
    fig.update_layout(legend=dict(bgcolor="rgba(0,0,0,0)", font_color="#94A3B8"))
    return fig


def chart_segments(df):
    colors = {"Consumer":"#10B981","Corporate":"#3B82F6","Home Office":"#F59E0B","N/A":"#64748B"}
    fig = go.Figure(go.Pie(
        labels=df["customer_segment"], values=df["cnt"], hole=0.55,
        marker=dict(colors=[colors.get(n,"#94A3B8") for n in df["customer_segment"]],
                    line=dict(color="#0F172A", width=2)),
        textinfo="label+percent", textfont=dict(color="#CBD5E1", size=12)))
    fig.update_layout(title="Customer Segments", showlegend=False, height=CHART_HEIGHT,
                      margin=dict(l=20,r=20,t=56,b=20),
                      plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                      font_color="#CBD5E1")
    return fig


def chart_monthly(df):
    if df.empty or "year_month" not in df.columns:
        fig = go.Figure()
        fig.add_annotation(text="No monthly data", showarrow=False, font_color="#64748B")
        fig.update_layout(**PLOT_LAYOUT)
        return fig
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(x=df["year_month"], y=df["total_sales"],
                         name="Revenue", marker_color="#3B82F6", opacity=0.85),
                  secondary_y=False)
    fig.add_trace(go.Scatter(x=df["year_month"], y=df["total_profit"],
                             name="Profit", line=dict(color="#10B981", width=2.5),
                             mode="lines+markers", marker=dict(size=5)),
                  secondary_y=False)
    fig.add_trace(go.Scatter(x=df["year_month"], y=df["order_count"],
                             name="Orders", line=dict(color="#F59E0B", width=2, dash="dot"),
                             mode="lines"), secondary_y=True)
    fig.update_layout(title="Monthly Sales Trend", height=CHART_HEIGHT, **PLOT_LAYOUT)
    fig.update_layout(legend=dict(bgcolor="rgba(0,0,0,0)", orientation="h", y=1.08))
    fig.update_yaxes(title_text="Revenue ($)", secondary_y=False,
                     gridcolor="rgba(255,255,255,0.04)")
    fig.update_yaxes(title_text="Orders", secondary_y=True, gridcolor="rgba(0,0,0,0)")
    return fig


def chart_return_reasons(df):
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(text="No data", showarrow=False, font_color="#64748B")
        fig.update_layout(**PLOT_LAYOUT)
        return fig
    fig = px.bar(df.head(10), x="total", y="product_name", orientation="h",
                 color="total",
                 color_continuous_scale=[[0,"#1E3A5F"],[0.5,"#EF4444"],[1,"#F87171"]],
                 template="plotly_dark", text="total",
                 labels={"total":"Count","product_name":""})
    fig.update_traces(texttemplate="%{text:,}", textposition="outside",
                      textfont=dict(color="#94A3B8", size=10))
    fig.update_layout(title="Top Return Reasons", coloraxis_showscale=False,
                      height=CHART_HEIGHT, **PLOT_LAYOUT)
    fig.update_yaxes(categoryorder="total ascending")
    return fig


def chart_status_treemap(df):
    if df.empty or df["revenue"].fillna(0).sum() == 0:
        fig = go.Figure()
        fig.add_annotation(text="No data", showarrow=False, font_color="#64748B")
        fig.update_layout(**PLOT_LAYOUT)
        return fig
    fig = px.treemap(df, path=["category"], values="revenue", color="revenue",
                     color_continuous_scale=[[0,"#1E3A5F"],[1,"#2DD4BF"]],
                     template="plotly_dark")
    fig.update_layout(title="Refund Value by Status", coloraxis_showscale=False,
                      height=CHART_HEIGHT, **PLOT_LAYOUT)
    fig.update_traces(textfont_color="#F8FAFC")
    return fig


def chart_vendor(df):
    if df.empty or "vendor_name" not in df.columns:
        fig = go.Figure()
        fig.add_annotation(text="No vendor data", showarrow=False, font_color="#64748B")
        fig.update_layout(**PLOT_LAYOUT)
        return fig
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(x=df["vendor_name"], y=df["vendor_gross_sales"],
                         name="Revenue", marker_color="#3B82F6", opacity=0.85),
                  secondary_y=False)
    fig.add_trace(go.Scatter(x=df["vendor_name"], y=df["refund_pct"],
                             name="Margin %", mode="lines+markers",
                             line=dict(color="#10B981", width=2.5),
                             marker=dict(size=8, color="#10B981")),
                  secondary_y=True)
    fig.update_layout(title="Vendor Sales Performance", height=CHART_HEIGHT, **PLOT_LAYOUT)
    fig.update_layout(legend=dict(bgcolor="rgba(0,0,0,0)", orientation="h", y=1.08))
    fig.update_xaxes(tickangle=-35)
    fig.update_yaxes(title_text="Revenue ($)", secondary_y=False,
                     gridcolor="rgba(255,255,255,0.04)")
    fig.update_yaxes(title_text="Margin %", secondary_y=True, gridcolor="rgba(0,0,0,0)")
    return fig


# ── Sidebar ───────────────────────────────────────────────────────────────────
def render_sidebar():
    st.sidebar.markdown(
        "<h2 style='color:#F8FAFC;margin-bottom:4px;'>🛒 GlobalMart</h2>"
        "<p style='color:#64748B;font-size:0.78rem;margin-top:0;'>Analytics Hub · Gold Layer</p>",
        unsafe_allow_html=True)
    st.sidebar.markdown("---")
    regions = st.sidebar.multiselect("📍 Regions",
        ["North","South","East","West","Central"],
        default=["North","South","East","West","Central"])
    segments = st.sidebar.multiselect("👥 Segment",
        ["Consumer","Corporate","Home Office"],
        default=["Consumer","Corporate","Home Office"])

    # ── Conversation History ──
    st.sidebar.markdown("---")
    st.sidebar.markdown("<p style='color:#94A3B8;font-size:0.85rem;font-weight:600;'>💬 Conversation History</p>",
                        unsafe_allow_html=True)
    if "genie_history" not in st.session_state:
        st.session_state.genie_history = []

    # Save current conversation button
    if st.sidebar.button("💾 Save Current Chat", use_container_width=True):
        msgs = st.session_state.get("genie_msgs", [])
        if len(msgs) > 1:
            first_user = next((p["content"] for m in msgs for p in m.get("parts", [])
                               if m["role"] == "user" and p["type"] == "text"), "Untitled")
            entry = {
                "title": first_user[:50],
                "timestamp": datetime.now().strftime("%b %d, %H:%M"),
                "msg_count": len(msgs),
                "cid": st.session_state.get("genie_cid"),
            }
            st.session_state.genie_history.append(entry)
            st.sidebar.success("Chat saved!")

    # Show history
    for hi, h in enumerate(reversed(st.session_state.genie_history)):
        st.sidebar.markdown(
            f'<div class="genie-history-item">'
            f'<div class="title">{h["title"]}</div>'
            f'<div class="meta">{h["timestamp"]} · {h["msg_count"]} messages</div>'
            f'</div>', unsafe_allow_html=True)

    # ── Pinned Responses ──
    pins = st.session_state.get("genie_pins", [])
    if pins:
        st.sidebar.markdown("---")
        st.sidebar.markdown("<p style='color:#FBBF24;font-size:0.85rem;font-weight:600;'>📌 Pinned Responses</p>",
                            unsafe_allow_html=True)
        msgs = st.session_state.get("genie_msgs", [])
        for pi in pins:
            if pi < len(msgs):
                msg = msgs[pi]
                preview = ""
                for p in msg.get("parts", []):
                    if p["type"] == "text":
                        preview = p["content"][:60].replace("**", "")
                        break
                    elif p["type"] == "query":
                        preview = p.get("title", "Query result")[:60]
                        break
                st.sidebar.markdown(
                    f'<div class="genie-history-item">'
                    f'<div class="title">📌 {preview}</div>'
                    f'</div>', unsafe_allow_html=True)

    return regions, segments


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    regions, segments = render_sidebar()

    st.markdown(
        "<h1 style='color:#F8FAFC;margin:0 0 2px 0;padding:0;font-size:1.8rem;font-weight:800;"
        "background:linear-gradient(135deg,#F8FAFC,#3B82F6);-webkit-background-clip:text;"
        "-webkit-text-fill-color:transparent;'>🛒 GlobalMart Analytics Hub</h1>",
        unsafe_allow_html=True)

    try:
        kpis = fetch_kpi_data(regions, segments)
    except Exception as e:
        st.error(f"Failed to load data: {e}")
        st.stop()

    if kpis.get("query_errors"):
        with st.expander("Query warnings"):
            for err in kpis["query_errors"]:
                st.warning(err)

    tab_overview, tab_returns, tab_vendors, tab_genie, tab_forecast = st.tabs([
        "📊 Overview", "🔄 Returns", "🏭 Vendors", "✨ Ask Genie", "🔮 Forecasting"
    ])

    # ═══════════════ TAB 1 — OVERVIEW ═══════════════
    with tab_overview:
        c1, c2, c3, c4, c5 = st.columns(5)
        with c1: kpi_card("💰", "Total Revenue", f"${kpis['total_revenue']:,.0f}", accent="text-positive")
        with c2: kpi_card("📦", "Total Orders",  f"{kpis['total_orders']:,}", accent="text-blue")
        with c3: kpi_card("📈", "Gross Profit",  f"${kpis['total_profit']:,.0f}",
                          sub=f"Margin: {kpis['profit_margin_pct']}%", accent="text-teal")
        with c4: kpi_card("⚠️", "High-Risk Customers", f"{kpis['high_risk_customers']:,}",
                          sub="≥5 returns", accent="text-danger")
        with c5: kpi_card("⏳", "Pending Returns", f"{kpis['slow_moving_products']:,}", accent="text-warning")

        st.markdown("<hr class='dash-divider'/>", unsafe_allow_html=True)
        col_a, col_b = st.columns([3, 2])
        with col_a: st.plotly_chart(chart_region(kpis["revenue_by_region"]), use_container_width=True)
        with col_b: st.plotly_chart(chart_segments(kpis["customer_segments"]), use_container_width=True)
        st.markdown("<hr class='dash-divider'/>", unsafe_allow_html=True)
        st.plotly_chart(chart_monthly(kpis["monthly_trend"]), use_container_width=True)

    # ═══════════════ TAB 2 — RETURNS ═══════════════
    with tab_returns:
        col_a, col_b = st.columns([3, 2])
        with col_a: st.plotly_chart(chart_return_reasons(kpis["top_products"]), use_container_width=True)
        with col_b: st.plotly_chart(chart_status_treemap(kpis["category_perf"]), use_container_width=True)
        st.markdown("<hr class='dash-divider'/>", unsafe_allow_html=True)

        if not kpis["top_products"].empty:
            st.markdown("**Return Reasons Detail**")
            display = kpis["top_products"].rename(columns={
                "product_name": "Reason", "total": "Count", "units_sold": "Total Refund ($)"})
            st.dataframe(display.style
                .format({"Count": "{:,}", "Total Refund ($)": "${:,.2f}"})
                .background_gradient(cmap="Reds", subset=["Count"]),
                use_container_width=True, hide_index=True)

        if not kpis["category_perf"].empty:
            st.markdown("<hr class='dash-divider'/>", unsafe_allow_html=True)
            st.markdown("**Return Status Summary**")
            display2 = kpis["category_perf"].rename(columns={
                "category": "Status", "revenue": "Total Refund ($)",
                "profit": "Count", "orders": "Distinct Orders"})
            st.dataframe(display2.style
                .format({"Total Refund ($)": "${:,.0f}", "Count": "{:,}", "Distinct Orders": "{:,}"})
                .background_gradient(cmap="Greens", subset=["Total Refund ($)"]),
                use_container_width=True, hide_index=True)

        st.markdown("<hr class='dash-divider'/>", unsafe_allow_html=True)
        st.markdown("**Top Return Customers**")
        if not kpis["top_returners"].empty:
            fmt = {}
            if "total_refund_amount" in kpis["top_returners"].columns:
                fmt["total_refund_amount"] = "${:,.2f}"
            if "avg_refund_amount" in kpis["top_returners"].columns:
                fmt["avg_refund_amount"] = "${:,.2f}"
            st.dataframe(kpis["top_returners"].style.format(fmt)
                .background_gradient(cmap="Oranges", subset=["return_count"]),
                use_container_width=True, hide_index=True)

    # ═══════════════ TAB 3 — VENDORS ═══════════════
    with tab_vendors:
        st.plotly_chart(chart_vendor(kpis["vendor_perf"]), use_container_width=True)
        st.markdown("<hr class='dash-divider'/>", unsafe_allow_html=True)
        st.markdown("**Vendor Detail**")
        if not kpis["vendor_perf"].empty:
            fmt = {}
            if "vendor_gross_sales" in kpis["vendor_perf"].columns:
                fmt["vendor_gross_sales"] = "${:,.0f}"
            if "refund_pct" in kpis["vendor_perf"].columns:
                fmt["refund_pct"] = "{:.1f}%"
            st.dataframe(kpis["vendor_perf"].style.format(fmt)
                .background_gradient(cmap="Blues", subset=["vendor_gross_sales"]),
                use_container_width=True, hide_index=True)

    # ═══════════════ TAB 4 — ASK GENIE ═══════════════
    with tab_genie:
        st.markdown(
            "<h3 style='color:#F8FAFC;'>✨ Ask Genie</h3>"
            "<p style='color:#64748B;'>Ask questions in plain English about your sales, "
            "returns, vendors, customers, and regions.</p>",
            unsafe_allow_html=True)

        st.markdown("<p style='color:#64748B;font-size:0.8rem;'>💡 Suggested questions</p>",
                    unsafe_allow_html=True)
        suggestions = [
            "What is the total revenue by region?",
            "Which vendor has the highest sales?",
            "Show top 5 customers by return count",
            "What is the profit margin by segment?",
            "How many pending returns are there?",
            "Compare monthly sales vs profit trends",
        ]
        cols = st.columns(3)
        for i, sug in enumerate(suggestions):
            with cols[i % 3]:
                if st.button(sug, key=f"sug_{i}", use_container_width=True):
                    st.session_state["genie_inject"] = sug

        st.markdown("<hr class='dash-divider'/>", unsafe_allow_html=True)

        # ── Action buttons row ──
        act_c1, act_c2 = st.columns(2)
        with act_c1:
            if st.button("🔄 New Conversation", use_container_width=True):
                for k in list(st.session_state.keys()):
                    if k.startswith(("genie_", "ct_", "dl_", "gc_", "fb_", "pin_", "ann_", "fu_")):
                        st.session_state.pop(k)
                st.rerun()
        with act_c2:
            compare_on = st.session_state.get("genie_compare", False)
            if st.button(
                "📊 Compare ON" if compare_on else "📊 Compare Mode",
                use_container_width=True,
                type="primary" if compare_on else "secondary"):
                st.session_state["genie_compare"] = not compare_on
                st.rerun()

        # Migrate old tuple-based state
        if "genie_msgs" in st.session_state and st.session_state.genie_msgs:
            if isinstance(st.session_state.genie_msgs[0], tuple):
                st.session_state.pop("genie_msgs", None)
                st.session_state.pop("genie_dfs", None)
                st.session_state.pop("genie_cid", None)

        if "genie_msgs" not in st.session_state:
            st.session_state.genie_msgs = [
                {"role": "assistant", "parts": [{"type": "text", "content":
                 "Hello! I'm Genie. Ask me anything about your data \u2014 "
                 "I'll generate charts and insights automatically. 📊"}]}
            ]
        if "genie_cid" not in st.session_state:
            st.session_state.genie_cid = None

        for idx, msg in enumerate(st.session_state.genie_msgs):
            with st.chat_message(msg["role"]):
                render_rich_response(msg["parts"], idx)

        if "genie_inject" in st.session_state:
            prompt = st.session_state.pop("genie_inject")
            st.session_state.genie_msgs.append(
                {"role": "user", "parts": [{"type": "text", "content": prompt}]})
            with st.chat_message("user"):
                st.markdown(prompt)
            with st.spinner("✨ Genie is analyzing your data…"):
                t0 = time.time()
                parts, cid = genie_ask(prompt, st.session_state.genie_cid)
                elapsed = time.time() - t0
                st.session_state.genie_cid = cid
                st.session_state[f"genie_time_{len(st.session_state.genie_msgs)}"] = elapsed
            st.session_state.genie_msgs.append({"role": "assistant", "parts": parts})
            st.rerun()

        # ── Compare mode: side-by-side ──
        if st.session_state.get("genie_compare"):
            st.markdown("<hr class='dash-divider'/>", unsafe_allow_html=True)
            st.markdown("<p style='color:#3B82F6;font-weight:600;'>📊 Compare Mode — Ask two questions side by side</p>",
                        unsafe_allow_html=True)
            cmp_c1, cmp_c2 = st.columns(2)
            with cmp_c1:
                q1 = st.text_input("Question 1", key="cmp_q1", placeholder="e.g. Revenue by region")
                if st.button("Run Q1", key="cmp_run1") and q1:
                    with st.spinner("Analyzing Q1..."):
                        parts1, _ = genie_ask(q1, st.session_state.genie_cid)
                    st.session_state["cmp_r1"] = parts1
                    st.rerun()
                if "cmp_r1" in st.session_state:
                    render_rich_response(st.session_state["cmp_r1"], 9000)
            with cmp_c2:
                q2 = st.text_input("Question 2", key="cmp_q2", placeholder="e.g. Revenue by segment")
                if st.button("Run Q2", key="cmp_run2") and q2:
                    with st.spinner("Analyzing Q2..."):
                        parts2, _ = genie_ask(q2, st.session_state.genie_cid)
                    st.session_state["cmp_r2"] = parts2
                    st.rerun()
                if "cmp_r2" in st.session_state:
                    render_rich_response(st.session_state["cmp_r2"], 9001)

        if prompt := st.chat_input("Ask Genie…"):
            st.session_state.genie_msgs.append(
                {"role": "user", "parts": [{"type": "text", "content": prompt}]})
            with st.chat_message("user"):
                st.markdown(prompt)
            with st.spinner("✨ Genie is analyzing your data…"):
                t0 = time.time()
                parts, cid = genie_ask(prompt, st.session_state.genie_cid)
                elapsed = time.time() - t0
                st.session_state.genie_cid = cid
                st.session_state[f"genie_time_{len(st.session_state.genie_msgs)}"] = elapsed
            st.session_state.genie_msgs.append({"role": "assistant", "parts": parts})
            st.rerun()

    # ═══════════════ TAB 5 — FORECASTING ═══════════════
    with tab_forecast:
        st.markdown(
            "<h3 style='color:#F8FAFC;'>🔮 Forecasting</h3>"
            "<p style='color:#64748B;'>Ask Genie to forecast sales, revenue, returns, "
            "and trends. Get projections with charts and scenario analysis.</p>",
            unsafe_allow_html=True)

        st.markdown("<p style='color:#64748B;font-size:0.8rem;'>💡 Forecast suggestions</p>",
                    unsafe_allow_html=True)
        fc_suggestions = [
            "Forecast monthly sales for the next 6 months",
            "Predict revenue trend for West region next year",
            "Forecast return volume for next quarter",
            "Project profit margins for each segment next year",
            "Forecast top vendor sales for the next 6 months",
            "Predict customer growth trend for next year",
        ]
        fc_cols = st.columns(3)
        for i, sug in enumerate(fc_suggestions):
            with fc_cols[i % 3]:
                if st.button(sug, key=f"fc_sug_{i}", use_container_width=True):
                    st.session_state["fc_inject"] = sug

        st.markdown("<hr class='dash-divider'/>", unsafe_allow_html=True)

        if st.button("🔄 New Forecast", use_container_width=True):
            for k in list(st.session_state.keys()):
                if k.startswith(("fc_", "fct_", "fdl_", "fgc_", "ffb_")):
                    st.session_state.pop(k)
            st.rerun()

        # ── Init forecast state ──
        if "fc_msgs" not in st.session_state:
            st.session_state.fc_msgs = [
                {"role": "assistant", "parts": [{"type": "text", "content":
                 "Hello! I'm your Forecasting assistant. Ask me to predict future "
                 "sales, revenue, returns, or any trend \u2014 I'll generate forecast "
                 "charts and scenario projections. \U0001f52e"}]}
            ]
        if "fc_cid" not in st.session_state:
            st.session_state.fc_cid = None

        # ── Render forecast chat history ──
        for idx, msg in enumerate(st.session_state.fc_msgs):
            with st.chat_message(msg["role"]):
                render_rich_response(msg["parts"], 5000 + idx)

        # ── Handle injected suggestion ──
        if "fc_inject" in st.session_state:
            prompt = st.session_state.pop("fc_inject")
            st.session_state.fc_msgs.append(
                {"role": "user", "parts": [{"type": "text", "content": prompt}]})
            with st.chat_message("user"):
                st.markdown(prompt)
            if not _is_forecast_question(prompt):
                reply = _NON_FORECAST_MSG
                st.session_state.fc_msgs.append(
                    {"role": "assistant", "parts": [{"type": "text", "content": reply}]})
                st.rerun()
            with st.spinner("\U0001f52e Generating forecast..."):
                t0 = time.time()
                parts, cid = genie_ask(prompt, st.session_state.fc_cid)
                elapsed = time.time() - t0
                st.session_state.fc_cid = cid
                st.session_state[f"genie_time_{5000 + len(st.session_state.fc_msgs)}"] = elapsed
            st.session_state.fc_msgs.append({"role": "assistant", "parts": parts})
            st.rerun()

        # ── Forecast chat input ──
        if prompt := st.chat_input("Ask a forecast question...", key="fc_chat_input"):
            st.session_state.fc_msgs.append(
                {"role": "user", "parts": [{"type": "text", "content": prompt}]})
            with st.chat_message("user"):
                st.markdown(prompt)
            if not _is_forecast_question(prompt):
                reply = _NON_FORECAST_MSG
                st.session_state.fc_msgs.append(
                    {"role": "assistant", "parts": [{"type": "text", "content": reply}]})
                st.rerun()
            with st.spinner("\U0001f52e Generating forecast..."):
                t0 = time.time()
                parts, cid = genie_ask(prompt, st.session_state.fc_cid)
                elapsed = time.time() - t0
                st.session_state.fc_cid = cid
                st.session_state[f"genie_time_{5000 + len(st.session_state.fc_msgs)}"] = elapsed
            st.session_state.fc_msgs.append({"role": "assistant", "parts": parts})
            st.rerun()



if __name__ == "__main__":
    main()
