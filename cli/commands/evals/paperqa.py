"""Evaluate sampled edges using PaperQA3 via the Edison client.

Provides two modes of operation to support massive scale:
1. "submit": Constructs prompts and submits all jobs to the platform, saving task_ids.
2. "poll": Checks the status of existing task_ids, downloads results, and logs to W&B.
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import re
from pathlib import Path
from datetime import datetime

import polars as pl
import wandb
from dotenv import load_dotenv
from edison_client import EdisonClient, JobNames
from httpx import HTTPStatusError
from tqdm import tqdm

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(_PROJECT_ROOT / ".env")

logger = logging.getLogger("cli")

_RETRYABLE_HTTP_STATUS = frozenset({429, 500, 502, 503, 504})

NODE_TYPE_MAP: dict[str, str] = {
    "ANA": "anatomical structure", "BPO": "biological process", "CCO": "cellular component",
    "DIS": "disease", "DRG": "drug", "EXP": "environmental exposure", "GEN": "gene or protein",
    "MFN": "molecular function", "PHE": "phenotype", "PWY": "pathway",
}

PROMPT_TEMPLATE = """Is there any scientific or medical evidence to support an association between the {seed_type} {seed_name} and the {target_type} {target_name}? Please rate the strength of the evidence on a 5-point scale, where:
    1 = No evidence (zero papers mentioning both {seed_name} and {target_name})
    2 = Weak evidence (1-2 papers mentioning both {seed_name} and {target_name} and no experimental evidence)
    3 = Moderate evidence (3-4 papers mentioning both {seed_name} and {target_name} or experimental evidence)
    4 = Strong evidence (5-6 papers mentioning both {seed_name} and {target_name} or several experimental studies)
    5 = Very strong evidence (more than 6 papers mentioning both {seed_name} and {target_name} or substantial experimental evidence)
    In your response, please also explain the reasoning behind your rating and reference any relevant scientific or medical sources (e.g., peer-reviewed studies, clinical guidelines, experimental data) that support your assessment. For each part of your response, indicate which sources most support it via citation keys at the end of sentences, like (Example2012Example pages 3-4). Only use valid citation keys.

    Instructions to the LLM: Respond with the following XML format exactly.
    <response>
    <reasoning>...</reasoning>
    <rating>...</rating>
    </response>

    `rating` is one of the following (must match exactly): 1, 2, 3, 4, or 5. Do not include any additional keys or text."""

def _build_prompt(row: dict) -> str:
    seed_type = NODE_TYPE_MAP.get(str(row.get("seed_node_type", "")), str(row.get("seed_node_type", "")))
    target_type = NODE_TYPE_MAP.get(str(row.get("target_node_type", "")), str(row.get("target_node_type", "")))
    return PROMPT_TEMPLATE.format(
        seed_type=seed_type, seed_name=row.get("seed_node_name", ""),
        target_type=target_type, target_name=row.get("target_node_name", ""),
    )


def _parse_response(answer_text: str | None) -> tuple[str | None, str | None]:
    if not answer_text or not answer_text.strip():
        return None, None
    reasoning = re.search(r"<reasoning>\s*(.*?)\s*</reasoning>", answer_text, re.DOTALL)
    rating = re.search(r"<rating>\s*(.*?)\s*</rating>", answer_text, re.DOTALL)
    return (
        reasoning.group(1).strip() if reasoning else None,
        rating.group(1).strip() if rating else None,
    )


async def _call_with_backoff(fn, *, max_attempts: int = 15, base_wait: float = 2.0, max_wait: float = 120.0):
    """Call a sync Edison function in a thread, retrying with exponential backoff on rate limits."""
    for attempt in range(max_attempts):
        try:
            return await asyncio.to_thread(fn)
        except HTTPStatusError as e:
            if e.response.status_code not in _RETRYABLE_HTTP_STATUS or attempt == max_attempts - 1:
                raise
            retry_after = e.response.headers.get("Retry-After")
            wait = float(retry_after) if retry_after else min(max_wait, base_wait * (2 ** attempt))
            wait += random.uniform(0, wait * 0.1)
            logger.warning("HTTP %s, retrying in %.1fs (attempt %d/%d)", e.response.status_code, wait, attempt + 1, max_attempts)
            await asyncio.sleep(wait)


async def execute_phase(
    df: pl.DataFrame,
    api_key: str,
    phase: str,
    out_path: Path,
    *,
    api_min_interval_sec: float = 1.0,
    max_rate_limit_attempts: int = 15,
) -> pl.DataFrame:
    client = EdisonClient(api_key=api_key)
    rows = df.to_dicts()
    results = []

    out_path.parent.mkdir(parents=True, exist_ok=True)

    for i, row in enumerate(tqdm(rows, desc=f"Executing {phase.capitalize()} Phase")):
        if phase == "submit":
            if row.get("task_id"):
                results.append(row)
                continue

            prompt = _build_prompt(row)
            task_id = await _call_with_backoff(
                lambda prompt=prompt: client.create_task({"name": JobNames.LITERATURE, "query": prompt}),
                max_attempts=max_rate_limit_attempts,
            )
            result_row = {**row, "task_id": str(task_id), "status": "pending"}

        else:  # poll
            task_id = row.get("task_id")
            if not task_id or row.get("status") in ["success", "failed"]:
                results.append(row)
                continue

            task_info = await _call_with_backoff(
                lambda task_id=task_id: client.get_task(task_id),
                max_attempts=max_rate_limit_attempts,
            )
            status = getattr(task_info, "status", None)
            result_row = {**row, "status": status}

            if status == "success":
                answer = getattr(task_info, "answer", None) or getattr(task_info, "formatted_answer", None)
                reasoning, rating = _parse_response(answer)
                result_row |= {
                    "answer": answer,
                    "has_successful_answer": getattr(task_info, "has_successful_answer", None),
                    "reasoning": reasoning,
                    "rating": rating,
                }

        results.append(result_row)

        # Write header on first row, then append
        with out_path.open("ab") as f:
            pl.DataFrame([result_row]).write_csv(f, include_header=(i == 0))

        await asyncio.sleep(api_min_interval_sec)

    return pl.DataFrame(results)


def run(
    input_path: Path,
    out_path: Path,
    action: str = "submit",
    limit: int | None = None,
    wandb_project: str | None = None,
    api_min_interval_sec: float = 1.0,
    max_rate_limit_attempts: int = 15,
) -> None:
    api_key = os.environ.get("EDISON_API_KEY")
    if not api_key:
        raise RuntimeError("EDISON_API_KEY is not set.")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    timestamped_out_path = out_path.with_name(f"{timestamp}_{out_path.name}")

    logger.info(f"Loading data from {input_path}")
    edges_df = pl.read_csv(input_path)
    if limit:
        edges_df = edges_df.head(limit)

    logger.info(f"Writing results incrementally to {timestamped_out_path}")
    results_df = asyncio.run(execute_phase(
        edges_df, api_key, action, timestamped_out_path,
        api_min_interval_sec=api_min_interval_sec,
        max_rate_limit_attempts=max_rate_limit_attempts,
    ))
    logger.info(f"Done. {len(results_df)} rows saved to {timestamped_out_path}")

    if wandb_project and action == "poll":
        finished = results_df.filter(pl.col("status") == "success")
        if finished.height > 0:
            wandb.init(project=wandb_project, job_type="paperqa_evaluation")
            wandb.log({"evaluation_results_table": wandb.Table(dataframe=finished.to_pandas())})

            if "rating" in finished.columns:
                agg_df = (
                    finished.filter(pl.col("rating").is_not_null())
                    .group_by(["is_true_edge", "rating"])
                    .agg(pl.len().alias("count"))
                    .to_pandas()
                )
                agg_df["group_label"] = agg_df["is_true_edge"].astype(str) + " Edge | Rating " + agg_df["rating"].astype(str)
                bar_table = wandb.Table(dataframe=agg_df)
                wandb.log({"rating_distribution": wandb.plot.bar(bar_table, label="group_label", value="count", title="Count of Ratings Grouped by is_true_edge")})

            wandb.finish()
        else:
            logger.info("No tasks have reached 'success' state yet. Skipping W&B logging.")
