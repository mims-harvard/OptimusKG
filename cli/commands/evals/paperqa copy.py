"""Evaluate sampled edges using PaperQA3 via the Edison client.

This command reads the ``sampled_edges.csv`` file produced by the
``cli evals sample-edges`` command, constructs literature-search
prompts for each edge, and submits them to the Edison Platform
(`JobNames.LITERATURE`, built on PaperQA3).

The Edison API key must be provided via the ``EDISON_API_KEY``
environment variable (e.g. in a ``.env`` file in the project root).
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from pathlib import Path

import polars as pl
import wandb
from dotenv import load_dotenv
from edison_client import EdisonClient, JobNames
from tqdm import tqdm

# Load .env from project root so EDISON_API_KEY is available
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(_PROJECT_ROOT / ".env")

logger = logging.getLogger("cli")

# Mapping from compact node codes to human-readable types
NODE_TYPE_MAP: dict[str, str] = {
    "ANA": "anatomy",
    "BPO": "biological process",
    "CCO": "cellular component",
    "DIS": "disease",
    "DRG": "drug",
    "EXP": "exposure",
    "GEN": "gene or protein",
    "MFN": "molecular function",
    "PHE": "phenotype",
    "PWY": "pathway",
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
    seed_type_code = str(row.get("seed_node_type") or "")
    target_type_code = str(row.get("target_node_type") or "")

    seed_type = NODE_TYPE_MAP.get(seed_type_code, seed_type_code)
    target_type = NODE_TYPE_MAP.get(target_type_code, target_type_code)

    seed_name = row.get("seed_node_name") or ""
    target_name = row.get("target_node_name") or ""

    return PROMPT_TEMPLATE.format(
        seed_type=seed_type,
        seed_name=seed_name,
        target_type=target_type,
        target_name=target_name,
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

async def _process_single_edge(client: EdisonClient, row: dict, semaphore: asyncio.Semaphore) -> tuple[dict, any]:
    """Process a single edge with a concurrency-limiting semaphore."""
    async with semaphore:
        prompt = _build_prompt(row)
        task_data = {
            "name": JobNames.LITERATURE,
            "query": prompt,
        }
        resp = await client.arun_tasks_until_done(task_data)
        return row, resp

async def _run_paperqa_async(edges_df: pl.DataFrame, api_key: str, max_concurrent: int, use_wandb: bool) -> list[dict]:
    """Submit tasks with a semaphore, extract elements from the response, and track real-time progress."""
    client = EdisonClient(api_key=api_key)
    semaphore = asyncio.Semaphore(max_concurrent)

    tasks = [
        _process_single_edge(client, row, semaphore)
        for row in edges_df.iter_rows(named=True)
    ]

    out_rows = []
    
    # Initialize the WandB table if logging is enabled
    wandb_table = wandb.Table(columns=[
        "seed_name", "target_name", "is_true_edge", 
        "status", "task_id", "rating", "reasoning", "raw_answer"
    ]) if use_wandb else None

    # as_completed yields futures as soon as they are done
    for future in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Evaluating via PaperQA3"):
        row, resp = await future
        
        # 1. Safely grab the first item from the resp list
        task_resp = resp[0] if isinstance(resp, list) and len(resp) > 0 else resp

        # 2. Extract specific fields using getattr to handle the PQATaskResponse object
        status = getattr(task_resp, "status", None)
        query = getattr(task_resp, "query", None)
        created_at = getattr(task_resp, "created_at", None)
        task_id = getattr(task_resp, "task_id", None)
        answer = getattr(task_resp, "answer", None)
        formatted_answer = getattr(task_resp, "formatted_answer", None)
        has_successful_answer = getattr(task_resp, "has_successful_answer", None)

        # Parse reasoning & rating
        reasoning, rating = _parse_response(answer or formatted_answer)

        # 3. Compile everything into the new row representation (Prefixes Dropped)
        new_row = dict(row)
        new_row.update({
            "status": status,
            "query": query,
            "created_at": str(created_at) if created_at else None,  # Cast datetime to string
            "task_id": str(task_id) if task_id else None,           # Cast UUID to string
            "answer": answer,
            "formatted_answer": formatted_answer,
            "has_successful_answer": has_successful_answer,
            "reasoning": reasoning,
            "rating": rating,
        })
        out_rows.append(new_row)

        # 4. Real-time W&B Logging
        if use_wandb:
            # Log real-time metrics so you can watch progress live on W&B charts
            wandb.log({
                "rating": int(rating) if rating and rating.isdigit() else 0,
                "is_successful_answer": 1 if has_successful_answer else 0
            })
            
            # Add to the table artifact that gets pushed at the end
            wandb_table.add_data(
                row.get("seed_node_name"),
                row.get("target_node_name"),
                row.get("is_true_edge"),
                status,
                str(task_id) if task_id else None,
                rating,
                reasoning,
                answer or formatted_answer
            )

    # Push the fully populated table artifact to WandB when everything finishes
    if use_wandb and wandb_table is not None:
        wandb.log({"evaluation_results": wandb_table})

    return out_rows

def run(
    sampled_edges_path: Path,
    out_path: Path,
    limit: int | None = None,
    wandb_project: str | None = None,
    max_concurrent: int = 10,
) -> None:
    if not sampled_edges_path.exists():
        msg = f"Sampled edges file not found: {sampled_edges_path}"
        raise FileNotFoundError(msg)

    api_key = os.environ.get("EDISON_API_KEY")
    if not api_key:
        msg = "EDISON_API_KEY is not set in the environment or .env file."
        raise RuntimeError(msg)

    logger.info("Loading sampled edges from %s", sampled_edges_path)
    edges_df = pl.read_csv(sampled_edges_path)
    
    # Pilot Truncation
    if limit and limit > 0:
        logger.info("Pilot mode activated: Limiting evaluation to %d queries.", limit)
        edges_df = edges_df.head(limit)

    logger.info("Starting evaluation for %d edges (Max Concurrent: %d).", edges_df.height, max_concurrent)

    # Initialize WandB
    use_wandb = bool(wandb_project)
    if use_wandb:
        wandb.init(
            project=wandb_project,
            job_type="paperqa_evaluation",
            config={"limit": limit, "max_concurrent": max_concurrent, "model": "paperqa3"}
        )

    # Run Asynchronously, capturing the fully compiled rows directly
    results_rows = asyncio.run(_run_paperqa_async(
        edges_df, 
        api_key=api_key, 
        max_concurrent=max_concurrent, 
        use_wandb=use_wandb
    ))

    if use_wandb:
        wandb.finish()

    # Save to CSV
    results_df = pl.DataFrame(results_rows)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    results_df.write_csv(out_path)
    logger.info("Saved PaperQA3 edge evaluations to %s", out_path)
