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
from dotenv import load_dotenv
from edison_client import EdisonClient, JobNames

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
    """Extract <reasoning> and <rating> from XML-style response.

    Returns:
        (reasoning, rating) — either may be None if missing or unparseable.
    """
    if not answer_text or not answer_text.strip():
        return None, None
    reasoning = re.search(r"<reasoning>\s*(.*?)\s*</reasoning>", answer_text, re.DOTALL)
    rating = re.search(r"<rating>\s*(.*?)\s*</rating>", answer_text, re.DOTALL)
    return (
        reasoning.group(1).strip() if reasoning else None,
        rating.group(1).strip() if rating else None,
    )


async def _run_paperqa_async(
    edges_df: pl.DataFrame,
    api_key: str,
) -> list:
    """Submit all edge prompts to the Edison client asynchronously."""
    client = EdisonClient(api_key=api_key)

    tasks_data = []
    for row in edges_df.iter_rows(named=True):
        prompt = _build_prompt(row)
        tasks_data.append(
            {
                "name": JobNames.LITERATURE,
                "query": prompt,
            }
        )

    logger.info("Submitting %d literature tasks to Edison...", len(tasks_data))
    responses = await client.arun_tasks_until_done(tasks_data)
    logger.info("Received %d task responses from Edison.", len(responses))
    return responses


def run(
    sampled_edges_path: Path,
    out_path: Path,
) -> None:
    """Run PaperQA3 (via Edison) on sampled edges and save results."""
    if not sampled_edges_path.exists():
        msg = f"Sampled edges file not found: {sampled_edges_path}"
        raise FileNotFoundError(msg)

    api_key = os.environ.get("EDISON_API_KEY")
    if not api_key:
        msg = (
            "EDISON_API_KEY is not set. Set it in the project root .env file "
            "or export it in your environment before running this command."
        )
        raise RuntimeError(msg)

    logger.info("Loading sampled edges from %s", sampled_edges_path)
    edges_df = pl.read_csv(sampled_edges_path)
    logger.info("Loaded %d edges for evaluation.", edges_df.height)

    # Run Edison / PaperQA3 asynchronously
    responses = asyncio.run(_run_paperqa_async(edges_df, api_key=api_key))

    # Extract answer text and parse <reasoning> / <rating> from response
    answers: list[str | None] = []
    reasonings: list[str | None] = []
    ratings: list[str | None] = []
    for resp in responses:
        answer_text: str | None = None
        # PQATaskResponse-style object
        if hasattr(resp, "answer"):
            answer_text = getattr(resp, "answer")
        # Dict-style response from API
        elif isinstance(resp, dict):
            answer_text = resp.get("answer") or resp.get("formatted_answer")

        answers.append(answer_text)
        reasoning, rating = _parse_response(answer_text)
        reasonings.append(reasoning)
        ratings.append(rating)

    if len(answers) != edges_df.height:
        logger.warning(
            "Number of answers (%d) does not match number of edges (%d). "
            "Truncating to the minimum.",
            len(answers),
            edges_df.height,
        )
        min_len = min(len(answers), edges_df.height)
        edges_df = edges_df.head(min_len)
        answers = answers[:min_len]
        reasonings = reasonings[:min_len]
        ratings = ratings[:min_len]

    results_df = edges_df.with_columns(
        pl.Series("paperqa_answer", answers),
        pl.Series("paperqa_reasoning", reasonings),
        pl.Series("paperqa_rating", ratings),
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    results_df.write_csv(out_path)
    logger.info("Saved PaperQA3 edge evaluations to %s", out_path)

