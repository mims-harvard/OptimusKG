"""CCDF degree distribution with fitted theoretical models.

Plots the complementary cumulative distribution function (CCDF) of node
degrees on log-log axes, overlaid with power-law, lognormal, and exponential
fits.
"""

from __future__ import annotations

import warnings
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import polars as pl
import powerlaw
from matplotlib import ticker

from cli.commands.metrics.utils import build_degree_map, load_parquet_dir

from . import style  # noqa: F401
from .style import BLUE_SCALE, apply_axis_styling, apply_legend_styling


def compute_data(nodes_dir: Path, edges_dir: Path) -> pl.DataFrame:
    """Compute the degree of every node in the knowledge graph.

    Returns a ``pl.DataFrame`` with a single column:
      - ``degree`` (Int64) – number of edges incident on the node

    The fitting and CCDF computation are performed at render time so that
    the stored data stays simple and reusable.
    """

    edges = load_parquet_dir(edges_dir)
    degree_map = build_degree_map(edges)
    return degree_map.select(pl.col("degree").cast(pl.Int64))


# Line styles using matplotlabs named colors.
_EMPIRICAL = {"color": "mpll:red", "linestyle": "-", "linewidth": 1.5}
_POWERLAW = {"color": BLUE_SCALE["600"], "linestyle": "--", "linewidth": 1.2}
_LOGNORMAL = {"color": "mpll:green", "linestyle": ":", "linewidth": 1.2}
_EXPONENTIAL = {"color": "mpll:purple", "linestyle": "-.", "linewidth": 1.2}


def _empirical_ccdf(data: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Compute the empirical CCDF over all unique values in *data*.

    Returns ``(unique_values, ccdf)`` where ``ccdf[i] = P(X >= unique_values[i])``.
    """
    sorted_data = np.sort(data)
    n = len(sorted_data)
    unique_vals = np.unique(sorted_data)
    ccdf = np.array([np.searchsorted(sorted_data, v, side="left") for v in unique_vals])
    ccdf = (n - ccdf) / n
    return unique_vals, ccdf


def _render_ccdf(
    degrees: np.ndarray,
    out_path: Path,
    *,
    xmin: int | None = None,
) -> None:
    """Render a single CCDF plot and save as PDF.

    Parameters
    ----------
    degrees:
        1-D array of node degrees.
    out_path:
        Destination PDF path.
    xmin:
        Minimum degree threshold passed to ``powerlaw.Fit``.  When *None*
        the library auto-selects the optimal ``xmin`` (tail fit).  Set to
        ``1`` to fit the distributions over the full degree range.
    """

    # Fit theoretical distributions.
    fit_kwargs: dict[str, object] = {"discrete": True, "verbose": False}
    if xmin is not None:
        fit_kwargs["xmin"] = xmin

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        fit = powerlaw.Fit(degrees, **fit_kwargs)

    fitted_xmin = fit.xmin

    # Empirical CCDF (full range).
    emp_x, emp_y = _empirical_ccdf(degrees)

    # Theoretical CCDFs evaluated on a log-spaced grid from fitted xmin to max.
    # powerlaw's .ccdf() for discrete distributions returns n-1 values
    # corresponding to x[1:], so we generate one extra point.
    x_grid = np.logspace(
        np.log10(fitted_xmin),
        np.log10(degrees.max()),
        201,
    )

    # Scale factor: the theoretical CCDFs model P(X >= x | X >= xmin),
    # so we multiply by P(X >= xmin) to align with the empirical CCDF.
    p_above_xmin = np.sum(degrees >= fitted_xmin) / len(degrees)

    pl_ccdf = fit.power_law.ccdf(x_grid) * p_above_xmin
    ln_ccdf = fit.lognormal.ccdf(x_grid) * p_above_xmin
    exp_ccdf = fit.exponential.ccdf(x_grid) * p_above_xmin

    # Align x-values with the returned CCDF arrays.
    x_theory = x_grid[1:] if len(pl_ccdf) == len(x_grid) - 1 else x_grid

    # -- plot ----------------------------------------------------------------
    fig, ax = plt.subplots(figsize=(4, 3.5))

    ax.plot(emp_x, emp_y, label="Empirical distribution", **_EMPIRICAL)
    ax.plot(x_theory, pl_ccdf, label="Power law", **_POWERLAW)
    ax.plot(x_theory, ln_ccdf, label="Lognormal", **_LOGNORMAL)
    ax.plot(x_theory, exp_ccdf, label="Exponential", **_EXPONENTIAL)

    ax.set_xscale("log")
    ax.set_yscale("log")

    ax.set_xlabel("Degree", fontsize=7, fontweight="bold")
    ax.set_ylabel("CCDF", fontsize=7, fontweight="bold")
    ax.tick_params(axis="both", labelsize=6)

    apply_axis_styling(ax)

    ax.xaxis.set_minor_formatter(ticker.NullFormatter())
    ax.tick_params(which="minor", left=False, bottom=False)

    legend = ax.legend(
        fontsize=6,
        loc="lower left",
        frameon=True,
    )
    apply_legend_styling(legend)

    plt.savefig(out_path)
    plt.close(fig)


def render_plot(data: pl.DataFrame, out_path: Path) -> None:
    """Render tail-fit and full-range CCDF plots."""

    degrees = data["degree"].to_numpy().astype(float)

    stem = out_path.stem
    suffix = out_path.suffix
    parent = out_path.parent

    _render_ccdf(degrees, parent / f"{stem}_tail{suffix}")
    _render_ccdf(degrees, parent / f"{stem}_full{suffix}", xmin=1)
