"""Plot styling configuration and helper functions.

Contains unified aesthetics for all figure plots including:
- Style constants (grid, spines, bars, legends, etc.)
- Helper functions for consistent styling
"""

import matplotlib.pyplot as plt
import scienceplots  # noqa: F401
from matplotlib.axes import Axes
from matplotlib.legend import Legend

# Apply science plot style
plt.style.use(["science", "ieee", "no-latex"])

plt.rcParams.update(
    {
        "figure.dpi": 300,
        "savefig.dpi": 300,
        "savefig.bbox": "tight",
        "savefig.pad_inches": 0.1,
        "font.size": 10,
        "axes.labelsize": 11,
        "axes.titlesize": 12,
        "xtick.labelsize": 10,
        "ytick.labelsize": 10,
        "legend.fontsize": 9,
        "legend.title_fontsize": 10,
        "figure.titlesize": 13,
    }
)

STYLE = {
    # Grid
    "grid_linestyle": "--",
    "grid_alpha": 0.4,
    "grid_linewidth": 0.5,
    # Spines
    "spine_linewidth": 0.5,
    "hide_top": True,
    "hide_right": True,
    # Bars
    "bar_edgecolor": "black",
    "bar_linewidth": 0.5,
    "bar_alpha": 0.9,
    "bar_width": 0.75,
    # Text labels
    "value_label_fontsize": 8,
    "title_fontsize": 12,
    "title_fontweight": "bold",
    "title_pad": 10,
    "axis_label_fontsize": 11,
    "axis_label_fontweight": "medium",
    "tick_label_fontsize": 10,
    # Legend
    "legend_fontsize": 9,
    "legend_frameon": True,
    "legend_fancybox": False,
    "legend_shadow": False,
    "legend_framealpha": 1.0,
    "legend_edgecolor": "black",
    "legend_frame_linewidth": 0.5,
    "legend_bbox_to_anchor": (0.5, 1.02),
    # Figure
    "fig_dpi": 300,
    "fig_facecolor": "white",
    "fig_tight_rect": [0, 0, 1, 0.96],
}


def apply_axis_styling(ax: Axes) -> None:
    """Apply unified axis styling to any axis."""
    ax.set_axisbelow(True)
    ax.grid(False)
    if STYLE["hide_top"]:
        ax.spines["top"].set_visible(False)
        ax.tick_params(top=False)
    if STYLE["hide_right"]:
        ax.spines["right"].set_visible(False)
        ax.tick_params(right=False)
    ax.spines["left"].set_linewidth(STYLE["spine_linewidth"])
    ax.spines["bottom"].set_linewidth(STYLE["spine_linewidth"])


def apply_legend_styling(legend: Legend) -> None:
    """Apply unified legend styling."""
    legend.get_frame().set_linewidth(STYLE["legend_frame_linewidth"])
