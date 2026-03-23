"""Plot styling configuration and helper functions.

Contains unified aesthetics for all figure plots including:
- Style constants (grid, spines, bars, legends, etc.)
- Helper functions for consistent styling
"""

import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import matplotlabs as mpll  # noqa: F401  — registers styles, colormaps, named colors

from matplotlib.axes import Axes
from matplotlib.legend import Legend

plt.style.use("mpll")

BLUE_SCALE = {
    "50": "#EFF6FF",
    "100": "#DBEAFE",
    "300": "#93C5FD",
    "400": "#60A5FA",
    "500": "#3B82F6",
    "600": "#2563EB",
    "700": "#1D4ED8",
    "800": "#1E40AF",
    "900": "#1E3A8A",
}

BLUE_CMAP = mcolors.LinearSegmentedColormap.from_list(
    "optimuskg-blue",
    [
        BLUE_SCALE["50"],
        BLUE_SCALE["100"],
        BLUE_SCALE["300"],
        BLUE_SCALE["400"],
        BLUE_SCALE["500"],
        BLUE_SCALE["600"],
        BLUE_SCALE["700"],
        BLUE_SCALE["800"],
        BLUE_SCALE["900"],
    ],
)

WARM_GRAY_SCALE = {
    "50": "#FAFAF9",
    "100": "#F5F5F4",
    "200": "#E7E5E4",
    "300": "#D6D3D1",
    "400": "#A8A29E",
    "500": "#78716C",
    "600": "#57534E",
    "700": "#44403C",
    "800": "#292524",
    "900": "#1C1917",
}

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
    "bar_edgecolor": WARM_GRAY_SCALE["900"],
    "bar_linewidth": 0.5,
    "bar_alpha": 0.9,
    "bar_width": 0.75,
    # Text labels
    "value_label_fontsize": 6,
    "title_fontsize": 8,
    "title_fontweight": "bold",
    "title_pad": 10,
    "axis_label_fontsize": 7,
    "axis_label_fontweight": "medium",
    "tick_label_fontsize": 7,
    # Legend
    "legend_fontsize": 6,
    "legend_frameon": True,
    "legend_fancybox": False,
    "legend_shadow": False,
    "legend_framealpha": 1.0,
    "legend_edgecolor": WARM_GRAY_SCALE["900"],
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
