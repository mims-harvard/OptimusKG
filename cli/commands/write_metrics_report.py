import json
import logging
from pathlib import Path

logger = logging.getLogger("cli")


def _format_number(number):
    """Formats a number to be more readable."""
    if isinstance(number, int | float):
        return f"{number:,}"
    return number


def _generate_overview_table(data: dict) -> str:
    """Generates the OptimusKG Data Overview table."""
    headers = ["Metric", "Value"]
    rows = [
        ["Total Elements", _format_number(data.get("total_elements"))],
        ["Total Properties", _format_number(data.get("total_properties"))],
        ["Number of Nodes", _format_number(data.get("num_nodes"))],
        ["Number of Edges", _format_number(data.get("num_edges"))],
        ["Processed Lines", _format_number(data.get("processed_lines"))],
        ["Malformed Lines", _format_number(data.get("malformed_lines"))],
    ]
    table = "### OptimusKG Data Overview\n\n"
    table += f"| {' | '.join(headers)} |\n"
    table += f"| {' | '.join(['---'] * len(headers))} |\n"
    for row in rows:
        table += f"| {' | '.join(map(str, row))} |\n"
    return table + "\n"


def _generate_node_metrics_table(data: dict) -> str:
    """Generates the Node Metrics by Label Type table."""
    headers = [
        "Label",
        "Count",
        "Percentage (%)",
        "Avg. Properties",
        "Std. Dev. Properties",
        "Avg. Degree",
        "Std. Dev. Degree",
    ]
    rows = []
    for entry in data.get("node_metrics", {}).get("label_type", []):
        props = entry.get("properties") or {}
        degree = entry.get("degree") or {}
        rows.append(
            [
                entry.get("type"),
                _format_number(entry.get("count")),
                f"{entry.get('percentage'):.2f}",
                f"{props.get('avg'):.2f}" if props.get("avg") is not None else "N/A",
                f"{props.get('std'):.2f}" if props.get("std") is not None else "N/A",
                f"{degree.get('avg'):.2f}" if degree.get("avg") is not None else "N/A",
                f"{degree.get('std'):.2f}" if degree.get("std") is not None else "N/A",
            ]
        )

    table = "### Node Metrics by Label Type\n\n"
    table += f"| {' | '.join(headers)} |\n"
    table += f"| {' | '.join(['---'] * len(headers))} |\n"
    for row in rows:
        table += f"| {' | '.join(map(str, row))} |\n"
    return table + "\n"


def _generate_edge_topology_table(data: dict) -> str:
    """Generates the Edge Topology Metrics table."""
    headers = ["Metric", "Value", "Description"]
    rows = []
    for key, value_dict in (
        data.get("edge_metrics", {}).get("topology_metrics", {}).items()
    ):
        rows.append(
            [
                key.replace("_", " ").title(),
                _format_number(value_dict.get("value")),
                value_dict.get("description"),
            ]
        )

    table = "### Edge Topology Metrics\n\n"
    table += f"| {' | '.join(headers)} |\n"
    table += f"| {' | '.join(['---'] * len(headers))} |\n"
    for row in rows:
        table += f"| {' | '.join(map(str, row))} |\n"
    return table + "\n"


def _generate_edge_metrics_table(data: dict) -> str:
    """Generates the Edge Metrics by Label Type table."""
    headers = [
        "Label",
        "Count",
        "Percentage (%)",
        "Avg. Properties",
        "Std. Dev. Properties",
    ]
    rows = []
    for entry in data.get("edge_metrics", {}).get("label_type", []):
        props = entry.get("properties") or {}
        rows.append(
            [
                entry.get("type"),
                _format_number(entry.get("count")),
                f"{entry.get('percentage'):.2f}",
                f"{props.get('avg'):.2f}" if props.get("avg") is not None else "N/A",
                f"{props.get('std'):.2f}" if props.get("std") is not None else "N/A",
            ]
        )

    table = "### Edge Metrics by Label Type\n\n"
    table += f"| {' | '.join(headers)} |\n"
    table += f"| {' | '.join(['---'] * len(headers))} |\n"
    for row in rows:
        table += f"| {' | '.join(map(str, row))} |\n"
    return table + "\n"


def write_metrics_report(in_path: Path, out_path: Path):
    """
    Reads the metrics.json file and writes a markdown report with summary tables.
    """
    logger.info(
        f"Attempting to generate metrics report from {in_path}, output will be written to {out_path}"
    )

    try:
        with open(in_path, encoding="utf-8") as infile:
            metrics_data = json.load(infile)

        report_parts = ["# OptimusKG Metrics Report\n"]

        report_parts.append(_generate_overview_table(metrics_data))
        report_parts.append(_generate_node_metrics_table(metrics_data))
        report_parts.append(_generate_edge_topology_table(metrics_data))
        report_parts.append(_generate_edge_metrics_table(metrics_data))

        report_content = "\n".join(report_parts)

        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as outfile:
            outfile.write(report_content)
        logger.info(f"Successfully wrote metrics report to {out_path}")

    except FileNotFoundError:
        logger.error(f"Input file not found: {in_path}")
        raise
    except Exception as e:
        logger.error(
            f"An error occurred generating the report from {in_path} or writing to {out_path}: {type(e).__name__} - {e}",
            exc_info=True,
        )
        raise
