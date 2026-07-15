"""Entry point launched by the MCPB manifest; delegates to the packaged server."""

from optimuskg_mcp.server import run_stdio

if __name__ == "__main__":
    run_stdio()
