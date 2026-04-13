"""
ExileSage CLI — PoE2 AI advisor command-line interface.
Entry point: exilesage = "exilesage.cli.app:app" (in pyproject.toml)

Commands:
  exilesage ask "question"     Answer a PoE2 question
  exilesage ingest             Reload JSON → SQLite
  exilesage update             Fetch fresh data, then ingest
"""

import os
import sys
import logging
from pathlib import Path
from typing import Optional

# Force UTF-8 output on Windows (GBK can't render Rich markdown bullets)
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import typer
from rich.console import Console
from rich.panel import Panel
from rich.markdown import Markdown
from rich.spinner import Spinner
import time

from exilesage.advisor.core import ask, classify_query
from exilesage.config import QueryType
from pipeline.ingest import run as ingest_run
from pipeline.update import run as update_run

# ── Setup ──────────────────────────────────────────────────────────────────────

console = Console(legacy_windows=False)
app = typer.Typer(
    name="exilesage",
    help="PoE2 AI advisor — ask questions about Path of Exile 2 crafting, builds, and mechanics.",
)

# Suppress Anthropic and other verbose logging
logging.getLogger("anthropic").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


# ── Command: ask ───────────────────────────────────────────────────────────────

@app.command(name="ask")
def ask_cmd(
    question: str = typer.Argument(..., help="Your question about PoE2"),
    query_type: Optional[str] = typer.Option(
        None,
        "--type",
        "-t",
        help="Force query type: factual, crafting, analysis, guide, innovation",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Show tool calls and classification in output",
    ),
) -> None:
    """
    Ask ExileSage a question about Path of Exile 2.

    Examples:
      exilesage ask "What does Life Leech do?"
      exilesage ask "How do I craft a crit dagger?" --type crafting
      exilesage ask "Best skills for a Ranger build?" --verbose
    """
    try:
        # Resolve query type
        resolved_type = None
        if query_type:
            query_type_lower = query_type.lower()
            try:
                resolved_type = QueryType(query_type_lower)
            except ValueError:
                console.print(
                    f"[red]Error: Invalid query type '{query_type}'[/red]\n"
                    f"Valid types: factual, crafting, analysis, guide, innovation"
                )
                raise typer.Exit(code=1)
        else:
            if verbose:
                console.print("[dim]Classifying query...[/dim]")
            resolved_type = classify_query(question)
            if verbose:
                console.print(f"[dim]→ Classified as: {resolved_type.value}[/dim]")

        # Show question panel
        from exilesage.config import MODEL_MAP
        model_name = MODEL_MAP[resolved_type].split("/")[-1]  # extract short name

        header = Panel(
            f"Q: {question}\n"
            f"Type: {resolved_type.value} | Model: {model_name}",
            expand=False,
            title="ExileSage",
            style="blue",
        )
        console.print(header)

        # Get answer
        if verbose:
            console.print("[dim]Querying advisor...[/dim]")

        answer = ask(question, query_type=resolved_type)

        # Render answer as markdown (fall back to plain text on Windows encoding errors)
        console.print()
        try:
            md = Markdown(answer)
            console.print(md)
        except Exception:
            console.print(answer)
        console.print()

        # Footer
        console.print("[dim]─── end of response ───[/dim]")

    except typer.Exit:
        raise
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted by user[/yellow]")
        raise typer.Exit(code=130)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        if verbose:
            import traceback
            traceback.print_exc()
        raise typer.Exit(code=1)


# ── Command: ingest ────────────────────────────────────────────────────────────

@app.command()
def ingest() -> None:
    """
    Reload JSON data → SQLite database.

    Reads processed JSON files from data/processed/ and populates exilesage.db
    with mods, base items, currencies, and augments.
    """
    try:
        console.print("[blue]Ingesting data into database...[/blue]")

        # Use spinner while ingesting
        with console.status("[bold blue]Processing...", spinner="dots"):
            ingest_run()

        console.print("[green]✓ Ingest complete[/green]")

    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted by user[/yellow]")
        raise typer.Exit(code=130)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(code=1)


# ── Command: update ────────────────────────────────────────────────────────────

@app.command()
def update(
    all_files: bool = typer.Option(
        False,
        "--all",
        help="Fetch all repoe files (not just crafting subset)",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Re-download even if cached",
    ),
    show_diff: bool = typer.Option(
        False,
        "--diff",
        help="Compute and save a diff vs previous data",
    ),
) -> None:
    """
    Fetch fresh PoE2 data from RePoE, then ingest into database.

    By default, only crafting-related files are fetched. Use --all to fetch
    every RePoE file. Use --force to re-download even if cached. Use --diff
    to see what changed vs the previous import.
    """
    try:
        console.print("[blue]Updating PoE2 data...[/blue]")

        # Update phase
        with console.status("[bold blue]Fetching and processing data...", spinner="dots"):
            update_run(fetch_all_files=all_files, force=force, show_diff=show_diff)

        # Ingest phase
        console.print("[blue]Ingesting into database...[/blue]")
        with console.status("[bold blue]Processing...", spinner="dots"):
            ingest_run()

        console.print("[green]✓ Update and ingest complete[/green]")

    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted by user[/yellow]")
        raise typer.Exit(code=130)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(code=1)


# ── Entrypoint ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app()
