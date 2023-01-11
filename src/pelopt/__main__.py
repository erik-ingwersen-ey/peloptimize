"""Command-line interface."""
import click


@click.command()
@click.version_option()
def main() -> None:
    """Peloptimize."""


if __name__ == "__main__":
    main(prog_name="peloptimize")  # pragma: no cover
