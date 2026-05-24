"""Command line entrypoint for the CDR Silver Pipeline."""

try:
    from cli import main
except ModuleNotFoundError:
    from .cli import main


if __name__ == "__main__":
    main()
