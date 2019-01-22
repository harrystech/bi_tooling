import argparse
import sys
import core.verify


def parse_args(args):

    p = argparse.ArgumentParser(
        prog="looker-checker: A tool for validating your lookml project",
        formatter_class=argparse.RawTextHelpFormatter,
        description="Additional tools for administering and validating your LookML",
        epilog="Select one of these sub-commands and you can find more help from there.",
    )

    subs = p.add_subparsers(title="Available sub-commands", dest="command")

    compare_sub = subs.add_parser(
        "verify",
        help="Ensure that every field in your loomkl can be selected without error.",
    )

    parsed = p.parse_args()
    return parsed


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    parsed = parse_args(args)

    if parsed.command == "verify":
        task = core.verify.Verify()
        task.run()

if __name__ == "__main__":
    main()
