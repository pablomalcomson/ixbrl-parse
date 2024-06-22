import csv
import json
import logging
import sys
from datetime import date
from typing import Any, Dict

# Standard Library Imports: Modules for CSV handling, 
# JSON handling, logging, system interactions, date handling, and type hinting.

import click

# Click Import: Import the Click library to create the CLI.

from ixbrlparse.__about__ import __version__
from ixbrlparse.core import IXBRL

# Project-Specific Imports: Import version information and the IXBRL class 
# from the ixbrlparse package.

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(name)s:%(message)s")

# Configures the logging system to display log messages with 
# a specific format (timestamp, log level, logger name, and log message)

@click.group(
    context_settings={"help_option_names": ["-h", "--help"]},
    invoke_without_command=True,
)

# `@click.group`: 
# Defines a group of related commands (command group) named `ixbrlparse_cli`
# `context_settings={"help_option_names": ["-h", "--help"]}`: 
# Allows using `-h` and `--help` to display help
# `invoke_without_command=True`: 
# Allows the group command to run even if no subcommand is provided.

@click.version_option(version=__version__, prog_name="IXBRLParse")

# Adds a --version option to the CLI to display the program version and name.

@click.option(
    "--format",
    "-f",
    "output_format",
    default="csv",
    help="Output format",
    type=click.Choice(["csv", "json", "jsonlines", "jsonl"]),
)

# `--format`, `-f`: Option for specifying the output format.
# `"output_format"`: The variable name used in the ixbrlparse_cli() function.
#  `default="csv"`: Default value is csv.
# `help="Output format"`: Help text describing the option.
# `type=click.Choice(["csv", "json", "jsonlines", "jsonl"])`: 
# Restricts the choices to the specified formats.

@click.option(
    "--fields", default="all", type=click.Choice(["numeric", "nonnumeric", "all"]), help="Which fields to output"
)

# `--fields``: Option for specifying which fields to output.
# `default="all"`: Default value is all.
# `help="Which fields to output"`: Help text describing the option.
# `type=click.Choice(["numeric", "nonnumeric", "all"])`: 
# Restricts the choices to the specified field types.

@click.option("--outfile", default=sys.stdout, help="Where to output the file", type=click.File("w", encoding="UTF-8"))

# `--outfile`: Option for specifying the output file / where to output the file.
# `default=sys.stdout`: Default value is standard output.
# `help="Where to output the file"`: Help text describing the option.
# `type=click.File("w", encoding="UTF-8")`: 
# Specifies the file should be opened in write mode with UTF-8 encoding.

@click.argument("infile", type=click.File("rb"), default=sys.stdin, nargs=1)

# `infile`: Positional argument (which, unlike options, have no prefix like -- or -) 
# for specifying the input file.
# `type=click.File("rb")`: Opens the file in read-binary mode.
# `default=sys.stdin`: Default value is standard input.
# `nargs=1`: Specifies that exactly one argument is expected.

# The decorators (@click.group, @click.version_option, @click.option, 
# and @click.argument) configure the CLI's behavior, options, and arguments.

def ixbrlparse_cli(output_format: str, fields: str, outfile, infile):
    x = IXBRL(infile)

    print(f"x: {x}", file=sys.stderr)

# `x = IXBRL(infile)`: Creates an instance of the `IXBRL` class with the input file.

    if output_format == "csv":
        # Print debug message to stderr
        # print("Writing CSV", file=sys.stderr)
        
        values = x.to_table(fields)
        
        # Print values to stderr
        # print(values, file=sys.stderr)

# transforms the raw iXBRL data into tabular format (more specifically, a structured list of dictionaries, where each 
# dictionary represents a row in a table with key-value pairs corresponding to column names and their respective data).
# this transformation enables the data to be easily written to (= converted and outputted to) a CSV file. 

        columns: Dict[str, Any] = {}

        # Print columns to stderr
        # print(columns, file=sys.stderr)

# This line initializes an empty dictionary to store column headers:
# a variable `columns` is declared and initialized as an empty dictionary;
# it uses type hints (`Dict[str, Any]`) to indicate that columns will be 
# a dictionary with string keys and values of any type.

        for r in values:
            columns = {**dict.fromkeys(r.keys()), **columns}

        # Print columns to stderr
        print(columns, file=sys.stderr)

# These two lines collect all unique column headers from the `values`.
# The `for` loop iterates over each dictionary `r` in `values`.
# For each row `r` in `values`, the inner dictionary comprehension 
# `dict.fromkeys(r.keys())` creates a new dictionary with the keys from `r` and default values of `None`.
# The `{**dict.fromkeys(r.keys()), **columns}` merges the new dictionary created from the current row's 
# keys with the existing `columns` dictionary. This ensures that all unique column headers are collected in `columns`.

        writer = csv.DictWriter(outfile, columns.keys())

# This line creates a `csv.DictWriter` object for writing the CSV file:
# a `csv.DictWriter` object named `writer` is initialized;
# the `outfile` (the output file object) and the keys from 
# the `columns` dictionary are used as the field names (column headers) for the CSV file.

        writer.writeheader()

# This line writes the header row to the CSV file:
# the `writeheader` method of the `csv.DictWriter` object writes the column headers 
# to the CSV file; the headers are based on the keys provided when the `DictWriter` 
# was created (i.e., the keys from the columns dictionary)

        writer.writerows(values)

# This line writes the data rows to the CSV file:
# the `writerows` method of the `csv.DictWriter` object writes all the rows from 
# `values` to the CSV file; each row is a dictionary, where the keys correspond 
# to the column headers and the values correspond to the data for each column

# Converts data to a table format.  
# Prepares column headers and writes the data as CSV.

    elif output_format in ["jsonlines", "jsonl"]:
        values = x.to_table(fields)
        for v in values:
            if isinstance(v["value"], date):
                v["value"] = str(v["value"])
            json.dump(v, outfile)
            outfile.write("\n")

# Converts data to a table format.
# Ensures date values are converted to strings.
# Writes each dictionary as a JSON object followed by a newline.

    elif output_format == "json":
        json.dump(x.to_json(), outfile, indent=4)

# Converts data to JSON format.
# Writes the JSON data with indentation for readability.

# The script defines a CLI for parsing iXBRL files, 
# providing options for output format, fields to output, 
# and input/output files. 
# The CLI supports multiple output formats (CSV, JSON Lines, JSON) 
# and handles the conversion and writing of data based on user-specified options