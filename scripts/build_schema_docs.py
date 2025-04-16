# Script to build markdown documentation from the schema/schema.yaml file

import os
import yaml

SCHEMA_PATH = "simple/schema.yaml"
DOCS_DIR = "docs/"
SCHEMA_DIAGRAM = "figures/auto_schema.png"
SCHEMA_SUB_DIR = "schema/"
SCHEMA_TOC_NAME = "README.md"

# Loop over each table in the schema
with open(SCHEMA_PATH, "r") as schema_file:
    schema = yaml.safe_load(schema_file)

    for table in schema["tables"]:
        table_name = table["name"]
        table_path = os.path.join(DOCS_DIR, SCHEMA_SUB_DIR, f"{table_name}.md")

        # Prepare a markdown file per table
        with open(table_path, "w") as out_file:
            out_file.write(f"# {table_name}\n")
            out_file.write(f"{table['description']}\n")
            out_file.write(
                "\n\nColumns marked with an exclamation mark ( :exclamation:) may not be empty.\n"
            )
            out_file.write(
                "| Column Name | Description | Datatype | Length | Units  | UCD |\n"
            )
            out_file.write("| --- | --- | --- | --- | --- | --- |\n")

            # Loop over column names to get the column information
            for column in table["columns"]:
                # Get the unit from the fits or ivoa tags
                units = column.get("fits:tunit", "")
                if units == "":
                    units = column.get("ivoa:unit", "")

                # Identify the required columns
                if column.get("nullable", "True") is False:
                    # If the column is required, bold and add asterisk to the name
                    column_name = f":exclamation:**{column['name']}**"
                else:
                    column_name = column["name"]

                # Write out the column
                out_file.write(
                    f"| {column_name} | {column['description']} | {column['datatype']} | {column.get('length', '')} | {units} | {column.get('ivoa:ucd', '')}  |\n"
                )
            out_file.write("\n")

            # Make the indexes table
            if "indexes" in table:
                out_file.write("## Indexes\n")
                out_file.write("| Name | Columns | Description |\n")
                out_file.write("| --- | --- | --- |\n")
                for index in table["indexes"]:
                    out_file.write(
                        f"| {index['name']} | {index['columns']} | {index.get('description', '')} |\n"
                    )
                out_file.write("\n")

            # Make the constraints table
            foreign_keys_exists = False
            checks_exists = False
            if "constraints" in table:

                # Do Foreign Keys
                foreign_key_table = "## Foreign Keys\n"
                foreign_key_table += "| Description | Columns | Referenced Columns |\n"
                foreign_key_table += "| --- | --- | --- |\n"

                checks_table = "## Checks\n"
                checks_table += "| Description | Expression |\n"
                checks_table += "| --- | --- |\n"

                for constraint in table["constraints"]:
                    if constraint.get("@type") == "ForeignKey":
                        foreign_keys_exists = True
                        foreign_key_table += f"| {constraint['description']} | {constraint.get('columns', '')} | {constraint.get('referencedColumns', '')} |\n"
                    elif constraint.get("@type") == "Check":
                        checks_exists = True
                        checks_table += f"| {constraint['description']} | {constraint.get('expression', '')} |\n"
                    else:
                        print(
                            f"Unknown constraint type {constraint.get('@type')} in table {table_name}"
                        )

                if foreign_keys_exists:
                    out_file.write(foreign_key_table)
                if checks_exists:
                    out_file.write(checks_table)

    # Make a table of contents-type file
    with open(os.path.join(DOCS_DIR, SCHEMA_TOC_NAME), "w") as out_file:
        out_file.write("# Schema Documentation\n")
        out_file.write(
            f"This documentation is generated from the [schema.yaml]({SCHEMA_PATH}) file using [build_schema_docs.py](scripts/build_schema_docs.py).\n"
        )
        out_file.write("\n## Tables\n")
        for table in schema["tables"]:
            table_name = table["name"]
            table_path = os.path.join(SCHEMA_SUB_DIR, f"{table_name}.md")
            out_file.write(f"- [{table_name}]({table_path})\n")
        out_file.write("\n")

        if os.path.exists(os.path.join(DOCS_DIR, SCHEMA_DIAGRAM)):
            out_file.write(
                "## Schema Diagram\n"
                f"This diagram is generated from the [schema.yaml]({SCHEMA_PATH}) file using [make_erd.py](scripts/make_erd.py).\n"
                f"![Schema Diagram]({SCHEMA_DIAGRAM})\n"
            )
