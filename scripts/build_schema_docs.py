# Script to build markdown documentation from the schema/schema.yaml file

import yaml

SCHEMA_PATH = "simple/schema.yaml"
OUT_DIR = "docs/schema/"

# Loop over each table in the schema
with open(SCHEMA_PATH, "r") as schema_file:
    schema = yaml.safe_load(schema_file)

    for table in schema["tables"]:
        table_name = table["name"]

        # Prepare a markdown file per table
        with open(f"{OUT_DIR}{table_name}.md", "w") as out_file:
            out_file.write(f"# {table_name}\n")
            out_file.write(f"{table['description']}\n")
            out_file.write(
                "| Column Name | Description | Datatype | Length | Units  | UCD | Nullable |\n"
            )
            out_file.write("| --- | --- | --- | --- | --- | --- | --- |\n")

            # Loop over column names to get the column information
            for column in table["columns"]:
                # Get the unit from the fits or ivoa tags
                units = column.get("fits:tunit", "")
                if units == "":
                    units = column.get("ivoa:unit", "")

                # Write out the column
                out_file.write(
                    f"| {column['name']} | {column['description']} | {column['datatype']} | {column.get('length', '')} | {units} | {column.get('ivoa:ucd', '')} | {column.get('nullable', 'True')} |\n"
                )
            out_file.write("\n")

            # Handle any indexes
            if "indexes" in table:
                out_file.write("### Indexes\n")
                out_file.write("| Name | Columns | Description |\n")
                out_file.write("| --- | --- | --- |\n")
                for index in table["indexes"]:
                    out_file.write(
                        f"| {index['name']} | {index['columns']} | {index.get('description', '')} |\n"
                    )
                out_file.write("\n")

            # Handle any constraints
            if "constraints" in table:
                out_file.write("### Constraints\n")
                out_file.write(
                    "| Type | Description | Columns | Referenced Columns |\n"
                )
                out_file.write("| --- | --- | --- | --- |\n")
                for constraint in table["constraints"]:
                    out_file.write(
                        f"| {constraint['@type']} | {constraint['description']} | {constraint.get('columns', '')} | {constraint.get('referencedColumns', '')} |\n"
                    )
                out_file.write("\n")
