import pathlib
import git
import re

def collapse_lines_between_curly_brackets(lines):

    processed_lines = []

    for line in lines:
        stripped_line = line.strip()
        last_line_stripped = None

        # Skip comments and empty lines
        if stripped_line.startswith("/") or stripped_line.startswith("*") or stripped_line.startswith("#") or stripped_line == "":
            continue

        # Skip extern
        if "extern" in stripped_line:
            continue

        # Set last line
        if len(processed_lines) > 0:
            last_line_stripped = processed_lines[-1].strip()

        # Merge formatted lines ending in comma
        if last_line_stripped and last_line_stripped.endswith(","):
           processed_lines[-1] += stripped_line
           continue

        # Merge curly braces
        if last_line_stripped and (last_line_stripped.count("{") > last_line_stripped.count("}")):
           processed_lines[-1] += stripped_line
           continue
            

        processed_lines.append(stripped_line)


    return processed_lines

def process_fdb_header():
    repo = git.Repo('.', search_parent_directories=True)
    root_dir = pathlib.Path(repo.working_tree_dir)

    fdb_c_header_path = root_dir / "src" / "fdb5" / "api" / "fdb_c.h"

    pattern = r'^((int|void|enum|const|struct|typedef)\b.*?;|enum\s+\w+\s*\{[^}]*\}\s*;)'

    collapsed_header_content = None

    with open(fdb_c_header_path, "r") as fdb_c_header_file:
        all_lines = fdb_c_header_file.readlines()
        collapsed_header_content = collapse_lines_between_curly_brackets(all_lines)

    matches = []

    for line in collapsed_header_content:
        matches.append(re.match(pattern, line))

    result = "\n".join([match.string for match in matches])

    return result

