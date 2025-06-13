def minimize_content(lines: list[str]):

    result = []

    for line in lines:
        
        if(line.strip() == ""):
            continue

        if len(result) > 0 and not result[-1].strip().endswith(";"):
            result[-1] += line.strip()
        else:
            result.append(line.strip())

    return result

def remove_comments_and_docs(lines):

    processed_lines = []

    for line in lines:
        stripped_line = line.strip()

        # Skip comments and empty lines
        if stripped_line.startswith("/") or stripped_line.startswith("*") or stripped_line.startswith("#") or stripped_line == "":
            continue

        # Skip extern
        if "extern" in stripped_line:
            continue

        processed_lines.append(stripped_line)


    return processed_lines

def process_fdb_header(root_dir):
    fdb_c_header_path = root_dir / "src" / "fdb5" / "api" / "fdb_c.h"

    collapsed_header_content = None

    with open(fdb_c_header_path.resolve(), "r") as fdb_c_header_file:
        all_lines = fdb_c_header_file.readlines()
        collapsed_header_content = remove_comments_and_docs(all_lines)

    return collapsed_header_content

