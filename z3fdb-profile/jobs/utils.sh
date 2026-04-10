(return 0 2>/dev/null) || { echo "Source this file, don't execute it" >&2; exit 1; }


upload_files() {
    # ──────────────────────────────────────────────────────────────────────────
    # Upload files to ECMWF Sites API under a specified path prefix.
    #
    # USAGE:
    #     upload_files <prefix> <file> [file...]
    #
    # ARGUMENTS:
    #     prefix    Remote path prefix (e.g., "/reports/2025")
    #     file...   One or more local file paths to upload
    #
    # ENVIRONMENT:
    #     SITES_TOKEN_FILE defaults to ~/.sites-token
    #         Must contain a valid API bearer token
    #
    # DEPENDENCIES:
    #     curl, jq
    #
    # OUTPUT:
    #     Progress is reported to stdout for each file:
    #         [n/total] ✓ file → remote_path    (success)
    #         [n/total] ✗ file - error_message  (failure, to stderr)
    #
    # EXAMPLES:
    #     # Upload single file
    #     upload_files "/reports" index.html
    #
    #     # Upload multiple files
    #     upload_files "/static/css" styles.css reset.css theme.css
    #
    #     # Upload with glob expansion
    #     upload_files "/images" assets/*.png
    #
    #     # Upload from find (files with spaces need different handling)
    #     while IFS= read -r -d '' f; do
    #         upload_files "/backup" "$f"
    #     done < <(find . -name "*.html" -print0)
    #
    # RETURNS:
    #     0   All files uploaded successfully
    #     1   One or more uploads failed
    # ──────────────────────────────────────────────────────────────────────────
    
    local prefix="$1"
    shift
    local files=("$@")
    
    if [[ -z "$prefix" || ${#files[@]} -eq 0 ]]; then
        echo "Usage: upload_files <prefix> <file> [file...]" >&2
        return 1
    fi
    
    local token_file="${SITES_TOKEN_FILE:-$HOME/.sites-token}"
    local base_url="https://sites.ecmwf.int/ecm7593/z3fdb/s/api/v2/files"
    local token
    token=$(cat "$token_file") || { echo "Error: Cannot read $token_file" >&2; return 1; }
    
    local total=${#files[@]}
    local count=0
    local failures=0
    
    for file in "${files[@]}"; do
        count=$((count + 1))
        
        if [[ ! -f "$file" ]]; then
            echo "[${count}/${total}] ✗ ${file} - File not found" >&2
            failures=$((failures + 1))
            continue
        fi
        
        # URL-encode the destination path
        local dest_path="${prefix%/}/${file}"
        local encoded_path
        encoded_path=$(printf '%s' "$dest_path" | jq -sRr @uri)
        
        # Determine content type (basic detection)
        local content_type="application/octet-stream"
        case "${file,,}" in  # ${,,} for case-insensitive matching
            *.html|*.htm) content_type="text/html" ;;
            *.css)        content_type="text/css" ;;
            *.js)         content_type="application/javascript" ;;
            *.json)       content_type="application/json" ;;
            *.xml)        content_type="application/xml" ;;
            *.png)        content_type="image/png" ;;
            *.jpg|*.jpeg) content_type="image/jpeg" ;;
            *.gif)        content_type="image/gif" ;;
            *.svg)        content_type="image/svg+xml" ;;
            *.pdf)        content_type="application/pdf" ;;
            *.txt)        content_type="text/plain" ;;
        esac
        
        local response
        response=$(curl -s -X PUT \
            "${base_url}/${encoded_path}?backup_if_present=false" \
            -H 'accept: application/json' \
            -H "Authorization: Bearer ${token}" \
            -H 'Content-Type: multipart/form-data' \
            -F "file=@${file};type=${content_type}")
        
        local status
        status=$(echo "$response" | jq -r '.status // empty')
        
        if [[ "$status" == "OK" ]]; then
            echo "[${count}/${total}] ✓ ${file} → ${dest_path}"
        else
            echo "[${count}/${total}] ✗ ${file} - $(echo "$response" | jq -r '.message // "Unknown error"')" >&2
            failures=$((failures + 1))
        fi
    done
    
    # Summary if multiple files
    if [[ $total -gt 1 ]]; then
        echo "────────────────────────────────"
        echo "Uploaded: $((total - failures))/${total} files"
    fi
    
    [[ $failures -eq 0 ]]
}
