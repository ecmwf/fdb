#!/bin/bash
set -euo pipefail

BASE=/tmp/fdb/target/fdb/bin

for binary_path in "$BASE"/* ; do
    [ -f "$binary_path" ] || continue
    
    binary_name=$(basename "$binary_path")

    if [ "$(uname)" = "Darwin" ] ; then
        # Check if file is a Mach-O binary
        if file "$binary_path" | grep -q "Mach-O" ; then
            echo "Processing macOS binary: $binary_name"
            
            # Extract existing LC_RPATH entries
            current_rpaths=$(otool -l "$binary_path" | awk '/cmd LC_RPATH/{g=1;next} g&&/path/{print $2;g=0}')
            
            new_rpaths=()
            for rpath in $current_rpaths; do
                # Keep existing loader_path entries
                if [[ "$rpath" == *'@loader_path'* ]]; then
                    new_rpaths+=("$rpath")
                # Convert prereqs paths to relative loader_path entries
                elif [[ "$rpath" == *'prereqs'* ]]; then
                    # Extract the library folder name (e.g., metkitlib)
                    lib_dir=$(echo "$rpath" | sed -E 's|.*/prereqs/([^/]+)/.*|\1|')
                    new_rpaths+=("@loader_path/../../$lib_dir/lib64")
                fi
            done
            
            # Delete all old RPATHs and inject the new filtered/relative ones
            for rpath in $current_rpaths; do
                # Ignore errors if a specific path removal fails
                install_name_tool -delete_rpath "$rpath" "$binary_path" 2>/dev/null || true
            done
            
            for rpath in "${new_rpaths[@]}"; do
                install_name_tool -add_rpath "$rpath" "$binary_path"
            done
        fi

    else
        # Check if file is an ELF binary
        if file "$binary_path" | grep -q "ELF" ; then
            echo "Processing Linux binary: $binary_name"
            
            # Get current RPATH, suppress errors if none exists
            current_rpath=$(patchelf --print-rpath "$binary_path" 2>/dev/null || echo "")
            
            if [ -n "$current_rpath" ]; then
                # Split by colon into an array
                IFS=':' read -r -a rpath_array <<< "$current_rpath"
                new_rpaths=()
                
                for element in "${rpath_array[@]}"; do
                    # Keep existing ORIGIN entries
                    if [[ "$element" == *'$ORIGIN'* ]]; then
                        new_rpaths+=("$element")
                    # Convert prereqs paths to relative ORIGIN entries
                    elif [[ "$element" == *'prereqs'* ]]; then
                        # Extract the library folder name (e.g., metkitlib)
                        lib_dir=$(echo "$element" | sed -E 's|.*/prereqs/([^/]+)/.*|\1|')
                        new_rpaths+=("\$ORIGIN/../../$lib_dir/lib64")
                    fi
                done
                
                # Join the new array back together with colons
                joined_rpath=$(IFS=:; echo "${new_rpaths[*]}")
                
                # Update the binary and shrink it to only these elements
                patchelf --set-rpath "$joined_rpath" "$binary_path"
            fi
        fi
    fi
done
