#!/bin/bash

# Script to process alibaba Node and MSResource data files with year-specific configurations 
# Usage: ./sort_and_format.sh <input_directory> --year <year> [-n] [-m]

usage() {
    echo "Usage: $0 <input_directory> --year <year> [-n] [-m]"
    echo "  <input_directory>  Path to directory containing data subdirectories"
    echo "  --year <year>      Year of data (2021 or 2022) - REQUIRED"
    echo "  -n                 Clean Node csv files and recompress as .csv.gz"
    echo "  -m                 Clean MSResource/MSMetrics csv files and recompress as .csv.gz"
    echo "  At least one of -n or -m must be specified"
    echo ""
    echo "Year-specific configurations:"
    echo "  2022: Uses NodeMetrics/ and MSMetrics/ subdirectories with NodeMetrics_*.tar.gz and MSMetrics_*.tar.gz files"
    echo "        Timestamp in first column for both"
    echo "  2021: Uses Node/ and MSResource/ subdirectories with Node_*.tar.gz and MSResource_*.tar.gz files"
    echo "        Timestamp in second column for Node data, seventh column for MSResource data"
    exit 1
}

# Function to process files in a given directory with a specific pattern
process_files() {
    local subdir="$1"
    local pattern="$2"
    local timestamp_col="$3"
    local full_path="${INPUT_DIR}/${subdir}"
    
    if [[ ! -d "$full_path" ]]; then
        echo "Warning: Directory $full_path does not exist, skipping..."
        return
    fi
    
    echo "Processing files in $full_path"
    
    # Find all files matching the pattern, sorted by index
    local files=($(ls "$full_path"/${pattern}_*.tar.gz 2>/dev/null | sort -V))
    
    if [[ ${#files[@]} -eq 0 ]]; then
        echo "No files matching ${pattern}_*.tar.gz found in $full_path"
        return
    fi
    
    echo "Found ${#files[@]} files in $subdir:"
    
    for file in "${files[@]}"; do
        echo "Processing: $(basename "$file")"
        
        # Create temporary directory for processing
        local temp_dir=$(mktemp -d)
        local base_name=$(basename "$file" .tar.gz)
        
        echo "  -> Extracting $file to temporary directory..."
        if ! tar -xzf "$file" -C "$temp_dir"; then
            echo "  -> Error: Failed to extract $file"
            rm -rf "$temp_dir"
            continue
        fi
        
        # Find the extracted CSV file
        local csv_file=$(find "$temp_dir" -name "*.csv" -type f | head -1)
        if [[ -z "$csv_file" ]]; then
            echo "  -> Error: No CSV file found in extracted archive"
            rm -rf "$temp_dir"
            continue
        fi
        
        # Check if file is already sorted using sort -c
        echo "  -> Checking if file is already sorted..."
        if tail -n +2 "$csv_file" | sort -t',' -k${timestamp_col},${timestamp_col}n -c 2>/dev/null; then
            echo "  -> File is already sorted, skipping sort step"
        else
            echo "  -> Sorting CSV file by timestamp (column $timestamp_col)..."
            # Use external sort for memory efficiency with large files
            # Preserve header line by extracting first line, sorting the rest, then combining
            # -t',' specifies comma as field separator
            # -k${timestamp_col},${timestamp_col}n sorts by specified field numerically
            # -S 1G uses 1GB of memory for sorting (adjust if needed)
            # --temporary-directory ensures temp files go to a writable location
            local sorted_file="${temp_dir}/sorted.csv"
            if ! (head -n 1 "$csv_file"; tail -n +2 "$csv_file" | sort -t',' -k${timestamp_col},${timestamp_col}n -S 1G --temporary-directory="$temp_dir") > "$sorted_file"; then
                echo "  -> Error: Failed to sort CSV file"
                rm -rf "$temp_dir"
                continue
            fi
            mv "$sorted_file" "$csv_file"
        fi
        
        echo "  -> Compressing sorted file..."
        local output_file="${full_path}/${base_name}.csv.gz"
        if ! gzip -c "$csv_file" > "$output_file"; then
            echo "  -> Error: Failed to compress sorted file"
            rm -rf "$temp_dir"
            continue
        fi
        
        echo "  -> Successfully processed: $(basename "$output_file")"
        
        # Clean up temporary directory
        rm -rf "$temp_dir"
    done
}

# Parse command line arguments
if [[ $# -lt 4 ]]; then
    usage
fi

INPUT_DIR="$1"
shift

YEAR=""
PROCESS_NODE=false
PROCESS_MS=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --year)
            YEAR="$2"
            if [[ "$YEAR" != "2021" && "$YEAR" != "2022" ]]; then
                echo "Error: Year must be either 2021 or 2022"
                usage
            fi
            shift 2
            ;;
        -n)
            PROCESS_NODE=true
            shift
            ;;
        -m)
            PROCESS_MS=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Validate required arguments
if [[ -z "$YEAR" ]]; then
    echo "Error: --year parameter is required"
    usage
fi

# Validate input directory
if [[ ! -d "$INPUT_DIR" ]]; then
    echo "Error: Input directory '$INPUT_DIR' does not exist"
    exit 1
fi

# Check that at least one flag is specified
if [[ "$PROCESS_NODE" == false && "$PROCESS_MS" == false ]]; then
    echo "Error: At least one of -n or -m must be specified"
    usage
fi

echo "Input directory: $INPUT_DIR"
echo "Year: $YEAR"
echo "Process Node data: $PROCESS_NODE"
echo "Process MSResource data: $PROCESS_MS"
echo

# Configure year-specific settings
if [[ "$YEAR" == "2022" ]]; then
    NODE_SUBDIR="NodeMetrics"
    MS_SUBDIR="MSMetrics"
    NODE_PATTERN="NodeMetrics"
    MS_PATTERN="MSMetrics"
    NODE_TIMESTAMP_COL=1
    MS_TIMESTAMP_COL=1
else  # 2021
    NODE_SUBDIR="Node"
    MS_SUBDIR="MSResource"
    NODE_PATTERN="Node"
    MS_PATTERN="MSResource"
    NODE_TIMESTAMP_COL=2
    MS_TIMESTAMP_COL=7
fi

# Process files based on flags
if [[ "$PROCESS_NODE" == true ]]; then
    process_files "$NODE_SUBDIR" "$NODE_PATTERN" "$NODE_TIMESTAMP_COL"
fi

if [[ "$PROCESS_MS" == true ]]; then
    process_files "$MS_SUBDIR" "$MS_PATTERN" "$MS_TIMESTAMP_COL"
fi

echo "Processing complete!"