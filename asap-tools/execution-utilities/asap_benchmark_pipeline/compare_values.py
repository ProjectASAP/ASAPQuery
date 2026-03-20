import argparse
import csv
import matplotlib.pyplot as plt
import re
import numpy as np

def extract_value(result_str):
    """Extracts the first numerical value from the result preview string."""
    if not result_str:
        return 0.0
    # Match integers or floats
    match = re.search(r"[-+]?\d*\.\d+|\d+", result_str)
    if match:
        return float(match.group())
    return 0.0

def load_results(csv_file):
    values = []
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['error']:
                    values.append(0.0)
                else:
                    values.append(extract_value(row['result_preview']))
    except FileNotFoundError:
        print(f"✗ Could not find {csv_file}")
        return []
    return values

def main():
    parser = argparse.ArgumentParser(description="Compare computed values from Baseline and ASAP runs.")
    parser.add_argument("--baseline", default="baseline_results.csv", help="Baseline CSV file")
    parser.add_argument("--asap", default="asap_results_run1.csv", help="ASAP CSV file")
    parser.add_argument("--output", default="value_comparison.png", help="Output image file")
    
    args = parser.parse_args()
    
    baseline_values = load_results(args.baseline)
    asap_values = load_results(args.asap)
    
    if not baseline_values or not asap_values:
        print("Missing data. Please make sure both CSVs exist and have data.")
        return

    # Ensure we only compare up to the matched length in case one failed early
    min_len = min(len(baseline_values), len(asap_values))
    baseline_values = baseline_values[:min_len]
    asap_values = asap_values[:min_len]
    
    # --- Plotting Code ---
    plt.figure(figsize=(12, 6))
    
    execution_order = np.arange(1, min_len + 1)
    bar_width = 0.4
    
    # Create grouped bars
    plt.bar(execution_order - bar_width/2, baseline_values, width=bar_width, 
            label='Baseline (Exact)', color='#1f77b4', edgecolor='black')
    plt.bar(execution_order + bar_width/2, asap_values, width=bar_width, 
            label='ASAP (Approximate)', color='#ff7f0e', edgecolor='black')
    
    plt.xlabel("Query Execution Order", fontsize=12, fontweight='bold')
    plt.ylabel("Computed Value (95th Quantile)", fontsize=12, fontweight='bold')
    plt.title("Query Output Comparison: Exact vs Approximate", fontsize=14, fontweight='bold')
    
    # Set tick marks at every 10 on the X axis
    plt.xticks(np.arange(0, min_len + 1, 10))
    
    plt.legend(loc='upper right')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    
    plt.savefig(args.output)
    print(f"✓ Value comparison graph successfully saved to {args.output}")

if __name__ == "__main__":
    main()