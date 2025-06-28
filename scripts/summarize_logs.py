import json
from collections import defaultdict
import argparse
import os
from datetime import datetime, timezone

def parse_iso_timestamp(ts_str):
    """Parses an ISO 8601 timestamp string into a datetime object."""
    # Handle both with and without fractional seconds
    if '.' in ts_str:
        return datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
    else:
        return datetime.fromisoformat(ts_str.replace('Z', '+00:00'))

def summarize_transaction_logs(log_file_path, output_file_path=None):
    """
    Reads streaming transaction logs, groups them by transaction_id,
    and generates a one-line summary for each completed transaction.
    """
    if not os.path.exists(log_file_path):
        print(f"Error: Input file not found at '{log_file_path}'")
        return

    # Step 1: Group all events by transaction_id
    transactions_events = defaultdict(list)
    print(f"Processing and grouping events from: {log_file_path}...")
    with open(log_file_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                log_entry = json.loads(line)
                fields = log_entry.get("fields", {})
                tx_id = fields.get("transaction_id")
                if tx_id is not None:
                    transactions_events[tx_id].append(log_entry)
            except json.JSONDecodeError:
                continue
    
    print(f"Found {len(transactions_events)} unique transactions. Now summarizing...")

    summaries = []
    # Step 2: Iterate through each transaction's events to build a summary
    for tx_id, events in transactions_events.items():
        if not events:
            continue

        # Sort events by time to process them chronologically
        events.sort(key=lambda x: x['timestamp'])

        summary = {
            "transaction_id": tx_id,
            "status": "UNKNOWN", # Default status
            "metrics": {},
            "timeline_ms": {},
            "error": None,
        }

        # Store start times of sub-processes to calculate duration
        start_times = {}
        
        # Extract initial info from the first event (usually 'task_created')
        first_event_fields = events[0].get("fields", {})
        summary["symbol"] = first_event_fields.get("symbol")
        summary["interval"] = first_event_fields.get("interval")

        # Process all events for this transaction
        for event in events:
            fields = event.get("fields", {})
            event_name = fields.get("event_name")
            timestamp = parse_iso_timestamp(event["timestamp"])

            if event_name:
                # Handle start events
                if event_name.endswith("_start"):
                    start_times[event_name] = timestamp
                
                # Handle success events
                elif event_name.endswith("_success"):
                    if event_name == "processing_success":
                        summary["status"] = "SUCCESS"
                    start_event_name = event_name.replace("_success", "_start")
                    if start_event_name in start_times:
                        duration = (timestamp - start_times[start_event_name]).total_seconds() * 1000
                        # e.g., "api_call_success" -> "api_call"
                        timeline_key = start_event_name.replace("_start", "")
                        summary["timeline_ms"][timeline_key] = round(duration, 2)
                    
                    # Collect metrics from success events
                    if "saved_kline_count" in fields:
                        summary["metrics"]["saved_kline_count"] = fields["saved_kline_count"]
                    if "received_kline_count" in fields:
                        summary["metrics"]["received_kline_count"] = fields["received_kline_count"]

                # Handle failure events
                elif event_name.endswith("_failure"):
                    summary["status"] = "FAILURE"
                    summary["error"] = fields.get("error.details") or fields.get("reason", "Unknown error")
                    start_event_name = event_name.replace("_failure", "_start")
                    if start_event_name in start_times:
                         duration = (timestamp - start_times[start_event_name]).total_seconds() * 1000
                         timeline_key = start_event_name.replace("_start", "")
                         summary["timeline_ms"][timeline_key] = round(duration, 2)


        # Calculate total duration
        start_ts = parse_iso_timestamp(events[0]["timestamp"])
        end_ts = parse_iso_timestamp(events[-1]["timestamp"])
        summary["duration_ms"] = round((end_ts - start_ts).total_seconds() * 1000, 2)

        summaries.append(summary)

    # Step 3: Sort summaries by transaction_id and output
    summaries.sort(key=lambda x: x["transaction_id"])

    if output_file_path:
        output_dir = os.path.dirname(output_file_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        with open(output_file_path, 'w', encoding='utf-8') as f:
            for summary in summaries:
                json.dump(summary, f)
                f.write('\n')
        print(f"Successfully generated summary file at: {output_file_path}")
    else:
        for summary in summaries:
            print(json.dumps(summary, indent=2))

def main():
    parser = argparse.ArgumentParser(
        description="Summarize streaming transaction logs into one-line-per-transaction.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "input_file",
        help="Path to the raw, streaming transaction log file."
    )
    parser.add_argument(
        "-o", "--output",
        help="Path to the output summary file. If not provided, prints to console."
    )
    args = parser.parse_args()
    
    summarize_transaction_logs(args.input_file, args.output)

if __name__ == "__main__":
    main()