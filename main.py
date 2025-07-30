import json
from datetime import datetime, timezone

def transform_telemetry_iso(data):
    """
    Transforms telemetry data from a format with ISO timestamps
    to the unified target format.
    """
    transformed_list = []
    for record in data:
        # IMPLEMENT: Convert ISO 8601 timestamp string to Unix milliseconds.
        # 1. Parse the ISO string (ending in 'Z' for Zulu/UTC) into a datetime object.
        iso_string = record['iso_timestamp']
        dt_object = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))

        # 2. Convert the datetime object to a Unix timestamp (seconds) and then to milliseconds.
        timestamp_ms = int(dt_object.timestamp() * 1000)

        # Create the new dictionary in the target format.
        transformed_list.append({
            "eventId": record["eventId"],
            "source": record["device"],
            "timestamp": timestamp_ms,
            "telemetry": {
                "value": record["value"]
            }
        })
    return transformed_list

def transform_telemetry_ms(data):
    """
    Transforms telemetry data from a format with millisecond timestamps
    to the unified target format.
    """
    transformed_list = []
    for record in data:
        # IMPLEMENT: Map the fields from the source format to the target format.
        # The timestamp is already in the correct millisecond format.
        transformed_list.append({
            "eventId": record["id"],
            "source": record["source"],
            "timestamp": record["ts"],
            "telemetry": {
                "value": record["reading"]
            }
        })
    return transformed_list


# --- Testing and Execution Logic (Provided by the original project) ---

def run():
    """Main function to run the solution."""
    # Load the datasets
    with open('data-1.json', 'r') as f:
        data_1 = json.load(f)
    with open('data-2.json', 'r') as f:
        data_2 = json.load(f)
    with open('data-result.json', 'r') as f:
        data_result = json.load(f)

    # Transform both datasets
    transformed_1 = transform_telemetry_iso(data_1)
    transformed_2 = transform_telemetry_ms(data_2)

    # Merge and sort the results
    # The key for sorting is the 'timestamp' field.
    solution = sorted(transformed_1 + transformed_2, key=lambda x: x['timestamp'])

    # Verify the solution
    if solution == data_result:
        print("✅ Solution is correct!")
    else:
        print("❌ Solution is incorrect.")
        # To help with debugging, you can print the differences
        # print("\nYour solution:")
        # print(json.dumps(solution, indent=2))
        # print("\nExpected solution:")
        # print(json.dumps(data_result, indent=2))

if __name__ == "__main__":
    run()
