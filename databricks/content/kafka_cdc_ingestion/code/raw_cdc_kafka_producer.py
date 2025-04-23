import json
import datetime
import random
import uuid
import base64

# --- Configuration ---
SOURCES_AND_KEYS = {
    "gods_and_goddesses": {
        "keys": ["pantheon_id", "god_id"],
        "attributes": ["name", "domain", "symbol", "power_level", "allegiance", "last_decree"],
        "pk_generator": lambda: ("olympian", f"{random.choice(['zeus', 'hera', 'poseidon', 'hades', 'apollo', 'artemis', 'athena', 'ares', 'hephaestus', 'aphrodite', 'hermes', 'dionysus'])}_{random.randint(1, 10):02d}")
    },
    "mythical_creatures": {
        "keys": ["creature_id"],
        "attributes": ["name", "type", "habitat", "danger_level", "abilities", "origin_myth_id"],
        "pk_generator": lambda: (f"creature_{uuid.uuid4()}",) # Single key
    },
    "heroic_deeds": {
        "keys": ["hero_id", "deed_id"],
        "attributes": ["description", "location", "monsters_defeated", "divine_intervention", "reward_granted", "status"],
        "pk_generator": lambda: (f"{random.choice(['hercules', 'theseus', 'perseus', 'odysseus', 'achilles'])}_{random.randint(1, 5):02d}", f"deed_{uuid.uuid4()}")
    }
}
REGIONS = ["us-olympus-1", "eu-elysium-1", "ap-tartarus-1"]
ACTIONS = ["INSERT", "MODIFY", "REMOVE"]

# --- Helper Functions ---

def generate_timestamp():
    """Generates a realistic ISO 8601 timestamp."""
    now = datetime.datetime.now(datetime.timezone.utc)
    offset = datetime.timedelta(seconds=random.uniform(-3600, 0))
    return (now + offset).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

def generate_sequence_number():
    """Generates a large, plausible sequence number string."""
    part1 = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S%f')[:17]
    part2 = random.randint(10**10, 10**18)
    return f"{part1}{part2}"

def generate_greek_value(attribute_name):
    """Generates placeholder Greek-themed values."""
    if "id" in attribute_name.lower():
        return str(uuid.uuid4())
    elif "name" in attribute_name.lower():
        return random.choice(["Golden Fleece", "Labyrinth", "Mount Olympus", "Underworld", "Trojan Horse", "Oracle of Delphi"])
    elif "level" in attribute_name.lower() or "power" in attribute_name.lower():
        return random.randint(1, 100)
    elif "status" in attribute_name.lower():
        return random.choice(["PENDING", "COMPLETED", "FAILED", "IMMORTALIZED", "BANISHED"])
    elif "description" in attribute_name.lower() or "decree" in attribute_name.lower() or "abilities" in attribute_name.lower():
         # Simulate a base64 encoded payload sometimes
        if random.random() < 0.2:
             payload = json.dumps({"detail": random.choice(["Slay the Hydra", "Retrieve the Golden Apples", "Resist the Sirens' Call", "Consult the Oracle"]), "severity": random.randint(1,5)})
             return base64.b64encode(payload.encode('utf-8')).decode('utf-8')
        else:
             return random.choice(["Beware the wrath of the gods", "Seek glory in battle", "Navigate the treacherous seas", "A quest for eternal fame"])
    else:
        return random.choice(["Ambrosia", "Nectar", "Ichor", "Styx", "Acheron", "Lethe", "Aegis", "Caduceus"])

def generate_item(attributes, pk_values, pk_names):
    """Generates a new item dictionary."""
    item = {}
    for i, key_name in enumerate(pk_names):
        item[key_name] = pk_values[i]
    for attr in attributes:
        if attr not in pk_names: # Don't overwrite PKs
             item[attr] = generate_greek_value(attr)
    # Ensure required keys always exist even if attributes list is short
    for key_name in pk_names:
        if key_name not in item:
             item[key_name] = generate_greek_value(key_name) # Generate a value if somehow missed
    return item


# --- Main Generation Logic ---

def generate_cdc_message():
    """Generates a single simulated CDC message as a JSON string."""
    source_name = random.choice(list(SOURCES_AND_KEYS.keys()))
    config = SOURCES_AND_KEYS[source_name]
    pk_names = config["keys"]
    attributes = config["attributes"]

    event_action = random.choice(ACTIONS)
    timestamp = generate_timestamp()
    seq_num = generate_sequence_number()
    region = random.choice(REGIONS)

    # Generate primary key values
    pk_values = config["pk_generator"]()

    message = {
        "eventaction": event_action,
        "eventtimestamp": timestamp,
        "sequencenumber": seq_num,
        "source": source_name,
        "region": region,
        "keys": pk_names,
    }

    # Add top-level key values
    for i, key_name in enumerate(pk_names):
        message[key_name] = pk_values[i]

    # Generate newitem and olditem based on action
    if event_action == "INSERT":
        message["newitem"] = generate_item(attributes, pk_values, pk_names)
        message["olditem"] = "" # Or potentially {} depending on exact source behavior
    elif event_action == "REMOVE":
        message["newitem"] = "" # Or potentially {}
        message["olditem"] = generate_item(attributes, pk_values, pk_names) # Generate a plausible old item
    elif event_action == "MODIFY":
        # Generate old state, then modify some fields for new state
        old_item = generate_item(attributes, pk_values, pk_names)
        new_item = old_item.copy() # Start with old item
        # Modify 1-3 non-key attributes
        non_key_attrs = [attr for attr in attributes if attr not in pk_names]
        if non_key_attrs: # Ensure there are non-key attributes to modify
            num_modifications = random.randint(1, min(3, len(non_key_attrs)))
            attrs_to_modify = random.sample(non_key_attrs, num_modifications)
            for attr in attrs_to_modify:
                new_item[attr] = generate_greek_value(attr) # Generate a *new* value
                # Ensure modification actually changes the value (simple check)
                while new_item[attr] == old_item.get(attr):
                     new_item[attr] = generate_greek_value(attr)
        else: # If only key attributes exist, just copy old to new for MODIFY (edge case)
             new_item = old_item

        message["newitem"] = new_item
        message["olditem"] = old_item

    return json.dumps(message)

# --- Formatting and Preview Functions ---

def format_and_print_record(record_json_string):
    """Parses a JSON string and prints it in a formatted way."""
    print("=" * 80) # Visual separator
    try:
        record = json.loads(record_json_string)
        print(f"Event Action: {record.get('eventaction', 'N/A')}")
        print(f"Timestamp:    {record.get('eventtimestamp', 'N/A')}")
        print(f"Sequence No:  {record.get('sequencenumber', 'N/A')}")
        print(f"Source Table: {record.get('source', 'N/A')}")
        print(f"Region:       {record.get('region', 'N/A')}")
        print(f"Keys:         {record.get('keys', 'N/A')}")
        # Print top-level keys
        if isinstance(record.get('keys'), list):
            for key in record['keys']:
                 print(f"  {key}: {record.get(key, 'N/A')}")

        print("-" * 20 + " New Item " + "-" * 20)
        if isinstance(record.get('newitem'), dict):
            print(json.dumps(record['newitem'], indent=2))
        else:
            print(record.get('newitem', 'N/A')) # Handle empty string or null

        print("-" * 20 + " Old Item " + "-" * 20)
        if isinstance(record.get('olditem'), dict):
            print(json.dumps(record['olditem'], indent=2))
        else:
            print(record.get('olditem', 'N/A')) # Handle empty string or null

    except json.JSONDecodeError:
        print("Error: Could not decode JSON record.")
        print(record_json_string) # Print raw string if decoding fails
    except Exception as e:
        print(f"An unexpected error occurred during formatting: {e}")
        print(record_json_string)
    print("=" * 80 + "\n") # Visual separator


def preview_records(num_records=5):
    """Generates and prints a specified number of formatted records."""
    print(f"--- Generating {num_records} Sample Greek Mythology CDC Messages ---")
    for i in range(num_records):
        print(f"--- Record {i+1} of {num_records} ---")
        record_str = generate_cdc_message()
        format_and_print_record(record_str)
    print(f"--- End of {num_records} Sample Messages ---")


# --- Main Execution Block ---
if __name__ == "__main__":
    preview_records(5) # Generate and print 5 records by default
