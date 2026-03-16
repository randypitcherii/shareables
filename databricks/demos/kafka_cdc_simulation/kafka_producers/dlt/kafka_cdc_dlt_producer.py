from datetime import datetime, timezone
import uuid
import random
import json

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
import dlt

# Initialize WorkspaceClient to access dbutils
w = WorkspaceClient()
dbutils = w.dbutils

# Initialize Spark session for Databricks Connect
spark = DatabricksSession.builder.getOrCreate()

# CDC Generator Configuration
"""
Focusing on the following situation:
- ingesting 2-3M records every 3 hours (going with 2.5M to split the difference)
- 30kb average payload size
- 30k updates every 3 hours, so 30k/2.5M is the update probability
"""
KAFKA_TOPIC = 'rpw_cdc_simulation__sad_lightning'
MESSAGES_PER_HOUR = int(2.5 * 10**6 / 3)
PAYLOAD_SIZE_KB = 30
CDC_UPDATE_PROBABILITY = (30 * 10**3 / 3) / MESSAGES_PER_HOUR

# get secrets
KAFKA_BOOTSTRAP_SERVERS = dbutils.secrets.get(scope="randy_pitcher_workspace_kafka_cdc", key="KAFKA_BROKERS")
KAFKA_USERNAME = dbutils.secrets.get(scope="randy_pitcher_workspace_kafka_cdc", key="KAFKA_USERNAME")
KAFKA_PASSWORD = dbutils.secrets.get(scope="randy_pitcher_workspace_kafka_cdc", key="KAFKA_PASSWORD")

# unique identifier for this run
RUN_STARTED_AT = datetime.now(timezone.utc).isoformat()

# CDC message payload
PAYLOAD_STRING = "🪐" * int(PAYLOAD_SIZE_KB * 1024 / 4) # Each 🪐 emoji is 4 bytes
PAYLOAD_VALUE_CHOICES = ["😀","😂","🥲","😍","😎","🤩","🥳","😇","🤓","🧐","😜","🤪","😱","🥶","🥵","🤠","😺","🙈","🐶","🐱","🦄","🐸","🐼","🐧","🐢","🐙","🦋","🌸","🌞","🌈","🍕","🍔","🍣","🍩","🍦","🍉","🍓","🥑","🥨","🍿","⚽","🏀","🏈","🎾","🏓","🎲","🎸","🎧","🎮","🚗","✈️","🚀","🛸","⛵","🏰","🗽","🌋","🪐","⭐","🌟","🌠","🌌","🌃","🌁","🌆","🌇","🌉","🌊","🌬️","🌪️","🌫️","🌩️","🌨️","🌧️","🌦️","🌥️","🌤️","🌞","🌝","🌚","🌕","🌖","🌗","🌘","🌑","🌒","🌓","🌔","🌙","🌎","🌍","🌏","🪁","🪃","🪀","🪅","🪆","🪩","🪨","🪵","🪺","🪹"]

def generate_record_payload(updated_at_timestamp):
  if updated_at_timestamp is None:
    raise ValueError("updated_at_timestamp cannot be None")
  
  record_payload = {
    "payload_string": PAYLOAD_STRING,
    "updated_at": updated_at_timestamp,
    "value": random.choice(PAYLOAD_VALUE_CHOICES)
  }
  return record_payload
  

def generate_cdc_message(record_id=None, record_type=None):
  """
  Generates a simulated CDC message.

  Parameters:
  - record_id: 
    Optional ID for the record. If provided, the message will be an UPDATE unless overridden by record_type. 
    If not provided, a new ID will be generated and the record will be an INSERT unless overridden by record_type.

  - record_type: 
    Optional string to overide record type logic. Must be one of "INSERT" or "UPDATE".
  """
  if record_type and record_type not in ["INSERT", "UPDATE"]:
    raise ValueError(f"Expecting record_type to be one of 'INSERT' or 'UPDATE', but got '{record_type}'")

  # determine if update or insert
  if record_id:
    record_type = record_type if record_type else "UPDATE" # use override if provided, else UPDATE
  else:
    record_id = str(uuid.uuid4())
    record_type = record_type if record_type else "INSERT" # use override if provided, else INSERT

  # get message time
  now = datetime.now(timezone.utc)
  now_iso_string = now.isoformat()
  now_microsecond_epoch = int(now.timestamp() * 10 * 10**6)

  message = {
    "record_id": record_id,
    "record_type": record_type,
    "record_sequence": now_microsecond_epoch,
    "source": "Lakeflow Declarative Pipeline - Kafka Sink",
    "run_started_at": RUN_STARTED_AT,
    "record_payload": generate_record_payload(now_iso_string)
  }
  return message


#---------------------
# Begin Pipelines
#---------------------
# Create Kafka sink using DLT Sink API
dlt.create_sink(
  name="kafka_cdc_sink",
  format="kafka",
  options={
    "kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "kafka.sasl.mechanism": "SCRAM-SHA-512",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";',
    "topic": KAFKA_TOPIC
  }
)

@dlt.table(
  comment="Initial CDC records for update pool (IDs 1-100)"
)
def initial_cdc_records():
  """Create initial INSERT records for the update pool."""
  initial_data = [ # get 100 records. Kafka sinks require a single value column with string type
    {"key": str(i), "value": json.dumps(generate_cdc_message(record_id=i, record_type="INSERT"))} for i in range(1, 101)
  ]
  df = spark.createDataFrame(initial_data)
  return df 

@dlt.append_flow(
  target="kafka_cdc_sink",
  comment="Inserts initial records to kafka sink one time",
  once=True
)
def append_initial_cdc_records():
  return spark.readStream.table('initial_cdc_records')