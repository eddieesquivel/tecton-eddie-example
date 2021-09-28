import pyspark
from tecton import HiveDSConfig, BatchDataSource
from tecton_spark.function_serialization import inlined

hive_transaction_events_batch = HiveDSConfig(
        database='demo_fraud',
        table='transactions',
        timestamp_column_name='timestamp'
)


transaction_events_batch = BatchDataSource(
        name='transaction_events_batch',
        batch_ds_config=hive_transaction_events_batch,
        family='fraud',
        owner='eddie@tecton.ai',
        tags={'release': 'production'}
)