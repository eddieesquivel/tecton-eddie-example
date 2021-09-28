import pyspark
from tecton import KinesisDSConfig, HiveDSConfig, StreamDataSource
from tecton_spark.function_serialization import inlined

# connecting to this kinesis stream: https://us-west-2.console.aws.amazon.com/kinesis/home?region=us-west-2#/streams/details/transaction_events/
# connecting to this historical batch table: https://us-west-2.console.aws.amazon.com/glue/home?region=us-west-2#table:catalog=472542229217;name=transactions;namespace=demo_fraud

@inlined
def raw_data_deserialization(df):
    from pyspark.sql.functions import col, from_json, from_utc_timestamp, when
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, BooleanType, IntegerType

    payload_schema = (
      StructType()
            .add('amount', StringType(), False)
            .add('nameOrig', StringType(), False)
            .add('nameDest', StringType(), False)
            .add('isFraud', StringType(), False)
            .add('isFlaggedFraud', StringType(), False)
            .add('type_CASH_IN', StringType(), False)
            .add('type_CASH_OUT', StringType(), False)
            .add('type_DEBIT', StringType(), False)
            .add('type_PAYMENT', StringType(), False)
            .add('type_TRANSFER', StringType(), False)
            .add('timestamp', StringType(), False)
    )

    return (
        df.selectExpr('cast (data as STRING) jsonData')
        .select(from_json('jsonData', payload_schema).alias('payload'))
        .select(
            col('payload.amount').cast('long').alias('amount'),
            col('payload.nameOrig').alias('nameOrig'),
            col('payload.nameDest').alias('nameDest'),
            col('payload.isFraud').cast('long').alias('isFraud'),
            col('payload.isFlaggedFraud').cast('long').alias('isFlaggedFraud'),
            col('payload.type_CASH_IN').cast('long').alias('type_CASH_IN'),
            col('payload.type_CASH_OUT').cast('long').alias('type_CASH_OUT'),
            col('payload.type_DEBIT').cast('long').alias('type_DEBIT'),
            col('payload.type_PAYMENT').cast('long').alias('type_PAYMENT'),
            col('payload.type_TRANSFER').cast('long').alias('type_TRANSFER'),
            from_utc_timestamp('payload.timestamp', 'UTC').alias('timestamp')
        )
    )



kinesis_transaction_events_stream = KinesisDSConfig(
        stream_name='transaction_events',
        region='us-west-2',
        timestamp_key='timestamp',
        default_watermark_delay_threshold="30 minutes",
        default_initial_stream_position="trim_horizon",
        raw_stream_translator=raw_data_deserialization,
        options={'roleArn': 'arn:aws:iam::472542229217:role/demo-cross-account-kinesis-ro'}
    )

hive_transaction_events_batch = HiveDSConfig(
        database='demo_fraud',
        table='transactions',
        timestamp_column_name='timestamp'
    )


transaction_events_stream = StreamDataSource(
        name="transaction_events",
        batch_ds_config=hive_transaction_events_batch,
        stream_ds_config=kinesis_transaction_events_stream,
        family='fraud',
        owner='eddie@tecton.ai',
        tags={'release': 'production'}
    )


