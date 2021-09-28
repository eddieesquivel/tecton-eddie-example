from tecton import batch_feature_view, Input, transformation, DatabricksClusterConfig, tecton_sliding_window, const, BackfillConfig, FeatureAggregation, WINDOW_UNBOUNDED_PRECEDING
from fraud.data_sources.hive_batch import transaction_events_batch
from fraud.my_entity import user 
from datetime import datetime

@transformation(mode='spark_sql')
def user_nighttime_transaction_count_transformation(window_input_df):
    return f'''
        select nameorig as user_id, count(*), window_end AS timestamp from {window_input_df} where hour(to_timestamp(timestamp)) >= 0 and hour(to_timestamp(timestamp)) <= 4 group by nameorig, window_end 
        '''


@batch_feature_view(
    inputs={'transaction_events_batch': Input(transaction_events_batch, window='30d')},
    entities=[user],
    mode='pipeline',
    backfill_config=BackfillConfig("multiple_batch_schedule_intervals_per_job"),
    online=True,
    offline=True,
    feature_start_time=datetime(2021, 1, 1),
    batch_schedule='1d',
    batch_cluster_config = DatabricksClusterConfig(
        instance_type = 'm5.2xlarge',
        spark_config = {"spark.executor.memory" : "12g"}
    ),
    ttl='120d',
    family='fraud',
    tags={'release': 'production'},
    owner='eddie@tecton.ai',
    description='How many transactions during the wee hours of the morning a user typically makes'
)
def user_nighttime_transaction_count_30d(transaction_events_batch):
    return user_nighttime_transaction_count_transformation(
        # Use tecton_sliding_transformation to create trailing 30 day time windows.
        # The slide_interval defaults to the batch_schedule (1 day).
        tecton_sliding_window(transaction_events_batch,
            timestamp_key=const('timestamp'),
            window_size=const('30d')))


