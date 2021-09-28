from tecton import batch_feature_view, Input, DatabricksClusterConfig, BackfillConfig, FeatureAggregation, WINDOW_UNBOUNDED_PRECEDING
from fraud.data_sources.hive_batch import transaction_events_batch
from fraud.my_entity import user 
from datetime import datetime

@batch_feature_view(
    inputs={'transaction_events_batch': Input(transaction_events_batch,
                                              WINDOW_UNBOUNDED_PRECEDING,
                                              '1hr')
            },
    entities=[user],
    mode='spark_sql',
    online=True,
    offline=True,
    feature_start_time=datetime(2021, 1, 1),
    batch_schedule='1d',
    batch_cluster_config = DatabricksClusterConfig(
        instance_type = 'm5.2xlarge',
        spark_config = {"spark.executor.memory" : "12g"}
    ),
    ttl='120d',
    backfill_config=BackfillConfig("multiple_batch_schedule_intervals_per_job"),
    family='fraud',
    tags={'release': 'production'},
    owner='eddie@tecton.ai',
    description='How many transactions during the wee hours of the morning a user typically makes'
)


def user_nighttime_transaction_count_transformation(transaction_events_batch):
    return f'''
        select nameorig as user_id, count(*), max(timestamp) from {transaction_events_batch} where hour(to_timestamp(timestamp)) >= 0 and hour(to_timestamp(timestamp)) <= 4 group by nameorig 
        '''