from tecton.feature_views import batch_window_aggregate_feature_view
from tecton.feature_views.feature_view import Input
from tecton import FeatureAggregation
from fraud.my_entity import user
from fraud.data_sources.hive_batch import transaction_events_batch
from datetime import datetime


@batch_window_aggregate_feature_view(
    inputs={'transactions': Input(transaction_events_batch)},
    entities=[user],
    mode='spark_sql',
    aggregation_slide_period='1d',
    aggregations=[FeatureAggregation(column='transaction', function='max', time_windows=['24h','72h','168h', '960h'])],
    online=True,
    offline=True,
    feature_start_time=datetime(2020, 10, 10),
    family='fraud',
    tags={'release': 'production'},
    owner='eddie@tecton.ai',
    description=''
)
def user_transaction_counts(transactions):
    return f'''
        SELECT
            nameorig as user_id,
            amount as transaction,
            timestamp
        FROM
            {transactions}
        '''
