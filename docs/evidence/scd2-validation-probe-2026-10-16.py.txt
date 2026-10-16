"""Disposable measured candidate, not a change to the production job."""
import json
import os
import time
from datetime import datetime
from pathlib import Path

import commerce_customer_scd2 as history
from pyspark.sql import Window, functions as F
from spark_catalog import build_session


def candidate(spark):
    target = spark.table(history.TABLE)
    ordered = Window.partitionBy('customer_id').orderBy('valid_from')
    target = target.withColumn('previous_valid_to', F.lag('valid_to').over(ordered))
    invalid = (
        (F.col('valid_to').isNotNull() & (F.col('valid_to') <= F.col('valid_from')))
        | (F.col('is_current') & F.col('valid_to').isNotNull())
        | (~F.col('is_current') & F.col('valid_to').isNull())
        | (F.col('previous_valid_to').isNotNull()
           & (F.col('previous_valid_to') != F.col('valid_from')))
    )
    grouped = target.groupBy('customer_id').agg(
        F.sum(F.when(F.col('is_current'), 1).otherwise(0)).alias('current_versions'),
        F.max(F.when(invalid, 1).otherwise(0)).alias('invalid_period'),
    )
    if grouped.filter((F.col('current_versions') != 1) | (F.col('invalid_period') != 0)).limit(1).count():
        raise RuntimeError('customer SCD2 target invariants failed')


mode = os.environ['VALIDATION_PROBE_MODE']
assert mode in ('correctness', 'baseline', 'candidate')
spark = build_session('lakehouse-ops-scd2-validation-' + mode)
spark.sparkContext.setLogLevel('WARN')
report = {'status': 'running', 'mode': mode}
try:
    if mode == 'correctness':
        start, mid, end = (datetime(2026, 1, d) for d in (1, 2, 3))
        schema = 'customer_id string, valid_from timestamp, valid_to timestamp, is_current boolean'
        cases = {
            'empty': ([], True),
            'current_only': ([('C1',start,None,True)], True),
            'continuous': ([('C1',start,mid,False),('C1',mid,None,True)], True),
            'many_customers': ([('C1',start,None,True),('C2',start,None,True)], True),
            'missing_current': ([('C1',start,mid,False)], False),
            'duplicate_current': ([('C1',start,None,True),('C1',mid,None,True)], False),
            'reversed_period': ([('C1',mid,start,False),('C1',end,None,True)], False),
            'zero_period': ([('C1',start,start,False),('C1',mid,None,True)], False),
            'closed_current': ([('C1',start,mid,True)], False),
            'open_historical': ([('C1',start,None,False),('C1',mid,None,True)], False),
            'gap': ([('C1',start,mid,False),('C1',end,None,True)], False),
            'overlap': ([('C1',start,end,False),('C1',mid,None,True)], False),
            'null_current_only': ([('C1',start,None,None)], False),
            'null_customer_current': ([(None,start,None,True)], True),
        }
        report['cases'] = []
        for name, (rows, valid) in cases.items():
            frame = spark.createDataFrame(rows, schema)
            class Frozen:
                def table(self, name):
                    assert name == history.TABLE
                    return frame
            outcomes = []
            for validator in (history.validate_target, candidate):
                try:
                    validator(Frozen())
                    outcomes.append(True)
                except RuntimeError as error:
                    assert str(error) == 'customer SCD2 target invariants failed'
                    outcomes.append(False)
            assert outcomes == [valid, valid], (name, outcomes)
            report['cases'].append(dict(name=name, expected=valid, baseline=outcomes[0], candidate=outcomes[1]))
    else:
        if mode == 'candidate':
            history.validate_target = candidate
        source = spark.table('lakehouse.silver.commerce_customers').filter(
            F.col('source_batch_id') == '24e47c2864bd8fb4')
        spark.sparkContext.setJobGroup('measured-stage', 'full replay stage including validation')
        started = time.perf_counter()
        result = history.apply_customer_history(spark, source, batch_id='24e47c2864bd8fb4')
        report['stage_seconds'] = time.perf_counter() - started
        report['spark_job_count'] = len(spark.sparkContext.statusTracker().getJobIdsForGroup('measured-stage'))
        assert result['status'] == 'ready' and result['changed_rows'] == 0
        assert result['source_rows'] == 10000
        assert result['target_rows_before'] == result['target_rows_after'] == 330000
        assert result['current_rows_before'] == result['current_rows_after'] == 10000
        assert result['rows_inserted'] == 0
        report['result'] = result
    report['status'] = 'ready'
finally:
    Path(os.environ['VALIDATION_PROBE_REPORT']).write_text(json.dumps(report, indent=2) + '\n')
    spark.stop()
