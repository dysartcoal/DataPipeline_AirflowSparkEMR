"""Custom EmrAddStepsOperator which supports templated steps"""
from __future__ import division, absolute_import, print_function

from airflow.utils import apply_defaults

from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator


class MyEmrAddStepsOperator(EmrAddStepsOperator):
    template_fields = ['job_flow_id', 'steps'] # override with steps to solve the issue above

    @apply_defaults
    def __init__(
            self,
            *args, **kwargs):
        super(MyEmrAddStepsOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        stepids = super(MyEmrAddStepsOperator, self).execute(context=context)
        return stepids
