# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from typing import List, Union, Type, Dict
from abc import ABC, abstractmethod

from airflow import LoggingMixin
from airflow.models import BaseOperator

from marquez.dataset import Dataset
from openlineage.facet import BaseFacet


class StepMetadata:
    def __init__(
            self,
            name,
            location=None,
            inputs: List[Dataset] = None,
            outputs: List[Dataset] = None,
            context=None,
            run_facets: Dict[str, BaseFacet] = None
    ):
        # TODO: Define a common way across extractors to build the
        # job name for an operator
        self.name = name
        self.location = location
        self.inputs = inputs
        self.outputs = outputs
        self.context = context
        self.run_facets = run_facets

        if not inputs:
            self.inputs = []
        if not outputs:
            self.outputs = []
        if not context:
            self.context = {}
        if not run_facets:
            self.run_facets = {}

    def __repr__(self):
        return "name: {}\t inputs: {} \t outputs: {}".format(
            self.name,
            ','.join([str(i) for i in self.inputs]),
            ','.join([str(o) for o in self.outputs]))


class BaseExtractor(ABC, LoggingMixin):
    operator_class: Type[BaseOperator] = None
    operator: operator_class = None

    def __init__(self, operator):
        self.operator = operator

    @classmethod
    def get_operator_class(cls):
        return cls.operator_class

    def validate(self):
        # TODO: maybe we should also enforce the module
        assert (self.operator_class is not None and
                self.operator.__class__ == self.operator_class)

    @abstractmethod
    def extract(self) -> Union[StepMetadata, List[StepMetadata]]:
        # In future releases, we'll want to deprecate returning a list of StepMetadata
        # and simply return a StepMetadata object. We currently return a list
        # for backwards compatibility.
        pass

    def extract_on_complete(self, task_instance) -> Union[StepMetadata, List[StepMetadata]]:
        # TODO: This method allows for the partial updating of task
        # metadata on completion. Marquez currently doesn't support
        # partial updates within the context of a DAG run, but this feature
        # will soon be supported:
        # https://github.com/MarquezProject/marquez/issues/816
        #
        # Also, we'll want to revisit the metadata extraction flow,
        # but for now, return an empty set as the default behavior
        # as not all extractors need to handle partial metadata updates.
        return self.extract()
