import errno
import json

from typing_extensions import Literal, Union
from pydantic import BaseModel, Field
from devtools import debug
import os

from mxcubecore import HardwareRepository as HWR
from mxcubecore.queue_entry.base_queue_entry import BaseQueueEntry

from mxcubecore.model.common import (
    PathParameters,
    CommonCollectionParamters,
    StandardCollectionParameters,
    ISPYBCollectionParameters,
    LegacyParameters,
)


DEFAULT_MAX_FREQ = 925


class MXPathParameters(PathParameters):
    use_experiment_name: bool = Field(
        False, description="Whether to use the experiment name in the data path"
    )


class BaseUserCollectionParameters(BaseModel):
    exp_time: float = Field(95e-6, gt=0, lt=1, description="s")


class MXBaseQueueTaskParameters(BaseModel):
    path_parameters: MXPathParameters
    common_parameters: CommonCollectionParamters
    collection_parameters: StandardCollectionParameters
    legacy_parameters: LegacyParameters
    lims_parameters: Union[ISPYBCollectionParameters, None]

    def update_dependent_fields(field_data):
        return {}

    @staticmethod
    def ui_schema():
        return json.dumps(
            {
                "ui:order": [
                    "num_images",
                    "exp_time",
                    "osc_range",
                    "osc_start",
                    "resolution",
                    "transmission",
                    "energy",
                    "*",
                ],
                "ui:submitButtonOptions": {
                    "norender": "true",
                },
            }
        )


class MXBaseQueueEntry(BaseQueueEntry):
    """
    Defines common MX collection methods.
    """

    def __init__(self, view, data_model):
        super().__init__(view=view, data_model=data_model)
        self._beamline_values = None

    def get_data_directory(self):
        return self.get_data_model().get_path_template().directory

    def get_data_path(self):
        return self.get_data_model().get_path_template().get_image_path()

    def create_directory(self):
        try:
            os.makedirs(self.get_data_directory())
        except os.error as e:
            if e.errno != errno.EEXIST:
                raise

    def start_processing(self, exp_type):
        data_root_path = self.get_data_path()

    def prepare_acquisition(self):
        pass

    def execute(self):
        super().execute()
        debug(self._data_model._task_data)

    def pre_execute(self):
        super().pre_execute()
        self.create_directory()

        self.emit_progress(0)

    def post_execute(self):
        super().post_execute()
        self.emit_progress(1)

    def emit_progress(self, progress):
        HWR.beamline.collect.emit_progress(progress)

    def stop(self):
        super().stop()
        HWR.beamline.detector.stop_acquisition()
