#
#  Project: MXCuBE
#  https://github.com/mxcube
#
#  This file is part of MXCuBE software.
#
#  MXCuBE is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  MXCuBE is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with MXCuBE. If not, see <http://www.gnu.org/licenses/>.

"""
"""
import abc
from datetime import datetime
from typing import List
from mxcubecore.BaseHardwareObjects import HardwareObject
from mxcubecore.model.lims_session import Lims, LimsSessionManager, Session
from mxcubecore import HardwareRepository as HWR
import logging

__credits__ = ["MXCuBE collaboration"]


class AbstractLims(HardwareObject, abc.ABC):
    __metaclass__ = abc.ABCMeta

    def __init__(self, name):
        super().__init__(name)

        # current lims session
        self.active_session = None

        self.beamline_name = "unknown"

        self.sessions = []

        self.session_manager = LimsSessionManager()

    def is_session_already_active(self, session_id: str) -> bool:
        # If curent selected session is already selected no need to do
        # anything else
        active_session = self.session_manager.active_session
        if active_session is not None:
            if active_session.session_id == session_id:
                return True
        return False

    @abc.abstractmethod
    def get_lims_name(self) -> List[Lims]:
        raise Exception("Abstract class. Not implemented")

    def get_session_id(self) -> str:
        return self.session_manager.active_session.session_id

    @abc.abstractmethod
    def get_user_name(self):
        raise Exception("Abstract class. Not implemented")

    @abc.abstractmethod
    def get_full_user_name(self):
        raise Exception("Abstract class. Not implemented")

    @abc.abstractmethod
    def login(
        self, login_id: str, password: str, create_session: bool
    ) -> List[Session]:
        raise Exception("Abstract class. Not implemented")

    @abc.abstractmethod
    def is_user_login_type(self) -> bool:
        raise Exception("Abstract class. Not implemented")

    @abc.abstractmethod
    def echo(self) -> bool:
        raise Exception("Abstract class. Not implemented")

    @abc.abstractmethod
    def init(self) -> None:
        self.beamline_name = HWR.beamline.session.beamline_name

    @abc.abstractmethod
    def get_proposals_by_user(self, login_id: str):
        raise Exception("Abstract class. Not implemented")

    @abc.abstractmethod
    def create_session(self, proposal_tuple: LimsSessionManager) -> LimsSessionManager:
        raise Exception("Abstract class. Not implemented")

    @abc.abstractmethod
    def get_samples(self):
        raise Exception("Abstract class. Not implemented")

    @abc.abstractmethod
    def store_robot_action(self, proposal_id: str):
        raise Exception("Abstract class. Not implemented")

    @abc.abstractmethod
    def update_bl_sample(self, bl_sample: str):
        """
        Creates or stos a BLSample entry.
        # NBNB update doc string
        :param sample_dict: A dictonary with the properties for the entry.
        :type sample_dict: dict
        """
        raise Exception("Abstract class. Not implemented")

    def get_dc_link(self):
        pass

    def is_scheduled_on_host_beamline(self, beamline: str) -> bool:
        return beamline.strip().upper() == self.override_beamline_name.strip().upper()

    def is_scheduled_now(self, startDate, endDate) -> bool:
        return self.is_time_between(startDate, endDate)

    def is_time_between(self, start_date: str, end_date: str, check_time=None):
        if start_date is None or end_date is None:
            return False

        begin_time = datetime.fromisoformat(start_date).date()
        end_time = datetime.fromisoformat(end_date).date()

        # If check time is not given, default to current UTC time
        check_time = check_time or datetime.utcnow().date()
        if begin_time <= check_time <= end_time:
            return True
        else:
            return False

    def set_sessions(self, sessions: List[Session]):
        """
        Sets the curent lims session
        :param session: lims session value
        :return:
        """
        logging.getLogger("HWR").debug(
            "%s sessions avaliable for user %s" % (len(sessions), self.get_user_name())
        )
        self.session_manager.sessions = sessions

    def get_active_session(self) -> Session:
        return self.session_manager.active_session

    def set_active_session_by_id(self, session_id: str) -> Session:
        raise Exception("Abstract class. Not implemented")
