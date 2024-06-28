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
from typing import List, Union
from mxcubecore.BaseHardwareObjects import HardwareObject
from mxcubecore.model.lims_session import ProposalTuple, Session
import time
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

    @abc.abstractmethod
    def get_lims_name(self, login_id, password, create_session):
        raise Exception("Abstract class. Not implemented")

    @abc.abstractmethod
    def get_user_name(self, login_id, password, create_session):
        raise Exception("Abstract class. Not implemented")

    @abc.abstractmethod
    def login(self, login_id, password, create_session):
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
    def create_session(self, proposal_tuple: ProposalTuple) -> ProposalTuple:
        raise Exception("Abstract class. Not implemented")

    @abc.abstractmethod
    def get_samples(self):
        raise Exception("Abstract class. Not implemented")

    @abc.abstractmethod
    def store_robot_action(self, proposal_id: str):
        raise Exception("Abstract class. Not implemented")

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

    def is_session_today(self, session: Session) -> Union[Session, None]:
        """
        Given a session it returns the session if it is scheduled for today in the beamline
        otherwise it returns None
        """
        beamline = session.get("beamlineName")  # session["beamlineName"]
        start_date = "%s 00:00:00" % session.startDate.split()[0]
        end_date = "%s 23:59:59" % session.endDate.split()[0]
        try:
            start_struct = time.strptime(start_date, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            raise Exception("Abstract class. Not implemented")
        else:
            try:
                end_struct = time.strptime(end_date, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                raise Exception("Abstract class. Not implemented")
            else:
                start_time = time.mktime(start_struct)
                end_time = time.mktime(end_struct)
                current_time = time.time()
                # Check beamline name
                if beamline == self.beamline_name:
                    # Check date
                    if current_time >= start_time and current_time <= end_time:
                        return session
        return None

    def set_sessions(self, sessions: List[Session]):
        """
        Sets the curent lims session
        :param session: lims session value
        :return:
        """
        logging.getLogger("HWR").debug(
            "%s sessions avaliable for user %s" % (len(sessions), self.get_user_name())
        )
        self.sessions = sessions

    def get_active_session(self) -> Session:
        return self.active_session

    def set_active_session_by_id(self, session_id: str) -> Session:
        if len(self.sessions) == 0:
            logging.getLogger("HWR").error(
                "Session list is empty. No session candidates"
            )
            raise BaseException("No sessions available")

        if len(self.sessions) == 1:
            self.active_session = self.sessions[0]
            logging.getLogger("HWR").debug(
                "Session list contains a single session. sesssion=%s",
                self.active_session,
            )
            return self.active_session

        session_list = [obj for obj in self.sessions if obj.session_id == session_id]
        if len(session_list) != 1:
            raise BaseException(
                "Session not found in the local list of sessions. session_id="
                + session_id
            )
        self.active_session = session_list[0]
        logging.getLogger("HWR").debug(
            "Active session selected. session_id=%s proposal_code=%s proposal_number=%s"
            % (session_id, self.active_session.code, self.active_session.number)
        )
        return self.active_session
