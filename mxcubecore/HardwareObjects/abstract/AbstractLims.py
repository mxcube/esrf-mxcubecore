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
from typing import Union
from mxcubecore.BaseHardwareObjects import HardwareObject
from mxcubecore.model.lims_session import LIMSSession, ProposalTuple, Session
import time
from mxcubecore import HardwareRepository as HWR

__credits__ = ["MXCuBE collaboration"]


class AbstractLims(HardwareObject, abc.ABC):
    __metaclass__ = abc.ABCMeta

    def __init__(self, name):
        super().__init__(name)

        # current lims session
        self.session = LIMSSession()

        self.beamline_name = "unknown"

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
    def test(self):
        pass

    @abc.abstractmethod
    def dc_link(self, id: str) -> str:
        raise Exception("Abstract class. Not implemented")

    @abc.abstractmethod
    def get_dc(self, id: str) -> dict:
        raise Exception("Abstract class. Not implemented")

    @abc.abstractmethod
    def get_dc_thumbnail(self, id: str):
        raise Exception("Abstract class. Not implemented")

    @abc.abstractmethod
    def get_dc_image(self, id: str):
        raise Exception("Abstract class. Not implemented")

    @abc.abstractmethod
    def get_quality_indicator_plot(self, id: str):
        raise Exception("Abstract class. Not implemented")

    @abc.abstractmethod
    def get_samples(self, proposal_id: str):
        raise Exception("Abstract class. Not implemented")

    @abc.abstractmethod
    def get_rest_token(self, proposal_id: str):
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

    def set_lims_session(self, session: LIMSSession):
        """
        Sets the curent lims session
        :param session: lims session value
        :return:
        """
        self.session = session

    def get_lims_session(self) -> LIMSSession:
        """
        Getter for the curent lims session
        :return: current lims session
        """
        return self.session
