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
from typing import Union
from mxcubecore.BaseHardwareObjects import HardwareObject
from mxcubecore.model.lims_session import LIMSSession, Session
from datetime import datetime, timedelta
import time



__credits__ = ["MXCuBE collaboration"]


class AbstractLims(HardwareObject):
    __metaclass__ = abc.ABCMeta

    def __init__(self, name):
        super().__init__(name)

        # current lims session
        self.session = LIMSSession()

    @abc.abstractmethod
    def is_scheduled_on_host_beamline(self, beamline) -> bool:
        return beamline.strip().upper() == self.override_beamline_name.strip().upper()

    @abc.abstractmethod
    def is_scheduled_now(self, startDate, endDate) -> bool:
        return self.is_time_between(startDate, endDate)

    @abc.abstractmethod
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

    @abc.abstractmethod
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
            pass
        else:
            try:
                end_struct = time.strptime(end_date, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                pass
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

    @abc.abstractmethod
    def set_lims_session(self, session: LIMSSession):
        """
        Sets the curent lims session
        :param session: lims session value
        :return:
        """
        self.session = session

    @abc.abstractmethod
    def get_lims_session(self) -> LIMSSession:
        """
        Getter for the curent lims session
        :return: current lims session
        """
        return self.session
