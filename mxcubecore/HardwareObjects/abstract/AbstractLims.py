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
from mxcubecore.BaseHardwareObjects import HardwareObject
from mxcubecore.model.lims_session import LIMSSession


__credits__ = ["MXCuBE collaboration"]


class AbstractLims(HardwareObject):
    __metaclass__ = abc.ABCMeta

    def __init__(self, name):
        super().__init__(name)

        # current lims session
        self.session = LIMSSession()

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
        print(self.session)
        return self.session
