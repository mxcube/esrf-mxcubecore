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


__credits__ = ["MXCuBE collaboration"]


class AbstractLims(HardwareObject):
    __metaclass__ = abc.ABCMeta

    def __init__(self, name):
        super().__init__(name)

    @abc.abstractmethod
    def set_horizontal_gap(self, value, timeout=None):
        """
        Sets vertical gap in microns
        :param value: target value
        :param timeout: timeout is sec. If None do not wait
        :return:
        """
        pass
