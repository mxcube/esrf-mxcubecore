from __future__ import print_function
import sys
import json
import time
import itertools
import os
import traceback
import warnings
from pprint import pformat
from collections import namedtuple
from datetime import datetime

from mxcubecore.HardwareObjects.abstract.ISPyBAbstractLims import ISPyBAbstractLIMS
from mxcubecore.model.lims_session import Person, Proposal, ProposalTuple, Status

try:
    from urlparse import urljoin
    from urllib2 import URLError
except Exception:
    # Python3
    from urllib.parse import urljoin
    from urllib.error import URLError

from mxcubecore.HardwareObjects.abstract import AbstractLims
from suds.sudsobject import asdict
from suds import WebFault
from suds.client import Client
from mxcubecore.BaseHardwareObjects import HardwareObject
from mxcubecore.utils.conversion import string_types
from mxcubecore import HardwareRepository as HWR

"""
A client for ISPyB Webservices.
"""

import logging
import gevent


suds_encode = str.encode

if sys.version_info > (3, 0):
    suds_encode = bytes.decode

logging.getLogger("suds").setLevel(logging.INFO)


LOGIN_TYPE_FALLBACK = "proposal"


# Production web-services:    http://160.103.210.1:8080/ispyb-ejb3/ispybWS/
# Test web-services:          http://160.103.210.4:8080/ispyb-ejb3/ispybWS/

# The WSDL root is configured in the hardware object XML file.
# _WS_USERNAME, _WS_PASSWORD have to be configured in the HardwareObject XML file.
_WSDL_ROOT = ""
_WS_BL_SAMPLE_URL = _WSDL_ROOT + "ToolsForBLSampleWebService?wsdl"
_WS_SHIPPING_URL = _WSDL_ROOT + "ToolsForShippingWebService?wsdl"
_WS_COLLECTION_URL = _WSDL_ROOT + "ToolsForCollectionWebService?wsdl"
_WS_AUTOPROC_URL = _WSDL_ROOT + "ToolsForAutoprocessingWebService?wsdl"
_WS_USERNAME = None
_WS_PASSWORD = None

_CONNECTION_ERROR_MSG = (
    "Could not connect to ISPyB, please verify that "
    + "the server is running and that your "
    + "configuration is correct"
)


SampleReference = namedtuple(
    "SampleReference",
    ["code", "container_reference", "sample_reference", "container_code"],
)


def trace(fun):
    def _trace(*args):
        log_msg = "lims client " + fun.__name__ + " called with: "

        for arg in args[1:]:
            try:
                log_msg += pformat(arg, indent=4, width=80) + ", "
            except Exception:
                pass

        logging.getLogger("ispyb_client").debug(log_msg)
        result = fun(*args)

        try:
            result_msg = (
                "lims client "
                + fun.__name__
                + " returned  with: "
                + pformat(result, indent=4, width=80)
            )
        except Exception:
            pass

        logging.getLogger("ispyb_client").debug(result_msg)
        return result

    return _trace
class ISPyBClient(ISPyBAbstractLIMS):
    """
    Web-service client for ISPyB.
    """

    def __init__(self, name):
        super().__init__(name)

    def login(self, loginID, psd, ldap_connection=None, create_session=True) -> ProposalTuple:
        login_name = loginID
        proposal_code = ""
        proposal_number = ""

        self.login_ok = False

        # For porposal login, split the loginID to code and numbers
        if self.loginType == "proposal":
            proposal_code = "".join(
                itertools.takewhile(lambda c: not c.isdigit(), loginID)
            )
            proposal_number = loginID[len(proposal_code) :]

        # if translation of the loginID is needed, need to be tested by ESRF
        if self.loginTranslate is True:
            login_name = self.translate(proposal_code, "ldap") + str(proposal_number)

        # Authentication
        if self.authServerType == "ldap":
            logging.getLogger("HWR").debug("LDAP login")
            ok = self.ldap_login(login_name, psd, ldap_connection)
            msg = loginID
            logging.getLogger("HWR").debug("searching for user %s" % login_name)
        elif self.authServerType == "ispyb":
            logging.getLogger("HWR").debug("ISPyB login")
            ok, msg = self._ispybLogin(login_name, psd)
        else:
            raise Exception("Authentication server type is not defined")

        if not ok:
            msg = "%s." % msg.capitalize()
            # refuse Login
            return {
                "status": {"code": "error", "msg": msg},
                "Proposal": None,
                "session": None,
            }

        # login succeed, get proposal and sessions
        if self.loginType == "proposal":
            # get the proposal ID
            _code = self.translate(proposal_code, "ispyb")
            proposalTuple = self.get_proposal(_code, proposal_number)
        elif self.loginType == "user":
            proposalTuple = self.adapter.get_proposal_tuple_by_username(loginID,  self.beamline_name) # get_proposal_by_username(loginID)

        # Check if everything went ok
        return proposalTuple

    @trace
    def get_proposal(self, code: str, number: str) -> ProposalTuple:
        logging.getLogger("HWR").debug(
            "get_proposal. code=%s number=%s beamline=%s"
            % (code, number, self.beamline_name)
        )
        return self.adapter.get_proposal_tuple_by_code_and_number(code, number, self.beamline_name) # type: ignore

# Bindings to methods called from older bricks.
