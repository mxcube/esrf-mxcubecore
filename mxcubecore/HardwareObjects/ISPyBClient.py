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


def in_greenlet(fun):
    def _in_greenlet(*args, **kwargs):
        log_msg = "lims client " + fun.__name__ + " called with: "

        for arg in args[1:]:
            try:
                log_msg += pformat(arg, indent=4, width=80) + ", "
            except Exception:
                pass

        logging.getLogger("ispyb_client").debug(log_msg)
        task = gevent.spawn(fun, *args)
        if kwargs.get("wait", False):
            task.get()

    return _in_greenlet


def utf_encode(res_d):
    for key, value in res_d.items():
        if isinstance(value, dict):
            utf_encode(value)

        try:
            # Decode bytes object or encode str object depending
            # on Python version
            res_d[key] = suds_encode("utf8", "ignore")
        except Exception:
            # If not primitive or Text data, complext type, try to convert to
            # dict or str if the first fails
            try:
                res_d[key] = utf_encode(asdict(value))
            except Exception:
                try:
                    res_d[key] = str(value)
                except Exception:
                    res_d[key] = "ISPyBClient: could not encode value"

    return res_d


def utf_decode(res_d):
    for key, value in res_d.items():
        if isinstance(value, dict):
            utf_decode(value)
        try:
            res_d[key] = value.decode("utf8", "ignore")
        except Exception:
            pass

    return res_d


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
            prop = self.get_proposal(_code, proposal_number)
        elif self.loginType == "user":
            prop = self.get_proposal_by_username(loginID)

        # Check if everything went ok
        prop_ok = True
        try:
            prop_ok = prop["status"]["code"] == "ok"
        except KeyError:
            prop_ok = False
        if not prop_ok:
            msg = "Couldn't contact the ISPyB database server: you've been logged as the local user.\nYour experiments' information will not be stored in ISPyB"
            return {
                "status": {"code": "ispybDown", "msg": msg},
                "Proposal": None,
                "session": None,
            }

        self.login_ok = True

        logging.getLogger("HWR").debug("Proposal is fine, get sessions from ISPyB...")
        logging.getLogger("HWR").debug(prop)

        proposal = prop["Proposal"]
        todays_session = self.get_todays_session(prop, create_session)

        logging.getLogger("HWR").debug(
            "LOGGED IN and todays session: " + str(todays_session)
        )

        session = todays_session["session"]
        session["is_inhouse"] = todays_session["is_inhouse"]
        todays_session_id = session.get("sessionId", None)
        local_contact = (
            self.get_session_local_contact(todays_session_id)
            if todays_session_id
            else {}
        )

        return {
            "status": {"code": "ok", "msg": msg},
            "Proposal": proposal,
            "Session": todays_session,
            "local_contact": local_contact,
            "Person": prop["Person"],
            "Laboratory": prop["Laboratory"],
        }


    @trace
    def get_proposal(self, code, number) -> ProposalTuple:
        """
        Returns the tuple (Proposal, Person, Laboratory, Session, Status).
        Containing the data from the coresponding tables in the database
        the status of the database operations are returned in Status.

        :param code: The proposal code
        :type code: str
        :param number: The proposal number
        :type number: int

        :returns: The dict (Proposal, Person, Laboratory, Sessions, Status).
        :rtype: dict
        """
        logging.getLogger("HWR").debug(
            "ISPyB. Obtaining proposal for code=%s / prop_number=%s"
            % (code, number)
        )

        # Default values
        person = Person()
        proposal = Proposal()
        sessions = []
        error = ProposalTuple(
                    person=person,
                    proposal=proposal,
                    sessions=sessions,
                    status=Status(code="error")
                )

        if self.adapter.isEnabled():
            try:
                try:
                    person = self.adapter.findPersonByProposal(code, number)
                    logging.getLogger("HWR").debug("ISPyB. person is=%s" % (person))
                except WebFault as e:
                    logging.getLogger("ispyb_client").exception(str(e))

                try:
                    proposal = self.adapter.findProposal(code, number)
                except WebFault as e:
                    logging.getLogger("ispyb_client").exception(str(e))
                    return ProposalTuple()

                try:
                    sessions = (
                        self.adapter.findSessionsByProposalAndBeamLine(code, number, self.beamline_name)
                    )
                except WebFault as e:
                    logging.getLogger("ispyb_client").exception(str(e))
            except URLError:
                logging.getLogger("ispyb_client").exception(_CONNECTION_ERROR_MSG)
                return error

            return ProposalTuple(
                person=person,
                proposal=proposal,
                sessions=sessions,
                status=Status(code="ok")
            )
        else:
            logging.getLogger("ispyb_client").exception(
                "Error in get_proposal: Could not connect to server,"
                + " returning empty proposal"
            )
            return error

# Bindings to methods called from older bricks.
