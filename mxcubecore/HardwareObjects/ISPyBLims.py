from __future__ import print_function
import itertools
import uuid
from mxcubecore.HardwareObjects.ISPyBAbstractLims import ISPyBAbstractLIMS
from mxcubecore.model.lims_session import ProposalTuple

"""
A client for ISPyB Webservices.
"""

import logging


class ISPyBLims(ISPyBAbstractLIMS):
    """
    ISPyB proposal-based client
    """

    def __init__(self, name):
        super().__init__(name)

    def is_user_login_type(self):
        return False

    def get_proposals_by_user(self, login_id: str):
        raise Exception("Not implemented")

    def get_lims_name(self):
        return "ISPyB"

    def get_user_name(self):
        """
        Because it is a proposal based it returns the proposal plus the uuid4
        """
        return f"{self.user_name}-{str(uuid.uuid4())}"

    def _authenticate(self, user_name, psd, ldap_connection):
        """
        Authentication step based on the authServerType
        """

        if self.authServerType == "ldap":
            logging.getLogger("HWR").debug(
                "Starting LDAP authentication %s" % user_name
            )
            ok = self.ldap_login(user_name, psd, ldap_connection)
            logging.getLogger("HWR").debug("User %s logged in LDAP" % user_name)
        elif self.authServerType == "ispyb":
            logging.getLogger("HWR").debug("ISPyB login")
            ok, msg = self._ispybLogin(user_name, psd)
        else:
            raise Exception("Authentication server type is not defined")

        if not ok:
            msg = "%s." % msg.capitalize()
            # refuse Login
            logging.getLogger("HWR").error("ISPyB login not ok")
            raise Exception("Authentication failed")

    def login(self, user_name, password, ldap_connection=None) -> ProposalTuple:

        # For porposal login, split the loginID to code and numbers
        proposal_code = "".join(
            itertools.takewhile(lambda c: not c.isdigit(), user_name)
        )
        proposal_number = user_name[len(proposal_code) :]

        # if translation of the loginID is needed, need to be tested by ESRF
        if self.loginTranslate is True:
            user_name = self._translate(proposal_code, "ldap") + str(proposal_number)

        # Authentication
        try:
            self._authenticate(user_name, password, ldap_connection)
            self.login_ok = True
            self.user_name = user_name
        except Exception:
            return ProposalTuple()

        # login succeed, get proposal and sessions
        # get the proposal ID
        _code = self._translate(proposal_code, "ispyb")
        return self.adapter.get_proposal_tuple_by_code_and_number(
            _code, proposal_number, self.beamline_name
        )
