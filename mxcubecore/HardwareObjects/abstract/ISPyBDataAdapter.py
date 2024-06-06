from __future__ import print_function
from typing import List

try:
    from urlparse import urljoin
    from urllib2 import URLError
except Exception:
    # Python3
    from urllib.parse import urljoin
    from urllib.error import URLError


from mxcubecore.model.lims_session import Person, Proposal, Session
from suds.sudsobject import asdict
from suds import WebFault
from suds.client import Client
import logging


class ISPyBDataAdapter:

    def __init__(self, ws_root: str, proxy: dict, ws_username: str, ws_password: str):
        self.ws_root = ws_root
        self.ws_username = ws_username
        self.ws_password = ws_password
        self.proxy = proxy  # type: ignore

        _WS_SHIPPING_URL = ws_root + "ToolsForShippingWebService?wsdl"
        _WS_COLLECTION_URL = ws_root + "ToolsForCollectionWebService?wsdl"

        self._shipping = self.__create_client(_WS_SHIPPING_URL)
        self._collection = self.__create_client(_WS_COLLECTION_URL)

    def __create_client(self, url: str):
        """
        Given a url it will create
        """
        if self.ws_root.strip().startswith("https://"):
            from suds.transport.https import HttpAuthenticated
        else:
            from suds.transport.http import HttpAuthenticated

        client = Client(
            url,
            timeout=3,
            transport=HttpAuthenticated(
                username=self.ws_username,  # type: ignore
                password=self.ws_password,
                proxy=self.proxy,
            ),
            cache=None,
            proxy=self.proxy,
        )
        client.set_options(cache=None, location=url)
        return client

    def isEnabled(self) -> object:
        return self._shipping  # type: ignore

    def __to_session(self, session: dict) -> Session:
        """
        Converts a dictionary composed by the person entries to the object proposal
        """
        return Session(
            sessionId=session.get("sessionId"),
            proposalId=session.get("proposalId"),
            beamlineName=session.get("beamlineName"),
            comments=session.get("comments"),
            endDate=session.get("endDate"),
            nbShifts=session.get("nbShifts"),
            scheduled=session.get("scheduled"),
            startDate=session.get("startDate"),
        )

    def __to_proposal(self, proposal: dict) -> Proposal:
        """
        Converts a dictionary composed by the person entries to the object proposal
        """
        return Proposal(
            code=proposal.get("code"),
            number=proposal.get("number"),
            proposalId=proposal.get("proposalId"),
            title=proposal.get("title"),
            type=proposal.get("type"),
        )

    def __to_person(self, person: dict) -> Person:  # type: ignore
        """
        Converts a dictionary composed by the person entries to the object Person
        """
        return Person(
            emailAddress=person.get("emailAddress"),
            familyName=person.get("familyName"),
            faxNumber=person.get("faxNumber"),
            givenName=person.get("givenName"),
            login=person.get("login"),
            personId=person.get("personId"),
            phoneNumber=person.get("phoneNumber"),
            siteId=person.get("siteId"),
            title=person.get("title"),
            laboratoryId=person.get("laboratoryId"),
        )

    def findPersonByProposal(self, code: str, number: str) -> Person:
        try:
            response = self._shipping.service.findPersonByProposal(code, number)  # type: ignore
            return self.__to_person(asdict(response))  # type: ignore
        except Exception as e:
            logging.getLogger("ispyb_adapter").exception(str(e))
            raise e

    def findProposal(self, code: str, number: str) -> Proposal:
        try:
            response = self._shipping.service.findProposal(code, number)  # type: ignore
            return self.__to_proposal(asdict(response))  # type: ignore
        except Exception as e:
            logging.getLogger("ispyb_adapter").exception(str(e))
            raise e

    def findSessionsByProposalAndBeamLine(
        self, code: str, number: str, beamline: str
    ) -> List[Session]:
        try:
            responses = self._collection.service.findSessionsByProposalAndBeamLine(code, number, beamline)  # type: ignore
            sessions: List[Session] = []
            for response in responses:
                print(response)
                sessions.append(self.__to_session(asdict(response)))
            return sessions
        except Exception as e:
            print(e)
            logging.getLogger("ispyb_adapter").exception(str(e))
            raise e
