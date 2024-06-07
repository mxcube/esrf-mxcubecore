from __future__ import print_function
import datetime
from typing import List

try:
    from urlparse import urljoin
    from urllib2 import URLError
except Exception:
    # Python3
    from urllib.parse import urljoin
    from urllib.error import URLError


from mxcubecore.model.lims_session import (
    Person,
    Proposal,
    ProposalTuple,
    Session,
    Status,
)
from suds.sudsobject import asdict
from suds import WebFault
from suds.client import Client
import logging


def utf_decode(res_d):
    for key, value in res_d.items():
        if isinstance(value, dict):
            utf_decode(value)
        try:
            res_d[key] = value.decode("utf8", "ignore")
        except Exception:
            pass

    return res_d


class ISPyBDataAdapter:

    def __init__(self, ws_root: str, proxy: dict, ws_username: str, ws_password: str):
        self.ws_root = ws_root
        self.ws_username = ws_username
        self.ws_password = ws_password
        self.proxy = proxy  # type: ignore

        self.logger = logging.getLogger("ispyb_adapter")

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

    def create_session(self, session: Session):
        """
        Create a new session for "current proposal", the attribute
        porposalId in <session_dict> has to be set (and exist in ISPyB).

        :param session_dict: Dictonary with session parameters.
        :type session_dict: dict

        :returns: The session id of the created session.
        :rtype: int
        """
        if self._collection:

            try:
                # The old API used date formated strings and the new
                # one uses DateTime objects.
                # session_dict["startDate"] = datetime.strptime(
                #    session_dict["startDate"], "%Y-%m-%d %H:%M:%S"
                # )
                # session_dict["endDate"] = datetime.strptime(
                #    session_dict["endDate"], "%Y-%m-%d %H:%M:%S"
                # )

                # try:
                #    session_dict["lastUpdate"] = datetime.strptime(
                #        session_dict["lastUpdate"].split("+")[0], "%Y-%m-%d %H:%M:%S"
                #    )
                #    session_dict["timeStamp"] = datetime.strptime(
                #        session_dict["timeStamp"].split("+")[0], "%Y-%m-%d %H:%M:%S"
                #    )
                # except Exception:
                #    pass

                print("Creating session--------")
                print(session)
                # return data to original codification
                # decoded_dict = utf_decode(session_dict)
                # session = self._collection.service.storeOrUpdateSession(decoded_dict)

                # changing back to string representation of the dates,
                # since the session_dict is used after this method is called,
                session_dict["startDate"] = datetime.strftime(
                    session_dict["startDate"], "%Y-%m-%d %H:%M:%S"
                )
                session_dict["endDate"] = datetime.strftime(
                    session_dict["endDate"], "%Y-%m-%d %H:%M:%S"
                )

            except WebFault as e:
                session = {}
                logging.getLogger("ispyb_client").exception(str(e))
            except URLError:
                logging.getLogger("ispyb_client").exception(_CONNECTION_ERROR_MSG)

            logging.getLogger("ispyb_client").info(
                "[ISPYB] Session goona be created: session_dict %s" % session_dict
            )
            logging.getLogger("ispyb_client").info(
                "[ISPYB] Session created: %s" % session
            )
            return session
        else:
            logging.getLogger("ispyb_client").exception(
                "Error in create_session: could not connect to server"
            )

    def __to_session(self, session: dict[str, str]) -> Session:
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

    def __to_proposal(self, proposal: dict[str, str]) -> Proposal:
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

    def __to_person(self, person: dict[str, str]) -> Person:  # type: ignore
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

    def _get_log(self):
        return self.logger

    def _info(self, msg: str):
        return self._get_log().info(msg)

    def find_person_by_proposal(self, code: str, number: str) -> Person:
        try:
            self._info("find_person_by_proposal. code=%s number=%s" % (code, number))
            response = self._shipping.service.findPersonByProposal(code, number)  # type: ignore
            return self.__to_person(asdict(response))  # type: ignore
        except Exception as e:
            self._get_log().exception(str(e))
            raise e

    def find_person_by_login(self, username: str, beamline_name: str) -> Person:
        try:
            self._info(
                "find_person_by_login. username=%s beamline_name=%s"
                % (username, beamline_name)
            )
            response = self._shipping.service.findPersonByLogin(username, beamline_name)  # type: ignore
            return self.__to_person(asdict(response))  # type: ignore
        except Exception as e:
            self._get_log().exception(str(e))
            raise e

    def find_proposal(self, code: str, number: str) -> Proposal:
        try:
            self._info("find_proposal. code=%s number=%s" % (code, number))
            response = self._shipping.service.findProposal(code, number)  # type: ignore
            return self.__to_proposal(asdict(response))  # type: ignore
        except Exception as e:
            self._get_log().exception(str(e))
            raise e

    def find_proposal_by_login_and_beamline(
        self, username: str, beamline_name: str
    ) -> Proposal:
        try:
            self._info(
                "find_proposal_by_login_and_beamline. username=%s beamline_name=%s"
                % (username, beamline_name)
            )
            response = self._shipping.service.findProposalByLoginAndBeamline(username, beamline_name)  # type: ignore
            return self.__to_proposal(asdict(response))  # type: ignore
        except Exception as e:
            self._get_log().exception(str(e))
            raise e

    def find_sessions_by_proposal_and_beamLine(
        self, code: str, number: str, beamline: str
    ) -> List[Session]:
        try:
            self._info(
                "find_sessions_by_proposal_and_beamLine. code=%s number=%s beamline=%s"
                % (code, number, beamline)
            )
            responses = self._collection.service.findSessionsByProposalAndBeamLine(
                code.upper(), number, beamline
            )
            sessions: List[Session] = []
            for response in responses:
                sessions.append(self.__to_session(asdict(response)))
            return sessions
        except Exception as e:
            self._get_log().exception(str(e))
            raise e

    def _get_todays_session(self, sessions: List[Session]) -> Session | None:
        try:
            for session in sessions:
                now = datetime.datetime.now()
                if session.startDate.date() <= now.date() <= session.endDate.date():
                    return session
            return None
        except Exception as e:
            self._get_log().exception(str(e))
            return None

    def get_proposal_tuple_by_code_and_number(
        self, code: str, number: str, beamline_name: str
    ) -> ProposalTuple:
        try:
            self._info(
                "get_proposal_tuple_by_code_and_number. code=%s number=%s beamline_name=%s"
                % (code, number, beamline_name)
            )

            person = self.find_person_by_proposal(code, number)  # type: ignore
            proposal = self.find_proposal(code, number)  # type: ignore
            sessions = self.find_sessions_by_proposal_and_beamLine(
                code, number, beamline_name
            )

            return ProposalTuple(
                person=person,
                proposal=proposal,
                sessions=sessions,
                status=Status(code="ok"),
                todays_session=self._get_todays_session(sessions),
            )
        except WebFault as e:
            self._get_log().exception(str(e))
        return ProposalTuple(
            status=Status(code="error"),
        )

    def get_proposal_tuple_by_username(
        self, username: str, beamline_name: str
    ) -> ProposalTuple:
        try:
            self._info(
                "get_proposal_tuple_by_username. username=%s beamline_name=%s"
                % (username, beamline_name)
            )
            person = self.find_person_by_login(username, beamline_name)  # type: ignore
            proposal = self.find_proposal_by_login_and_beamline(username, beamline_name)  # type: ignore
            sessions = self.find_sessions_by_proposal_and_beamLine(
                proposal.code, proposal.number, beamline_name
            )
            return ProposalTuple(
                person=person,
                proposal=proposal,
                sessions=sessions,
                status=Status(code="ok"),
                todays_session=self._get_todays_session(sessions),
            )
        except WebFault as e:
            self._get_log().exception(str(e))
        return ProposalTuple()
