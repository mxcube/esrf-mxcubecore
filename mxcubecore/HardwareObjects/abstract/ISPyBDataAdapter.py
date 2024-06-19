from __future__ import print_function
import time
from datetime import datetime, timedelta
from typing import List
from typing import Union
from mxcubecore.HardwareObjects.abstract.ISPyBValueFactory import (
    ISPyBValueFactory,
)
from mxcubecore.utils.conversion import string_types

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
import sys

suds_encode = str.encode

if sys.version_info > (3, 0):
    suds_encode = bytes.decode


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


class ISPyBDataAdapter:

    def __init__(self, ws_root: str, proxy: dict, ws_username: str, ws_password: str):
        self.ws_root = ws_root
        self.ws_username = ws_username
        self.ws_password = ws_password
        self.proxy = proxy  # type: ignore

        self.logger = logging.getLogger("ispyb_adapter")

        self._shipping = self.__create_client(
            self.ws_root + "ToolsForShippingWebService?wsdl"
        )
        self._collection = self.__create_client(
            self.ws_root + "ToolsForCollectionWebService?wsdl"
        )
        self._tools_ws = self.__create_client(
            self.ws_root + "ToolsForBLSampleWebService?wsdl"
        )

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

    def create_session(
        self, proposal_tuple: ProposalTuple, beamline_name: str
    ) -> ProposalTuple:

        try:
            current_time = time.localtime()
            start_time = time.strftime("%Y-%m-%d 00:00:00", current_time)
            end_time = time.mktime(current_time) + 60 * 60 * 24
            tomorrow = time.localtime(end_time)
            end_time = time.strftime("%Y-%m-%d 07:59:59", tomorrow)

            session = {}
            session["proposalId"] = proposal_tuple.proposal.proposal_id
            session["beamlineName"] = beamline_name
            session["scheduled"] = 0
            session["nbShifts"] = 3
            session["comments"] = "Session created by the BCM"
            current_time = datetime.now()
            session["startDate"] = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
            session["endDate"] = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")

            # return data to original codification
            logging.getLogger("ispyb_client").info("Session creation: %s" % session)
            session_id = self._collection.service.storeOrUpdateSession(
                utf_decode(session)
            )
            logging.getLogger("ispyb_client").info(
                "Session created. session_id=%s" % session_id
            )

            return self.get_proposal_tuple_by_code_and_number(
                proposal_tuple.proposal.code,
                proposal_tuple.proposal.number,
                beamline_name,
            )
        except Exception:
            raise

    def __to_session(self, session: dict[str, str]) -> Session:
        """
        Converts a dictionary composed by the person entries to the object proposal
        """
        return Session(
            session_id=session.get("sessionId"),
            proposal_id=session.get("proposalId"),
            beamline_name=session.get("beamlineName"),
            comments=session.get("comments"),
            end_date=session.get("endDate"),
            nb_shifts=session.get("nbShifts"),
            scheduled=session.get("scheduled"),
            start_date=session.get("startDate"),
        )

    def __to_proposal(self, proposal: dict[str, str]) -> Proposal:
        """
        Converts a dictionary composed by the person entries to the object proposal
        """
        return Proposal(
            code=proposal.get("code").lower(),
            number=proposal.get("number").lower(),
            proposal_id=proposal.get("proposalId"),
            title=proposal.get("title"),
            type=proposal.get("type"),
        )

    def __to_person(self, person: dict[str, str]) -> Person:  # type: ignore
        """
        Converts a dictionary composed by the person entries to the object Person
        """
        return Person(
            email_address=person.get("emailAddress"),
            family_name=person.get("familyName"),
            given_name=person.get("givenName"),
            login=person.get("login"),
            person_id=person.get("personId"),
            phone_number=person.get("phoneNumber"),
            site_id=person.get("siteId"),
            title=person.get("title"),
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

    def find_session(self, session_id: str) -> Session:
        try:
            response = self._collection.service.findSession(session_id)
            return self.__to_session(asdict(response))
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
            # raise e
        return []

    def _is_session_scheduled_today(self, session: Session) -> bool:
        now = datetime.now()
        if session.start_date.date() <= now.date() <= session.end_date.date():
            return True
        return False

    def _get_todays_session(self, sessions: List[Session]) -> Union[Session, None]:
        try:
            for session in sessions:
                if self._is_session_scheduled_today(session):
                    return session
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

    ############# Legacy methods #####################
    def update_data_collection(self, mx_collection, wait=False):
        if "collection_id" in mx_collection:
            try:
                # Update the data collection group
                self.store_data_collection_group(mx_collection)
                data_collection = ISPyBValueFactory().from_data_collect_parameters(
                    self._collection, mx_collection
                )
                self._collection.service.storeOrUpdateDataCollection(data_collection)
            except WebFault as e:
                logging.getLogger("ispyb_client").exception(e)
            except URLError as e:
                logging.getLogger("ispyb_client").exception(e)
        else:
            logging.getLogger("ispyb_client").error(
                "Error in update_data_collection: "
                + "collection-id missing, the ISPyB data-collection is not updated."
            )

    def store_image(self, image_dict):
        """
        Stores the image (image parameters) <image_dict>

        :param image_dict: A dictonary with image pramaters.
        :type image_dict: dict

        :returns: None
        """
        if self._collection:
            logging.getLogger("HWR").debug(
                "Storing image in lims. data to store: %s" % str(image_dict)
            )
            if "dataCollectionId" in image_dict:
                try:
                    image_id = self._collection.service.storeOrUpdateImage(image_dict)
                    logging.getLogger("HWR").debug(
                        "  - storing image in lims ok. id : %s" % image_id
                    )
                    return image_id
                except WebFault:
                    logging.getLogger("ispyb_client").exception(
                        "ISPyBClient: exception in store_image"
                    )
                except URLError as e:
                    logging.getLogger("ispyb_client").exception(e)
            else:
                logging.getLogger("ispyb_client").error(
                    "Error in store_image: "
                    + "data_collection_id missing, could not store image in ISPyB"
                )
        else:
            logging.getLogger("ispyb_client").exception(
                "Error in store_image: could not connect to server"
            )

    def get_samples(self, proposal_id, session_id):
        response_samples = []

        if self._tools_ws:
            try:
                response_samples = (
                    self._tools_ws.service.findSampleInfoLightForProposal(
                        proposal_id, self.beamline_name
                    )
                )
                response_samples = [
                    utf_encode(asdict(sample)) for sample in response_samples
                ]

            except WebFault as e:
                logging.getLogger("ispyb_client").exception(str(e))
            except URLError as e:
                logging.getLogger("ispyb_client").exception(e)
        else:
            logging.getLogger("ispyb_client").exception(
                "Error in get_samples: could not connect to server"
            )

        return response_samples

    def update_bl_sample(self, bl_sample):
        """
        Creates or stos a BLSample entry.
        # NBNB update doc string
        :param sample_dict: A dictonary with the properties for the entry.
        :type sample_dict: dict
        """
        if self._tools_ws:
            try:
                status = self._tools_ws.service.storeOrUpdateBLSample(bl_sample)
            except WebFault as e:
                logging.getLogger("ispyb_client").exception(str(e))
                status = {}
            except URLError as e:
                logging.getLogger("ispyb_client").exception(e)

            return status
        else:
            logging.getLogger("ispyb_client").exception(
                "Error in update_bl_sample: could not connect to server"
            )
