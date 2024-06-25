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

    def __init__(
        self,
        ws_root: str,
        proxy: dict,
        ws_username: str,
        ws_password: str,
        beamline_name: str,
    ):
        self.ws_root = ws_root
        self.ws_username = ws_username
        self.ws_password = ws_password
        self.proxy = proxy  # type: ignore
        self.beamline_name = beamline_name

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
    def _store_data_collection_group(self, group_data):
        return self._collection.service.storeOrUpdateDataCollectionGroup(group_data)

    def store_data_collection_group(self, mx_collection):
        """
        Stores or updates a DataCollectionGroup object.
        The entry is updated of the group_id in the
        mx_collection dictionary is set to an exisiting
        DataCollectionGroup id.

        :param mx_collection: The dictionary of values to create the object from.
        :type mx_collection: dict

        :returns: DataCollectionGroup id
        :rtype: int
        """
        group_id = None
        if mx_collection["ispyb_group_data_collections"]:
            group_id = mx_collection.get("group_id", None)
        if group_id is None:
            # Create a new group id
            group = ISPyBValueFactory().dcg_from_dc_params(
                self._collection, mx_collection
            )
            self.group_id = self._collection.service.storeOrUpdateDataCollectionGroup(
                group
            )
        mx_collection["group_id"] = self.group_id

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

    def store_robot_action(self, robot_action_dict):
        """Stores robot action"""
        logging.getLogger("HWR").debug(
            "  - storing robot_actions in lims : %s" % str(robot_action_dict)
        )

        if True:
            robot_action_vo = self._collection.factory.create("robotActionWS3VO")

            robot_action_vo.actionType = robot_action_dict.get("actionType")
            robot_action_vo.containerLocation = robot_action_dict.get(
                "containerLocation"
            )
            robot_action_vo.dewarLocation = robot_action_dict.get("dewarLocation")

            # robot_action_vo.endTime = robot_action_dict.get("endTime")
            robot_action_vo.message = robot_action_dict.get("message")
            robot_action_vo.sampleBarcode = robot_action_dict.get("sampleBarcode")
            robot_action_vo.sessionId = robot_action_dict.get("sessionId")
            robot_action_vo.blSampleId = robot_action_dict.get("sampleId")
            robot_action_vo.startTime = datetime.strptime(
                robot_action_dict.get("startTime"), "%Y-%m-%d %H:%M:%S"
            )
            robot_action_vo.endTime = datetime.strptime(
                robot_action_dict.get("endTime"), "%Y-%m-%d %H:%M:%S"
            )
            robot_action_vo.status = robot_action_dict.get("status")
            robot_action_vo.xtalSnapshotAfter = robot_action_dict.get(
                "xtalSnapshotAfter"
            )
            robot_action_vo.xtalSnapshotBefore = robot_action_dict.get(
                "xtalSnapshotBefore"
            )
            return self._collection.service.storeRobotAction(robot_action_vo)
        return None

    def associate_bl_sample_and_energy_scan(self, entry_dict):
        try:
            return self._collection.service.storeBLSampleHasEnergyScan(
                entry_dict["energyScanId"], entry_dict["blSampleId"]
            )
        except Exception as e:
            logging.getLogger("ispyb_client").exception(str(e))
            return -1

    def get_data_collection(self, data_collection_id):
        """
        Retrives the data collection with id <data_collection_id>

        :param data_collection_id: Id of data collection.
        :type data_collection_id: int

        :rtype: dict
        """
        try:
            dc_response = self._collection.service.findDataCollection(
                data_collection_id
            )

            dc = utf_encode(asdict(dc_response))
            dc["startTime"] = datetime.strftime(dc["startTime"], "%Y-%m-%d %H:%M:%S")
            dc["endTime"] = datetime.strftime(dc["endTime"], "%Y-%m-%d %H:%M:%S")
            return dc
        except Exception as e:
            logging.getLogger("ispyb_client").exception(str(e))
            return {}

    def _store_data_collection(self, mx_collection, bl_config=None):
        """
        Stores the data collection mx_collection, and the beamline setup
        if provided.

        :param mx_collection: The data collection parameters.
        :type mx_collection: dict

        :param bl_config: The beamline setup.
        :type bl_config: dict

        :returns: None

        """
        logging.getLogger("HWR").debug(
            "Storing data collection in lims. data to store: %s" % str(mx_collection)
        )

        data_collection = ISPyBValueFactory().from_data_collect_parameters(
            self._collection, mx_collection
        )

        detector_id = 0
        if bl_config:
            lims_beamline_setup = ISPyBValueFactory.from_bl_config(
                self._collection, bl_config
            )

            lims_beamline_setup.synchrotronMode = data_collection.synchrotronMode

            self.store_beamline_setup(mx_collection["sessionId"], lims_beamline_setup)

            detector_params = ISPyBValueFactory().detector_from_blc(
                bl_config, mx_collection
            )

            detector = self.find_detector(*detector_params)
            detector_id = 0

            if detector:
                detector_id = detector.detectorId
                data_collection.detectorId = detector_id

        collection_id = self._collection.service.storeOrUpdateDataCollection(
            data_collection
        )
        logging.getLogger("HWR").debug(
            "  - storing data collection ok. collection id : %s" % collection_id
        )

        return (collection_id, detector_id)

    def store_beamline_setup(self, session_id, bl_config):
        """
        Stores the beamline setup dict <bl_config>.

        :param session_id: The session id that the beamline_setup
                           should be associated with.
        :type session_id: int

        :param bl_config: The dictonary with beamline settings.
        :type bl_config: dict

        :returns beamline_setup_id: The database id of the beamline setup.
        :rtype: str
        """
        blSetupId = None
        session = None

        try:
            session = self.get_session(session_id)
        except Exception:
            logging.getLogger("ispyb_client").exception(
                "ISPyBClient: exception in store_beam_line_setup"
            )
        else:
            if session is not None:
                try:
                    blSetupId = self._collection.service.storeOrUpdateBeamLineSetup(
                        bl_config
                    )
                    session["beamLineSetupId"] = blSetupId
                    self.update_session(session)
                except Exception as e:
                    logging.getLogger("ispyb_client").exception(str(e))
        return blSetupId

    def get_session(self, session_id):
        """
        Retrieves the session with id <session_id>.

        :returns: Dictionary with session data.
        :rtype: dict
        """
        try:
            session = self._collection.service.findSession(session_id)
            if session is not None:
                session.startDate = datetime.strftime(
                    session.startDate, "%Y-%m-%d %H:%M:%S"
                )
                session.endDate = datetime.strftime(
                    session.endDate, "%Y-%m-%d %H:%M:%S"
                )
                return utf_encode(asdict(session))
        except Exception as e:
            logging.getLogger("ispyb_client").exception(e)

        return {}

    def store_energy_scan(self, energyscan_dict):
        """
        Store energyscan.

        :param energyscan_dict: Energyscan data to store.
        :type energyscan_dict: dict

        :returns Dictonary with the energy scan id:
        :rtype: dict
        """

        status = {"energyScanId": -1}

        try:
            energyscan_dict["startTime"] = datetime.strptime(
                energyscan_dict["startTime"], "%Y-%m-%d %H:%M:%S"
            )

            energyscan_dict["endTime"] = datetime.strptime(
                energyscan_dict["endTime"], "%Y-%m-%d %H:%M:%S"
            )

            try:
                del energyscan_dict["remoteEnergy"]
            except KeyError:
                pass

            status["energyScanId"] = self._collection.service.storeOrUpdateEnergyScan(
                energyscan_dict
            )

        except Exception as e:
            logging.getLogger("ispyb_client").exception(str(e))

        return status

    def store_xfe_spectrum(self, xfespectrum_dict):
        """
        Stores a xfe spectrum.

        :returns: A dictionary with the xfe spectrum id.
        :rtype: dict

        """
        status = {"xfeFluorescenceSpectrumId": -1}
        try:
            if isinstance(xfespectrum_dict["startTime"], string_types):
                xfespectrum_dict["startTime"] = datetime.strptime(
                    xfespectrum_dict["startTime"], "%Y-%m-%d %H:%M:%S"
                )

                xfespectrum_dict["endTime"] = datetime.strptime(
                    xfespectrum_dict["endTime"], "%Y-%m-%d %H:%M:%S"
                )
            else:
                xfespectrum_dict["startTime"] = xfespectrum_dict["startTime"]
                xfespectrum_dict["endTime"] = xfespectrum_dict["endTime"]

            status["xfeFluorescenceSpectrumId"] = (
                self._collection.service.storeOrUpdateXFEFluorescenceSpectrum(
                    xfespectrum_dict
                )
            )

        except URLError as e:
            logging.getLogger("ispyb_client").exception(str(e))

        return status

    def _store_workflow(self, info_dict):
        workflow_id = None
        workflow_mesh_id = None
        grid_info_id = None

        if self._collection:
            workflow_vo = ISPyBValueFactory().workflow_from_workflow_info(info_dict)
            workflow_id = self._collection.service.storeOrUpdateWorkflow(workflow_vo)

            workflow_mesh_vo = ISPyBValueFactory().workflow_mesh_from_workflow_info(
                info_dict
            )
            workflow_mesh_vo.workflowId = workflow_id

            workflow_mesh_id = self._collection.service.storeOrUpdateWorkflowMesh(
                workflow_mesh_vo
            )

            grid_info_vo = ISPyBValueFactory().grid_info_from_workflow_info(info_dict)
            grid_info_vo.workflowMeshId = workflow_mesh_id

            grid_info_id = self._collection.service.storeOrUpdateGridInfo(grid_info_vo)
            return workflow_id, workflow_mesh_id, grid_info_id
        else:
            logging.getLogger("ispyb_client").exception(
                "Error in store_workflow: could not connect" + " to server"
            )
        return workflow_id, workflow_mesh_id, grid_info_id

    def _store_workflow_step(self, workflow_info_dict):
        """
        :param mx_collection: The data collection parameters.
        :type mx_collection: dict
        :returns: None
        """

        workflow_step_id = None
        if self._collection:
            workflow_step_dict = {}
            workflow_step_dict["workflowId"] = workflow_info_dict.get("workflow_id")
            workflow_step_dict["workflowStepType"] = workflow_info_dict.get(
                "workflow_type", "MeshScan"
            )
            workflow_step_dict["status"] = workflow_info_dict.get("status", "")
            workflow_step_dict["folderPath"] = workflow_info_dict.get(
                "result_file_path"
            )
            workflow_step_dict["imageResultFilePath"] = workflow_info_dict[
                "cartography_path"
            ]
            workflow_step_dict["htmlResultFilePath"] = workflow_info_dict[
                "html_file_path"
            ]
            workflow_step_dict["resultFilePath"] = workflow_info_dict["json_file_path"]
            workflow_step_dict["comments"] = workflow_info_dict.get("comments", "")
            workflow_step_dict["crystalSizeX"] = workflow_info_dict.get(
                "crystal_size_x"
            )
            workflow_step_dict["crystalSizeY"] = workflow_info_dict.get(
                "crystal_size_y"
            )
            workflow_step_dict["crystalSizeZ"] = workflow_info_dict.get(
                "crystal_size_z"
            )
            workflow_step_dict["maxDozorScore"] = workflow_info_dict.get(
                "max_dozor_score"
            )

            workflow_step_id = self._collection.service.storeWorkflowStep(
                json.dumps(workflow_step_dict)
            )
        else:
            logging.getLogger("ispyb_client").exception(
                "Error in store_workflow step: could not connect" + " to server"
            )
        return workflow_step_id

    def update_session(self, session_dict):
        """
        Update the session with the data in <session_dict>, the attribute
        sessionId in <session_dict> must be set.

        Warning: Missing attibutes in <session_dict> will set to null,
                 this could leed to loss of data.

        :param session_dict: The session to update.
        :type session_dict: dict

        :returns: None
        """
        session_dict["startDate"] = datetime.strptime(
            session_dict["startDate"], "%Y-%m-%d %H:%M:%S"
        )
        session_dict["endDate"] = datetime.strptime(
            session_dict["endDate"], "%Y-%m-%d %H:%M:%S"
        )

        try:
            session_dict["lastUpdate"] = datetime.strptime(
                session_dict["lastUpdate"].split("+")[0], "%Y-%m-%d %H:%M:%S"
            )
            session_dict["timeStamp"] = datetime.strptime(
                session_dict["timeStamp"].split("+")[0], "%Y-%m-%d %H:%M:%S"
            )
        except Exception:
            pass

        # return self.create_session(session_dict)
        return self._collection.service.storeOrUpdateSession(utf_decode(session_dict))