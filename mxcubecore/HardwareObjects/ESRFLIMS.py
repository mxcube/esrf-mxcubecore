import logging
from typing import List
from mxcubecore import HardwareRepository as HWR
from mxcubecore.model.lims_session import (
    Lims,
    LimsSessionManager,
    Session,
)
from mxcubecore.HardwareObjects.abstract.AbstractLims import AbstractLims
from pyicat_plus.client.main import IcatClient, IcatInvestigationClient
from pyicat_plus.client.models.session import Session as ICATSession


class ESRFLIMS(AbstractLims):
    """
    ESRF client.
    """

    def __init__(self, name):
        super().__init__(name)

    def init(self):
        self.drac = self.get_object_by_role("drac")
        self.ispyb = self.get_object_by_role("ispyb")

        self.is_local_host = False

    def get_lims_name(self) -> List[Lims]:
        """
        Gets the lims supported by this abstract LIMS

        Returns:HWR.beamline.session.session_id
            A list with the supported Lims objects
        """
        return self.drac.get_lims_name() + self.ispyb.get_lims_name()

    def get_session_id(self) -> str:
        """
        Gets session_id

        For the ESRF LIMS it will be the ISPyB session_id
        """
        logging.getLogger("MX3.HWR").debug(
            "Setting up drac session_id=%s" % (self.drac.get_session_id())
        )
        return self.drac.get_session_id()

    def login(self, user_name, token, is_local_host=False) -> LimsSessionManager:
        """
        This will try to authenticate with the token in DRAC
        If there is an active session then it will try to look
        for a available session in ISPyB
        Args:
            login_id
            password
            is_local_host
        Returns:
            LimsSessionManager a manager of the sessions corresponding to DRAC
        """
        self.is_local_host = is_local_host
        session_manager = self.drac.login(user_name, token, is_local_host)
        logging.getLogger("MX3.HWR").debug(
            "DRAC sessions=%s" % (len(self.drac.session_manager.sessions),)
        )

        if session_manager.active_session is None:
            logging.getLogger("MX3.HWR").debug(
                "DRAC no session selected then no activation of session in ISPyB"
            )
        else:
            self.ispyb.get_session_manager_by_code_number(
                session_manager.active_session.code,
                session_manager.active_session.number,
                self.is_local_host,
            )

        return self.drac.session_manager

    def is_user_login_type(self) -> bool:
        return True

    def get_samples(self, lims_name):
        """
        return [{'containerCode': 'Unipuck_1', 'containerSampleChangerLocation': '1', 'crystalId': '828455', 'crystalSpaceGroup': 'None',
        'diffractionPlan': {'diffractionPlanId': '1489676'}, 'proteinAcronym': 'LYS', 'sampleId': '996556', 'sampleLocation': '1', 'sampleName': 'Lys_p1_ISPyB_1'}]

        """
        logging.getLogger("MX3.HWR").debug("[ESRFLIMS] get_samples %s" % lims_name)

        drac_lims = [
            lims for lims in self.drac.get_lims_name() if lims.name == lims_name
        ]
        if len(drac_lims) == 1:
            return self.drac.get_samples(lims_name)
        else:
            return self.ispyb.get_samples(lims_name)

    def get_proposals_by_user(self, login_id: str):
        raise Exception("Not implemented")

    def create_session(self, session_dict):
        pass

    def create_mx_collection(self, collection_parameters):
        logging.getLogger("MX3.HWR").debug(
            "create_mx_collection. collection_parameters=%s", str(collection_parameters)
        )
        self.drac.create_mx_collection(collection_parameters)

    def _store_data_collection_group(self, group_data):
        """
        sessionId is the session_id of the ESRFLIMS that corresponds to DRAC
        to be changed by ISPyB
        group_data={'sessionId': '1721409431', 'experimentType': 'OSC'}
        """
        group_data["sessionId"] = self.ispyb.get_session_id()
        return self.ispyb._store_data_collection_group(group_data)

    def store_data_collection(self, mx_collection, bl_config=None):
        mx_collection["sessionId"] = self.ispyb.get_session_id()
        return self.ispyb.store_data_collection(mx_collection, bl_config)

    def store_image(self, image_dict):
        """
        Stores the image (image parameters) <image_dict>

        :param image_dict: A dictonary with image pramaters. Example:{'dataCollectionId': 3273801, 'fileName': 'Sample-1-1-01_1_0001.h5', 'fileLocation': '/data/visitor/blc15427/id23eh1/20240604/RAW_DATA/Sample-1-1-01/run_07_datacollection/', 'imageNumber': 1, 'measuredIntensity': 0.0, 'synchrotronCurrent': -0.015544, 'machineMessage': '', 'temperature': 0, 'jpegFileFullPath': '/data/pyarch/2024/id23eh1/blc15427/20240604/RAW_DATA/Sample-1-1-01/run_07_datacollection/Sample-1-1-01_1_0001.jpeg', 'jpegThumbnailFileFullPath': '/data/pyarch/2024/id23eh1/blc15427/20240604/RAW_DATA/Sample-1-1-01/run_07_datacollection/Sample-1-1-01_1_0001.thumb.jpeg'}
        :type image_dict: dict

        :returns: None
        """
        self.ispyb.store_image(image_dict)

    def find_sample_by_sample_id(self, sample_id):
        """
        Returns the ISPyB sample with the matching sample_id.

        Args:
            sample_id(int): Sample id from lims.
        Returns:
            (): sample or None if not found
        """
        return self.ispyb.find_sample_by_sample_id(sample_id)

    def update_data_collection(self, mx_collection, wait=False):
        """
        Updates the datacollction mx_collection, this requires that the
        collectionId attribute is set and exists in the database.

        :param mx_collection: The dictionary with collections parameters.
        :type mx_collection: dict

        :returns: None
        """
        mx_collection["sessionId"] = self.ispyb.get_session_id()
        return self.ispyb.update_data_collection(mx_collection, wait)

    def store_robot_action(self, robot_action_dict):
        robot_action_dict["sessionId"] = self.ispyb.get_session_id()
        return self.ispyb.store_robot_action(robot_action_dict)

    def is_session_already_active(self, session_id: str) -> bool:
        return self.drac.is_session_already_active(session_id)

    def set_active_session_by_id(self, session_id: str) -> Session:
        logging.getLogger("MX3.HWR").debug(
            "set_active_session_by_id. session_id=%s", str(session_id)
        )

        if self.drac.session_manager.active_session is not None:
            if self.ispyb.session_manager.active_session is not None:
                if self.drac.session_manager.active_session.session_id == session_id:
                    return self.drac.session_manager.active_session

        session = self.drac.set_active_session_by_id(session_id)

        # Check that session is not active already

        if self.ispyb.is_session_already_active_by_code(
            self.drac.session_manager.active_session.code,
            self.drac.session_manager.active_session.number,
        ):
            return self.drac.session_manager.active_session

        if session is not None:
            self.ispyb.get_session_manager_by_code_number(
                self.drac.session_manager.active_session.code,
                self.drac.session_manager.active_session.number,
                self.is_local_host,
            )

            if (
                self.drac.session_manager.active_session is not None
                and self.ispyb.session_manager.active_session is not None
            ):
                logging.getLogger("MX3.HWR").info(
                    "[ESRFLIMS] MXCuBE succesfully connected to DRAC:(%s, %s) ISPYB:(%s,%s)"
                    % (
                        self.drac.session_manager.active_session.proposal_name,
                        self.drac.session_manager.active_session.session_id,
                        self.ispyb.session_manager.active_session.proposal_name,
                        self.ispyb.session_manager.active_session.session_id,
                    )
                )
            else:
                logging.getLogger("MX3.HWR").warning(
                    "[ESRFLIMS] Problem when set_active_session_by_id. DRAC:(%s) ISPYB:(%s)"
                    % (
                        self.drac.session_manager.active_session.proposal_name,
                        self.ispyb.session_manager.active_session,
                    )
                )
            return self.drac.session_manager.active_session
        else:
            raise BaseException("Any candidate session was found")

    def allow_session(self, session: Session):
        return self.drac.allow_session(session)

    def get_session_by_id(self, id: str):
        return self.drac.get_session_by_id(id)

    def get_user_name(self):
        return self.drac.icat_session["username"]

    def authenticate(self, login_id: str, password: str) -> LimsSessionManager:
        return self.drac.authenticate(login_id, password)

    def echo(self):
        """Mockup for the echo method."""
        return True

    def is_connected(self):
        return True

    def update_bl_sample(self, bl_sample):
        self.ispyb.update_bl_sample(bl_sample)

