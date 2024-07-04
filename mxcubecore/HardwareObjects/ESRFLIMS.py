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
        return self.drac.get_lims_name() + self.ispyb.get_lims_name()

    def login(self, login_id, password, is_local_host=False) -> LimsSessionManager:
        """
        This does login on both drac and ISPyB and fails in case any of them fails
        """
        self.is_local_host = is_local_host
        session_manager = self.drac.login(login_id, password, is_local_host)
        logging.getLogger("MX3.HWR").debug(
            "DRAC sessions=%s" % (len(self.drac.session_manager.sessions),)
        )

        if session_manager.active_session is None:
            logging.getLogger("MX3.HWR").debug(
                "DRAC no session selected then no activation of session in ISPyB"
            )
        else:
            self._set_ispyb_active_session_by_id(
                session_manager.active_session.code,
                session_manager.active_session.number,
                self.is_local_host,
            )
        return self.drac.session_manager

    def is_user_login_type(self) -> bool:
        return True

    def get_samples(self, lims_name):
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

    def _store_data_collection_group(self, group_data):
        return self.ispyb._store_data_collection_group(group_data)

    def store_robot_action(self, robot_action_dict):
        return self.ispyb.store_robot_action(robot_action_dict)

    def set_active_session_by_id(self, session_id: str) -> Session:
        logging.getLogger("MX3.HWR").debug(
            "[ESRFLims] set_active_session_by_id. session_id=%s" % (session_id)
        )
        session = self.drac.set_active_session_by_id(session_id)

        if session is not None:
            self._set_ispyb_active_session_by_id(
                self.drac.session_manager.active_session.code,
                self.drac.session_manager.active_session.number,
                self.is_local_host,
            )

            if (
                self.drac.session_manager.active_session is not None
                and self.ispyb.session_manager.active_session is not None
            ):
                logging.getLogger("MX3.HWR").debug(
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

    def _set_ispyb_active_session_by_id(
        self, code: str, number: str, is_local_host: bool
    ) -> Session:
        self.ispyb.get_session_manager_by_code_number(code, number, is_local_host)
        logging.getLogger("MX3.HWR").debug(
            "ISPyB sessions=%s active_session=%s proposal=%s"
            % (
                len(self.drac.session_manager.sessions),
                self.drac.session_manager.active_session.session_id,
                self.drac.session_manager.active_session.proposal_name,
            )
        )

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
