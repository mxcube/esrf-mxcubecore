from datetime import datetime, timedelta
import logging
import pathlib
import json
import shutil
from mxcubecore.BaseHardwareObjects import HardwareObject
from mxcubecore import HardwareRepository as HWR
from mxcubecore.model.lims_session import (
    Proposal,
    ProposalTuple,
    Session,
    Status,
)
from mxcubecore.HardwareObjects.abstract.AbstractLims import AbstractLims
from pyicat_plus.client.main import IcatClient, IcatInvestigationClient
from pyicat_plus.client.models.session import Session as ICATSession


class ICATLIMSRestClient(AbstractLims):
    """
    ICAT client.
    """

    def __init__(self, name):
        super().__init__(name)
        HardwareObject.__init__(self, name)
        self.investigations = None
        self.icatClient = None
        self.catalogue = None
        self.lims_rest = None
        self.ingesters = None

    def init(self):
        self.url = self.get_property("ws_root")
        self.ingesters = self.get_property("queue_urls")
        self.investigations = []

        self.icatClient = IcatClient(
            catalogue_url=self.url,
            tracking_url=self.url,
            metadata_urls=["bcu-mq-01:61613"],
        )

    def get_lims_name(self):
        return "ICAT"

    def login(self, login_id, password, create_session=False) -> ProposalTuple:
        return self.authenticate(login_id, password)

    def is_user_login_type(self) -> bool:
        return True

    def get_proposals_by_user(self, user_name):
        logging.getLogger("MX3.HWR").debug("get_proposals_by_user %s" % user_name)

        logging.getLogger("MX3.HWR").debug(
            "[ICATCLient] Read %s investigations" % len(self.lims_rest.investigations)
        )
        return self.lims_rest.to_sessions(self.lims_rest.investigations)

    def get_samples(self):
        """
        This returns the samples that are in the status processing

        proposal_id : it is the identifier of the experimental session (in ICAT is the investigationId)
        session_id: it does not seem to be used
        """
        try:
            logging.getLogger("MX3.HWR").debug(
                "[ICATClient] get_samples %s %s",
                self.session.session_id,
                self.session.proposal_name,
            )
            parcels = self.get_parcels_by_investigation_id()
            queue_samples = []
            for parcel in parcels:
                pucks = parcel["content"]
                logging.getLogger("MX3.HWR").debug(
                    "[ICATClient] Reading parcel '%s' with '%s' pucks"
                    % (parcel["name"], len(pucks))
                )
                # Parcels contains pucks: unipucks and spine pucks
                for puck in pucks:
                    tracking_samples = puck["content"]
                    if "sampleChangerLocation" in puck:
                        logging.getLogger("MX3.HWR").debug(
                            "[ICATClient] Processing puck '%s' within parcel '%s' at position '%s'. Number of samples '%s'"
                            % (
                                puck["name"],
                                parcel["name"],
                                puck["sampleChangerLocation"],
                                len(tracking_samples),
                            )
                        )
                        for tracking_sample in tracking_samples:
                            queue_samples.append(
                                self.__to_sample(tracking_sample, puck)
                            )

        except Exception as e:
            logging.getLogger("MX3.HWR").error(e)
            return []

        logging.getLogger("MX3.HWR").debug(
            "[ICATClient] Read %s samples" % (len(queue_samples))
        )

        return queue_samples

    def find(self, arr, atribute_name):
        for x in arr:
            if x["key"] == atribute_name:
                return x["value"]
        return ""

    def __to_sample(self, tracking_sample, puck):
        """Converts the sample tracking into the expected sample data structure"""
        experiment_plan = tracking_sample["experimentPlan"]
        return {
            "cellA": self.find(experiment_plan, "unit_cell_a"),
            "cellAlpha": self.find(experiment_plan, "unit_cell_alpha"),
            "cellB": self.find(experiment_plan, "unit_cell_b"),
            "cellBeta": self.find(experiment_plan, "unit_cell_beta"),
            "cellC": self.find(experiment_plan, "unit_cell_c"),
            "cellGamma": self.find(experiment_plan, "unit_cell_gamma"),
            "containerSampleChangerLocation": str(puck["sampleChangerLocation"]),
            "crystalSpaceGroup": self.find(experiment_plan, "forceSpaceGroup"),
            "diffractionPlan": {
                # "diffractionPlanId": 457980, TODO: do we need this?
                "experimentKind": self.find(experiment_plan, "experimentKind"),
                "numberOfPositions": self.find(experiment_plan, "numberOfPositions"),
                "observedResolution": self.find(experiment_plan, "observedResolution"),
                "preferredBeamDiameter": self.find(
                    experiment_plan, "preferredBeamDiameter"
                ),
                "radiationSensitivity": self.find(
                    experiment_plan, "radiationSensitivity"
                ),
                "requiredCompleteness": self.find(
                    experiment_plan, "requiredCompleteness"
                ),
                "requiredMultiplicity": self.find(
                    experiment_plan, "requiredMultiplicity"
                ),
                "requiredResolution": self.find(experiment_plan, "requiredResolution"),
            },
            "experimentType": self.find(experiment_plan, "workflowType"),
            "proteinAcronym": tracking_sample["name"],
            "sampleId": tracking_sample["sampleId"],
            "sampleLocation": tracking_sample["sampleContainerPosition"],
            "sampleName": tracking_sample["name"],
            "smiles": None,
        }

    def create_session(self, session_dict):
        pass

    def _store_data_collection_group(self, group_data):
        pass

    def dc_link(self, id: str) -> str:
        raise Exception("Not implemented")

    def get_dc(self, id: str) -> dict:
        raise Exception("Not implemented")

    def get_dc_thumbnail(self, id: str):
        raise Exception("Not implemented")

    def get_dc_image(self, id: str):
        raise Exception("Not implemented")

    def get_quality_indicator_plot(self, id: str):
        raise Exception("Not implemented")

    def store_robot_action(self, proposal_id: str):
        raise Exception("Not implemented")

    def get_rest_token(self, proposal_id: str):
        raise Exception("Not implemented")

    @property
    def filter(self):
        return self.get_property("filter", None)

    @property
    def override_beamline_name(self):
        return self.get_property(
            "override_beamline_name", HWR.beamline.session.beamline_name
        )

    @property
    def compatible_beamlines(self):
        return self.get_property(
            "compatible_beamlines", HWR.beamline.session.beamline_name
        )

    @property
    def data_portal_url(self):
        return self.get_property("data_portal_url", None)

    @property
    def user_portal_url(self):
        return self.get_property("user_portal_url", None)

    @property
    def logbook_url(self):
        return self.get_property("logbook_url", None)

    @property
    def before_offset_days(self):
        return self.get_property("before_offset_days", "1")

    @property
    def after_offset_days(self):
        return self.get_property("after_offset_days", "1")

    def _string_to_format_date(self, date: str, format: str) -> str:
        if date is not None:
            date_time = self._tz_aware_fromisoformat(date)
            if date_time is not None:
                return date_time.strftime(format)
        return ""

    def _string_to_date(self, date: str) -> str:
        return self._string_to_format_date(date, "%Y%m%d")

    def _string_to_time(self, date: str) -> str:
        return self._string_to_format_date(date, "%H:%M:%S")

    def _tz_aware_fromisoformat(self, date: str) -> datetime:
        try:
            return datetime.fromisoformat(date).astimezone()
        except Exception:
            return None

    def allow_session(self, session):
        logging.getLogger("MX3.HWR").debug(
            "[ICATRestClient] allow_session investigationId=%s",
            session["sessionId"],
        )
        self.catalogue.reschedule_investigation(session["sessionId"])

    def get_session_by_id(self, id: str):
        logging.getLogger("MX3.HWR").debug(
            "[ICATRestClient] get_session_by_id investigationId=%s investigations=%s",
            id,
            str(len(self.investigations)),
        )
        investigation_list = list(filter(lambda p: p["id"] == id, self.investigations))
        if len(investigation_list) == 1:
            self.investigation = investigation_list[0]
            return self.__to_session(investigation_list[0])
        logging.getLogger("MX3.HWR").warn(
            "[ICATRestClient] No investigation found. get_session_by_id investigationId=%s investigations=%s",
            id,
            str(len(self.investigations)),
        )
        return None

    def get_todays_session(self, prop, create_session=True):
        logging.getLogger("MX3.HWR").debug(
            "[ICATRestClient] get_todays_session investigationId=%s",
            prop["Proposal"]["proposalId"],
        )
        session = self.get_session_by_id(prop["Proposal"]["proposalId"])
        return {
            "session": session["Session"]["session"],
            "new_session_flag": False,
            "is_inhouse": False,
        }

    def __get_all_investigations(self):
        """Returns all investigations by user. An investigation corresponds to
        one experimental session. It returns an empty array in case of error"""
        try:

            self.investigations = []
            logging.getLogger("MX3.HWR").debug(
                "[ICATRestClient] __get_all_investigations before=%s after=%s beamline=%s isInstrumentScientist=%s isAdministrator=%s compatible_beamlines=%s"
                % (
                    self.before_offset_days,
                    self.after_offset_days,
                    self.override_beamline_name,
                    self.session["isInstrumentScientist"],
                    self.session["isAdministrator"],
                    self.compatible_beamlines,
                )
            )

            # TODO: This can be commented out when https://gitlab.esrf.fr/marcus.oscarsson/mxcube3/-/issues/985
            if self.session is not None and (
                self.session["isAdministrator"] or self.session["isInstrumentScientist"]
            ):
                self.investigations = self.catalogue.get_investigations_by(
                    start_date=datetime.today()
                    - timedelta(days=float(self.before_offset_days)),
                    end_date=datetime.today()
                    + timedelta(days=float(self.after_offset_days)),
                    instrument_name=self.compatible_beamlines,
                )
            else:
                self.investigations = self.catalogue.get_investigations_by(
                    filter=self.filter,
                    instrument_name=self.override_beamline_name,
                    start_date=datetime.today()
                    - timedelta(days=float(self.before_offset_days)),
                    end_date=datetime.today()
                    + timedelta(days=float(self.after_offset_days)),
                )
            logging.getLogger("MX3.HWR").debug(
                "[ICATRestClient] __get_all_investigations retrieved %s investigations"
                % len(self.investigations)
            )
            return self.investigations
        except Exception as e:
            self.investigations = []
            logging.getLogger("MX3.HWR").error(
                "[ICATRestClient] __get_all_investigations %s " % e
            )
        return self.investigations

    def __get_proposal_number_by_investigation(self, investigation):
        """
        Given an investigation it returns the proposal number.
        Example: investigation["name"] = "MX-1234"
        returns: 1234

        TODO: this might not work for all type of proposals (example: TEST proposals)
        """
        return (
            investigation["name"]
            .replace(investigation["type"]["name"], "")
            .replace("-", "")
        )

    def _get_data_portal_url(self, investigation):
        try:
            return (
                self.data_portal_url.replace("{id}", str(investigation["id"]))
                if self.data_portal_url is not None
                else ""
            )
        except Exception:
            return ""

    def _get_logbook_url(self, investigation):
        try:
            return (
                self.logbook_url.replace("{id}", str(investigation["id"]))
                if self.logbook_url is not None
                else ""
            )
        except Exception:
            return ""

    def _get_user_portal_url(self, investigation):
        try:
            return (
                self.user_portal_url.replace(
                    "{id}", str(investigation["parameters"]["Id"])
                )
                if self.user_portal_url is not None
                and investigation["parameters"]["Id"] is not None
                else ""
            )
        except Exception:
            return ""

    def __to_session(self, investigation) -> Session:
        """This methods converts a ICAT investigation into a session"""

        actual_start_date = (
            investigation["parameters"]["__actualStartDate"]
            if "__actualStartDate" in investigation["parameters"]
            else investigation["startDate"]
        )
        actual_end_date = (
            investigation["parameters"]["__actualEndDate"]
            if "__actualEndDate" in investigation["parameters"]
            else investigation.get("endDate", None)
        )
        # If session has been rescheduled new date is overwritten
        return Session(
            code=investigation["type"]["name"],
            number=self.__get_proposal_number_by_investigation(investigation),
            title=f'{investigation["title"]}',
            session_id=investigation["id"],
            proposal_id=investigation["id"],
            proposal_name=investigation["name"],
            beamline_name=investigation["instrument"]["name"],
            comments="",
            start_datetime=investigation.get(
                "startDate", None
            ),  # self._string_to_date(investigation.get("startDate", None)),
            start_date=self._string_to_date(investigation.get("startDate", None)),
            start_time=self._string_to_time(investigation.get("startDate", None)),
            end_datetime=investigation.get("endDate", None),
            end_date=self._string_to_date(
                investigation.get("endDate", None)
            ),  # self._string_to_time(investigation.get("endDate", None)),
            end_time=self._string_to_time(investigation.get("endDate", None)),
            actual_start_date=self._string_to_date(actual_start_date),
            actual_start_time=self._string_to_time(actual_start_date),
            actual_end_date=self._string_to_date(actual_end_date),
            actual_end_time=self._string_to_time(actual_end_date),
            nb_shifts=3,
            scheduled=self.is_scheduled_on_host_beamline(
                investigation["instrument"]["name"]
            ),
            is_scheduled_time=self.is_scheduled_now(actual_start_date, actual_end_date),
            is_scheduled_beamline=self.is_scheduled_on_host_beamline(
                investigation["instrument"]["name"]
            ),
            data_portal_URL=self._get_data_portal_url(investigation),
            user_portal_URL=self._get_user_portal_url(investigation),
            logbook_URL=self._get_logbook_url(investigation),
        )

        """
        session = {
            "code"= investigation["type"]["name"],
            "number": self.__get_proposal_number_by_investigation(investigation),
            "title": f'{investigation["title"]}',
            "proposalId": investigation["id"],
            "person": "",
            "startDate": self._string_to_date(investigation.get("startDate", None)),
            "endDate": self._string_to_date(investigation.get("endDate", None)),
            "startTime": self._string_to_time(investigation.get("startDate", None)),
            "endTime": self._string_to_time(investigation.get("endDate", None)),
            "actualStartDate": self._string_to_date(actual_start_date),
            "actualStartTime": self._string_to_time(actual_start_date),
            "actualEndDate": self._string_to_date(actual_end_date),
            "actualEndTime": self._string_to_time(actual_end_date),
            "beamlineName": investigation["instrument"]["name"],
            "sessionId": investigation["id"],
            "isScheduledBeamline": self.is_scheduled_on_host_beamline(
                investigation["instrument"]["name"]
            ),
            "isScheduledTime": self.is_scheduled_now(
                actual_start_date, actual_end_date
            ),
            "isRescheduled": (
                True if "__actualEndDate" in investigation["parameters"] else False
            ),
            "dataPortalURL": self._get_data_portal_url(investigation),
            "userPortalURL": self._get_user_portal_url(investigation),
            "logbookURL": self._get_logbook_url(investigation),
        }

        return {
            "status": {"code": "ok", "msg": "Successful login"},
            "Proposal": {
                "code": investigation["type"]["name"],
                "number": self.__get_proposal_number_by_investigation(investigation),
                "title": f'{investigation["title"]}',
                "proposalId": investigation["id"],
            },
            "Session": {
                "session": session,
                "new_session_flag": False,
                "is_inhouse": False,
            },
            # "local_contact": "BL Scientist", TODO: unused
            "Person": {"personId": 1, "laboratoryId": 1, "login": "test Login"},
            "Laboratory": {"laboratoryId": 1, "name": "TEST eh1"},
        }
        """

    def get_user_name(self):
        return self.session["username"]

    def to_sessions(self, investigations):
        return [self.__to_session(investigation) for investigation in investigations]

    def get_parcels_by_investigation_id(self):
        """Returns the parcels associated to an investigation"""
        try:
            logging.getLogger("MX3.HWR").debug(
                "[ICATRestClient] Retrieving parcels by investigation_id %s "
                % (self.investigation["id"])
            )
            parcels = self.tracking.get_parcels_by(self.session.session_id)
            logging.getLogger("MX3.HWR").debug(
                "[ICATRestClient] Successfully retrieved %s parcels" % (len(parcels))
            )
            return parcels
        except Exception as e:
            logging.getLogger("MX3.HWR").error(
                "[ICATRestClient] get_parcels_by_investigation_id %s " % (str(e))
            )
        return []

    def authenticate(self, login_id, password):
        logging.getLogger("MX3.HWR").debug(
            "[ICATRestClient] authenticate %s" % (login_id)
        )
        # Initialize ICAT client and catalogue
        self.icatClient = IcatClient(
            catalogue_url=self.url,
            tracking_url=self.url,
            catalogue_queues=["bcu-mq-01:61613"],
        )
        self.session: ICATSession = self.icatClient.do_log_in(password)
        # Catalogue client to retrieve investigations
        self.catalogue = self.icatClient.catalogue_client

        # Catalogue client to retrieve parcels and samples
        self.tracking = self.icatClient.tracking_client

        if self.catalogue is None or self.tracking is None:
            logging.getLogger("MX3.HWR").error(
                "[ICATRestClient] Error initializing catalogue/tracking. catalogue=%s tracking=%s"
                % (self.url, self.url)
            )
            raise RuntimeError("Could not initialize catalogue/tracking")

        # Connected to metadata catalogue
        logging.getLogger("MX3.HWR").debug(
            "[ICATRestClient] Connected succesfully to catalogue. fullName=%s url=%s"
            % (self.session["fullName"], self.url)
        )

        # Retrieving user's investigations
        investigations = self.__get_all_investigations()
        logging.getLogger("MX3.HWR").debug(
            "[ICATRestClient] Successfully retrieved %s investigations"
            % (len(investigations))
        )

        # if no investigation then we refuse login(?) TODO: sure?
        # refuse Login
        if len(investigations) == 0:
            return ProposalTuple()

        # Filter by investigation scheduled now
        sessions = self.to_sessions(self.investigations)

        self.investigation = investigations[0]
        logging.getLogger("MX3.HWR").debug(
            "[ICATRestClient] Selected investigation %s " % (self.investigation["name"])
        )
        self.set_sessions(sessions)

        proposal = Proposal(
            proposal_id=self.investigation["id"],
            type=self.investigation["type"]["name"],
            code=self.investigation["type"]["name"],
            number=self.__get_proposal_number_by_investigation(self.investigation),
            title=f'{self.investigation["title"]}',
        )
        proposal_tuple = ProposalTuple(
            proposal=proposal,
            todays_session=self.__to_session(self.investigation),
            sessions=self.sessions,
            status=Status(code="ok"),
        )

        return proposal_tuple

    def echo(self):
        """Mockup for the echo method."""
        return True

    def is_connected(self):
        return self.login_ok

    def __add_protein_acronym(self, sample_node, metadata):
        """
        Fills the sample acronym that should match with the acronym defined in the sample sheet
        """
        if sample_node is not None:
            if sample_node.crystals is not None:
                if len(sample_node.crystals) > 0:
                    crystal = sample_node.crystals[0]
                    if crystal.protein_acronym is not None:
                        metadata["SampleProtein_acronym"] = crystal.protein_acronym

    def __add_sample_changer_position(self, cell, puck, metadata):
        """
        Adds to the sample changer position based on the cell and the puck number

        Args:
            cell(str): cell position of the puck in the sample changer
            puck(str): position of the puck within the cell
            metadata(dict): metadata to be pushed to ICAT
        """
        try:
            if cell is not None and puck is not None:
                position = int(cell * 3) + int(puck)
                metadata["SampleChanger_position"] = position
        except Exception as e:
            logging.getLogger("HWR").exception(e)

    def add_sample_metadata(self, metadata, collection_parameters):
        """
        Adds to the metadata dictionary the metadata concerning sample position, container and tracking

        Args:
            metadata(dict): metadata to be pushed to ICAT
            collection_parameters(dict): Data collection parameters
        """
        try:
            queue_entry = HWR.beamline.queue_manager.get_current_entry()
            sample_node = queue_entry.get_data_model().get_sample_node()
            # sample_node.name this is name of the sample
            (cell, puck, sample_position) = sample_node.location  # Example: (8,2,5)
            self.__add_sample_changer_position(cell, puck, metadata)
            metadata["SampleTrackingContainer_position"] = sample_position
            metadata["SampleTrackingContainer_type"] = (
                "UNIPUCK"  # this could be read from the configuration file somehow
            )
            metadata["SampleTrackingContainer_capaticy"] = (
                "16"  # this could be read from the configuration file somehow
            )

            self.__add_protein_acronym(sample_node, metadata)

            if HWR.beamline.lims is not None:
                sample = HWR.beamline.lims.find_sample_by_sample_id(
                    collection_parameters.get("blSampleId")
                )
                if sample is not None:
                    if "containerCode" in sample:
                        metadata["SampleTrackingContainer_id"] = sample["containerCode"]
                    else:
                        metadata["SampleTrackingContainer_id"] = (
                            str(cell) + "_" + str(puck)
                        )  # Fake identifier that needs to be replaced by container code

        except Exception as e:
            logging.getLogger("HWR").exception(e)

    def create_mx_collection(self, collection_parameters):
        try:
            fileinfo = collection_parameters["fileinfo"]
            directory = pathlib.Path(fileinfo["directory"])
            dataset_name = directory.name
            # Determine the scan type
            if dataset_name.endswith("mesh"):
                scanType = "mesh"
            elif dataset_name.endswith("line"):
                scanType = "line"
            elif dataset_name.endswith("characterisation"):
                scanType = "characterisation"
            elif dataset_name.endswith("datacollection"):
                scanType = "datacollection"
            else:
                scanType = collection_parameters["experiment_type"]
            workflow_type = collection_parameters.get("workflow_type")
            if workflow_type is None:
                if directory.name.startswith("run"):
                    sample_name = directory.parent.name
                else:
                    sample_name = directory.name
                    dataset_name = fileinfo["prefix"]
            else:
                sample_name = directory.parent.parent.name
            oscillation_sequence = collection_parameters["oscillation_sequence"][0]
            beamline = HWR.beamline.session.beamline_name.lower()
            distance = HWR.beamline.detector.distance.get_value()
            proposal = f"{HWR.beamline.session.proposal_code}{HWR.beamline.session.proposal_number}"
            metadata = {
                "MX_beamShape": collection_parameters["beamShape"],
                "MX_beamSizeAtSampleX": collection_parameters["beamSizeAtSampleX"],
                "MX_beamSizeAtSampleY": collection_parameters["beamSizeAtSampleY"],
                "MX_dataCollectionId": collection_parameters["collection_id"],
                "MX_detectorDistance": distance,
                "MX_directory": str(directory),
                "MX_exposureTime": oscillation_sequence["exposure_time"],
                "MX_flux": collection_parameters["flux"],
                "MX_fluxEnd": collection_parameters["flux_end"],
                "MX_positionName": collection_parameters["position_name"],
                "MX_numberOfImages": oscillation_sequence["number_of_images"],
                "MX_oscillationRange": oscillation_sequence["range"],
                "MX_oscillationStart": oscillation_sequence["start"],
                "MX_oscillationOverlap": oscillation_sequence["overlap"],
                "MX_resolution": collection_parameters["resolution"],
                "scanType": scanType,
                "MX_startImageNumber": oscillation_sequence["start_image_number"],
                "MX_template": fileinfo["template"],
                "MX_transmission": collection_parameters["transmission"],
                "MX_xBeam": collection_parameters["xBeam"],
                "MX_yBeam": collection_parameters["yBeam"],
                "Sample_name": sample_name,
                "InstrumentMonochromator_wavelength": collection_parameters[
                    "wavelength"
                ],
                "Workflow_name": collection_parameters.get("workflow_name", None),
                "Workflow_type": collection_parameters.get("workflow_type", None),
                "Workflow_id": collection_parameters.get("workflow_uid", None),
                "MX_kappa_settings_id": collection_parameters.get(
                    "workflow_kappa_settings_id", None
                ),
                "MX_characterisation_id": collection_parameters.get(
                    "workflow_characterisation_id", None
                ),
                "MX_position_id": collection_parameters.get(
                    "workflow_position_id", None
                ),
                "group_by": collection_parameters.get("workflow_group_by", None),
            }
            # Store metadata on disk
            self.add_sample_metadata(metadata, collection_parameters)
            icat_metadata_path = pathlib.Path(directory) / "metadata.json"
            with open(icat_metadata_path, "w") as f:
                f.write(json.dumps(metadata, indent=4))
            # Create ICAT gallery
            gallery_path = directory / "gallery"
            gallery_path.mkdir(mode=0o755, exist_ok=True)
            for snapshot_index in range(1, 5):
                key = f"xtalSnapshotFullPath{snapshot_index}"
                if key in collection_parameters:
                    snapshot_path = pathlib.Path(collection_parameters[key])
                    if snapshot_path.exists():
                        logging.getLogger("HWR").debug(
                            f"Copying snapshot index {snapshot_index} to gallery"
                        )
                        shutil.copy(snapshot_path, gallery_path)
            logging.getLogger("HWR").info(f"Beamline: {beamline}")
            logging.getLogger("HWR").info(f"Proposal: {proposal}")

            self.icatClient.store_dataset(
                beamline=beamline,
                proposal=proposal,
                dataset=dataset_name,
                path=str(directory),
                metadata=metadata,
            )
            logging.getLogger("HWR").debug("Done uploading to ICAT")
        except Exception as e:
            logging.getLogger("HWR").exception(e)

    def create_ssx_collection(
        self, data_path, collection_parameters, beamline_parameters, extra_lims_values
    ):
        logging.getLogger("HWR").info("Storing data to ICAT")
        try:
            data = {
                "MX_scanType": "SSX-Jet",
                "MX_beamShape": beamline_parameters.beam_shape,
                "MX_beamSizeAtSampleX": beamline_parameters.beam_size_x,
                "MX_beamSizeAtSampleY": beamline_parameters.beam_size_y,
                "MX_detectorDistance": beamline_parameters.detector_distance,
                "MX_directory": data_path,
                "MX_exposureTime": collection_parameters.user_collection_parameters.exp_time,
                "MX_flux": extra_lims_values.flux_start,
                "MX_fluxEnd": extra_lims_values.flux_end,
                "MX_numberOfImages": collection_parameters.collection_parameters.num_images,
                "MX_resolution": beamline_parameters.resolution,
                "MX_transmission": beamline_parameters.transmission,
                "MX_xBeam": beamline_parameters.beam_x,
                "MX_yBeam": beamline_parameters.beam_y,
                "Sample_name": collection_parameters.path_parameters.prefix,
                "InstrumentMonochromator_wavelength": beamline_parameters.wavelength,
                "chipModel": extra_lims_values.chip_model,
                "monoStripe": extra_lims_values.mono_stripe,
                "energyBandwidth": beamline_parameters.energy_bandwidth,
                "detector_id": HWR.beamline.detector.get_property("detector_id"),
                "experimentType": collection_parameters.common_parameters.type,
            }

            data.update(collection_parameters.user_collection_parameters.dict())
            data.update(collection_parameters.collection_parameters.dict())

            # self.icatClient.store_dataset(
            #     beamline="ID29",
            #     proposal=f"{HWR.beamline.session.proposal_code}{HWR.beamline.session.proposal_number}",
            #     dataset=collection_parameters.path_parameters.prefix,
            #     path=data_path,
            #     metadata=data,
            # )

            icat_metadata_path = pathlib.Path(data_path) / "metadata.json"
            with open(icat_metadata_path, "w") as f:
                f.write(json.dumps(data, indent=4))
                logging.getLogger("HWR").info(f"Wrote {icat_metadata_path}")

        except Exception:
            logging.getLogger("HWR").exception("")
