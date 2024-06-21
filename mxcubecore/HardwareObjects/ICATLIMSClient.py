"""
A client for PyISPyB Webservices.
"""

import json
import logging
import pathlib
import shutil
from mxcubecore.BaseHardwareObjects import HardwareObject
from mxcubecore import HardwareRepository as HWR
from mxcubecore.HardwareObjects.abstract.AbstractLims import AbstractLims
from pyicat_plus.client.main import IcatClient, IcatInvestigationClient


class ICATLIMSClient(AbstractLims):
    """
    LIMS implementation for ICAT-based facilities
    """

    def __init__(self, name):
        super().__init__(name)
        self.beamline_name = HWR.beamline.session.beamline_name
        self.icatClient = None
        self.catalogue = None
        self.lims_rest = None

    def init(self):
        if HWR.beamline.session:
            self.beamline_name = HWR.beamline.session.beamline_name
        self.url = self.get_property("ws_root")
        self.lims_rest = self.get_object_by_role("lims_rest")

    def _store_data_collection_group(self, group_data):
        pass

    @property
    def loginType(self):
        login_type = self.get_property("loginType")
        if login_type != "user":
            raise Exception(f"Unsupported login type: {login_type}")
        return "user"

    def _select_current_investigation(self, investigations):
        """
        Given a list of investigations will return the investigation
        scheduled now (startDate < now < endDate)
        """

        # TODO: it should return the investigation that is scheduled now
        return self.lims_rest._select_current_investigation()

    def __get_all_investigations(self):
        logging.getLogger("MX3.HWR").debug("[ICATClient] __get_all_investigations")
        return self.lims_rest.__get_all_investigations()

    def login(self, login_id, password, create_session):
        logging.getLogger("MX3.HWR").debug("[ICATClient] login")
        return self.lims_rest.authenticate(login_id, password)

    def _set_session_id(self, session_id):
        self.session_id = session_id
        if self.lims_rest is not None:
            self.lims_rest._set_session_id(session_id)

    def create_session(self, session_dict):
        pass

    def get_todays_session(self, prop, create_session=True):
        return self.lims_rest.get_todays_session(prop)

    def echo(self):
        """Mockup for the echo method."""
        return True

    def allow_session(self, session):
        return self.lims_rest.allow_session(session)

    def get_proposals_by_user(self, user_name):
        logging.getLogger("MX3.HWR").debug("get_proposals_by_user %s" % user_name)

        logging.getLogger("MX3.HWR").debug(
            "[ICATCLient] Read %s investigations" % len(self.lims_rest.investigations)
        )
        return self.lims_rest.to_sessions(self.lims_rest.investigations)

    def get_session_local_contact(self, session_id):
        logging.getLogger("MX3.HWR").debug("TDB: get_session_local_contact")
        return {
            "personId": 1,
            "laboratoryId": 1,
            "login": None,
            "familyName": "operator on ID14eh1",
        }

    def translate(self, code, what):
        """
        Given a proposal code, returns the correct code to use in the GUI,
        or what to send to LDAP, user office database, or the ISPyB database.
        """
        logging.getLogger("MX3.HWR").debug("TDB: translate")
        try:
            translated = self.__translations[code][what]
        except KeyError:
            translated = code

        return translated

    def is_connected(self):
        return self.login_ok

    def isInhouseUser(self, proposal_code, proposal_number):
        """
        Returns True if the proposal is considered to be a
        in-house user.

        :param proposal_code:
        :type proposal_code: str

        :param proposal_number:
        :type proposal_number: str

        :rtype: bool
        """
        logging.getLogger("MX3.HWR").debug("TDB: isInhouseUser")
        for proposal in self["inhouse"]:
            if proposal_code == proposal.code:
                if str(proposal_number) == str(proposal.number):
                    return True
        return False

    def store_data_collection(self, mx_collection, bl_config=None):
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
            "Data collection parameters stored " + "in ISPyB: %s" % str(mx_collection)
        )
        logging.getLogger("HWR").debug(
            "Beamline setup stored in ISPyB: %s" % str(bl_config)
        )

        return None, None

    def store_beamline_setup(self, session_id, bl_config):
        """
        Stores the beamline setup dict <bl_config>.

        :param session_id: The session id that the bl_config
                           should be associated with.
        :type session_id: int

        :param bl_config: The dictonary with beamline settings.
        :type bl_config: dict

        :returns beamline_setup_id: The database id of the beamline setup.
        :rtype: str
        """
        pass

    def update_data_collection(self, mx_collection, wait=False):
        """
        Updates the datacollction mx_collection, this requires that the
        collectionId attribute is set and exists in the database.

        :param mx_collection: The dictionary with collections parameters.
        :type mx_collection: dict

        :returns: None
        """
        pass

    def update_bl_sample(self, bl_sample):
        """
        Creates or stos a BLSample entry.

        :param sample_dict: A dictonary with the properties for the entry.
        :type sample_dict: dict
        # NBNB update doc string
        """
        pass

    def store_image(self, image_dict):
        """
        Stores the image (image parameters) <image_dict>

        :param image_dict: A dictonary with image pramaters.
        :type image_dict: dict

        :returns: None
        """
        pass

    def __find_sample(self, sample_ref_list, code=None, location=None):
        """
        Returns the sample with the matching "search criteria" <code> and/or
        <location> with-in the list sample_ref_list.

        The sample_ref object is defined in the head of the file.

        :param sample_ref_list: The list of sample_refs to search.

        :param code: The vial datamatrix code (or bar code)

        :param location: A tuple (<basket>, <vial>) to search for.
        :type location: tuple
        """
        logging.getLogger("MX3.HWR").debug("TDB: __find_sample")

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

    # TODO: it seems that both proposal_id and session_id are eNone
    def get_samples(self, proposal_id, session_id):
        """
        This returns the samples that are in the status processing

        proposal_id : it is the identifier of the experimental session (in ICAT is the investigationId)
        session_id: it does not seem to be used
        """
        try:
            logging.getLogger("MX3.HWR").debug(
                "[ICATClient] get_samples %s %s", proposal_id, session_id
            )
            parcels = self.lims_rest.get_parcels_by_investigation_id()
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
            metadata[
                "SampleTrackingContainer_type"
            ] = "UNIPUCK"  # this could be read from the configuration file somehow
            metadata[
                "SampleTrackingContainer_capaticy"
            ] = "16"  # this could be read from the configuration file somehow

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

            self.icatClient.store_dataset(
                beamline="ID29",
                proposal=f"{HWR.beamline.session.proposal_code}{HWR.beamline.session.proposal_number}",
                dataset=collection_parameters.path_parameters.prefix,
                path=data_path,
                metadata=data,
            )

            icat_metadata_path = pathlib.Path(data_path) / "metadata.json"
            with open(icat_metadata_path, "w") as f:
                f.write(json.dumps(data, indent=4))
                logging.getLogger("HWR").info(f"Wrote {icat_metadata_path}")

        except Exception:
            logging.getLogger("HWR").exception("")
