"""
A client for PyISPyB Webservices.
"""
import json
import logging
import pathlib
import shutil

from mxcubecore.BaseHardwareObjects import HardwareObject
from mxcubecore import HardwareRepository as HWR
from pyicat_plus.client.main import IcatClient


class ICATClient(HardwareObject):
    """
    ICAT client.
    """

    def __init__(self, name):
        super().__init__(name)

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
            # metadata_urls = ["bcu-mq-04.esrf.fr:61613"]  # Test ICAT server
            metadata_urls = ["bcu-mq-01.esrf.fr:61613", "bcu-mq-02.esrf.fr:61613"]
            client = IcatClient(metadata_urls=metadata_urls)
            client.store_dataset(
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
            }

            metadata_urls = ["bcu-mq-01.esrf.fr:61613", "bcu-mq-02.esrf.fr:61613"]
            client = IcatClient(metadata_urls=metadata_urls)

            client.store_dataset(
                beamline="ID29",
                proposal=f"{HWR.beamline.session.proposal_code}{HWR.beamline.session.proposal_number}",
                dataset=collection_parameters.path_parameters.prefix,
                path=data_path,
                metadata=data,
            )
        except Exception:
            logging.getLogger("HWR").exception("")
