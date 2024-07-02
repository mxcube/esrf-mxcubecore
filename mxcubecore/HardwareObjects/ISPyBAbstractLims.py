from __future__ import print_function
from urllib.parse import urljoin
import warnings
from pprint import pformat
from mxcubecore.HardwareObjects.abstract.AbstractLims import AbstractLims
from mxcubecore.HardwareObjects.abstract.ISPyBDataAdapter import ISPyBDataAdapter
from mxcubecore.model.lims_session import LimsSessionManager
from mxcubecore import HardwareRepository as HWR
import logging
import gevent


def in_greenlet(fun):
    def _in_greenlet(*args, **kwargs):
        log_msg = "lims client " + fun.__name__ + " called with: "

        for arg in args[1:]:
            try:
                log_msg += pformat(arg, indent=4, width=80) + ", "
            except Exception:
                pass

        logging.getLogger("ispyb_client").debug(log_msg)
        task = gevent.spawn(fun, *args)
        if kwargs.get("wait", False):
            task.get()

    return _in_greenlet


class ISPyBAbstractLIMS(AbstractLims):
    """
    Web-service client for ISPyB.
    """

    def __init__(self, name):
        super().__init__(name)
        self.ldapConnection = None
        self.pyispyb = None
        self._translations = {}
        self.authServerType = None
        self.loginTranslate = None
        self.base_result_url = None
        self.login_ok = False

    def init(self):
        """
        Init method declared by HardwareObject.
        """
        super().init()
        self.pyispyb = self.get_object_by_role("pyispyb")
        self.icat_client = self.get_object_by_role("icat_client")

        self.samples = []
        self.authServerType = self.get_property("authServerType") or "ldap"
        if self.authServerType == "ldap":
            # Initialize ldap
            self.ldapConnection = self.get_object_by_role("ldapServer")
            if self.ldapConnection is None:
                logging.getLogger("HWR").debug("LDAP Server is not available")

        self.loginTranslate = self.get_property("loginTranslate") or True

        # ISPyB Credentials
        self.ws_root = self.get_property("ws_root")
        self.ws_username = self.get_property("ws_username") or None
        self.ws_password = str(self.get_property("ws_password")) or None

        self.proxy_address = self.get_property("proxy_address")
        if self.proxy_address:
            self.proxy = {"http": self.proxy_address, "https": self.proxy_address}
        else:
            self.proxy = {}

        try:
            self.base_result_url = self.get_property("base_result_url").strip()
        except AttributeError:
            pass

        self.adapter = ISPyBDataAdapter(
            self.ws_root.strip(),
            self.proxy,
            self.ws_username,
            self.ws_password,
            self.beamline_name,
        )
        logging.getLogger("HWR").debug("[ISPYB] Proxy address: %s" % self.proxy)

        # Add the porposal codes defined in the configuration xml file
        # to a directory. Used by translate()
        if hasattr(HWR.beamline.session, "proposals"):
            for proposal in HWR.beamline.session["proposals"]:
                code = proposal.code
                self._translations[code] = {}
                try:
                    self._translations[code]["ldap"] = proposal.ldap
                except AttributeError:
                    pass
                try:
                    self._translations[code]["ispyb"] = proposal.ispyb
                except AttributeError:
                    pass
                try:
                    self._translations[code]["gui"] = proposal.gui
                except AttributeError:
                    pass

    def get_user_name(self):
        raise Exception("Abstract class. Not implemented")

    def is_user_login_type(self):
        raise Exception("Abstract class. Not implemented")

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
        self.adapter.store_beamline_setup(session_id, bl_config)

    def _translate(self, code, what):
        """
        Given a proposal code, returns the correct code to use in the GUI,
        or what to send to LDAP, user office database, or the ISPyB database.
        """
        try:
            translated = self._translations[code][what]
        except KeyError:
            translated = code
        return translated

    def echo(self):
        """
        Method to ensure the communication with the SOAP server.

        :returns: A boolean that indicates if the answer recived was
         satisfactory.
        :rtype: boolean
        """
        if not self.adapter._shipping:
            msg = "Error in echo: Could not connect to server."
            logging.getLogger("ispyb_client").warning(msg)
            raise Exception("Error in echo: Could not connect to server.")

        try:
            self.adapter._shipping._shipping.service.echo()
            return True
        except Exception as e:
            logging.getLogger("ispyb_client").error(str(e))

        return False

    def ldap_login(self, login_name, psd):
        warnings.warn(
            (
                "Using Authenticatior from ISPyBClient is deprecated,"
                "use Authenticator to authenticate spereatly and then login to ISPyB"
            ),
            DeprecationWarning,
        )

        return self.ldapConnection.authenticate(login_name, psd)

    def store_data_collection(self, *args, **kwargs):
        try:
            return self._store_data_collection(*args, **kwargs)
        except gevent.GreenletExit:
            # aborted by user ('kill')
            raise
        except Exception:
            # if anything else happens, let upper level process continue
            # (not a fatal error), but display exception still
            logging.exception("Could not store data collection")
            return (0, 0, 0)

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
        return self.adapter.store_data_collection(mx_collection, bl_config)

    @in_greenlet
    def update_data_collection(self, mx_collection, wait=False):
        """
        Updates the datacollction mx_collection, this requires that the
        collectionId attribute is set and exists in the database.

        :param mx_collection: The dictionary with collections parameters.
        :type mx_collection: dict

        :returns: None
        """
        self.adapter.update_data_collection(mx_collection, wait)

    def update_bl_sample(self, bl_sample):
        """
        Creates or stos a BLSample entry.
        # NBNB update doc string
        :param sample_dict: A dictonary with the properties for the entry.
        :type sample_dict: dict
        """
        return self.adapter.update_bl_sample(bl_sample)

    def store_image(self, image_dict):
        """
        Stores the image (image parameters) <image_dict>

        :param image_dict: A dictonary with image pramaters.
        :type image_dict: dict

        :returns: None
        """
        self.adapter.store_image(image_dict)

    def find_sample_by_sample_id(self, sample_id):
        """
        Returns the sample  with the matching sample_id.

        Args:
            sample_id(int): Sample id from lims.
        Returns:
            (): sample or None if not found
        """
        for sample in self.samples:
            try:
                if int(sample.get("limsID")) == sample_id:
                    return sample
            except (TypeError, KeyError):
                pass
        return None

    def get_samples(self):
        self.samples = []
        if self.session_manager.active_session is not None:
            self.samples = self.adapter.get_samples(
                self.session_manager.active_session.proposal_id
            )
        logging.getLogger("HWR").debug(
            "get_samples. %s samples retrieved. proposal_id=%s"
            % (len(self.samples), self.session_manager.active_session.proposal_id)
        )
        return self.samples

    def create_session(self, proposal_id: str):
        logging.getLogger("HWR").debug("create_session. proposal_id=%s" % proposal_id)
        session_manager = self.adapter.create_session(proposal_id, self.beamline_name)
        logging.getLogger("HWR").debug("Session created. proposal_id=%s" % proposal_id)
        return session_manager

    def store_energy_scan(self, energyscan_dict):
        """
        Store energyscan.

        :param energyscan_dict: Energyscan data to store.
        :type energyscan_dict: dict

        :returns Dictonary with the energy scan id:
        :rtype: dict
        """
        return self.adapter.store_energy_scan(energyscan_dict)

    def associate_bl_sample_and_energy_scan(self, entry_dict):
        return self.adapter.associate_bl_sample_and_energy_scan(entry_dict)

    def get_data_collection(self, data_collection_id):
        """
        Retrives the data collection with id <data_collection_id>

        :param data_collection_id: Id of data collection.
        :type data_collection_id: int

        :rtype: dict
        """
        return self.adapter.get_data_collection(self, data_collection_id)

    def get_session(self, session_id):
        """
        Retrieves the session with id <session_id>.

        :returns: Dictionary with session data.
        :rtype: dict
        """
        return self.adapter.get_session(session_id)

    def store_xfe_spectrum(self, xfespectrum_dict):
        """
        Stores a xfe spectrum.

        :returns: A dictionary with the xfe spectrum id.
        :rtype: dict

        """
        return self.adapter.store_xfe_spectrum(xfespectrum_dict)

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
        for proposal in self["inhouse"]:
            if proposal_code == proposal.code:
                if str(proposal_number) == str(proposal.number):
                    return True
        return False

    def _store_data_collection_group(self, group_data):
        return self.adapter._store_data_collection_group(group_data)

    def store_workflow(self, *args, **kwargs):
        try:
            return self._store_workflow(*args, **kwargs)
        except gevent.GreenletExit:
            raise
        except Exception:
            logging.exception("Could not store workflow")
            return None, None, None

    def store_robot_action(self, robot_action_dict):
        """Stores robot action
        dict: {'actionType': 'LOAD', 'containerLocation': 2, 'dewarLocation': 3, 'sampleBarcode': '', 'sampleId': '993763', 'sessionId': '86652', 'startTime': '2024-06-28 03:04:18', 'endTime': '2024-06-28 03:04:52', 'status': 'SUCCESS'}
        """
        return self.adapter.store_robot_action(robot_action_dict)

    def create_mx_collection(self, collection_parameters):
        self.icat_client.create_mx_collection(collection_parameters)

    def create_ssx_collection(
        self, data_path, collection_parameters, beamline_parameters, extra_lims_values
    ):
        self.icat_client.create_ssx_collection(
            data_path, collection_parameters, beamline_parameters, extra_lims_values
        )

    # Bindings to methods called from older bricks.
    # getProposal = get_proposal
    createSession = create_session
    getSession = get_session
    storeDataCollection = store_data_collection
    getDataCollection = get_data_collection
    updateBLSample = update_bl_sample
    associateBLSampleAndEnergyScan = associate_bl_sample_and_energy_scan
    updateDataCollection = update_data_collection
    storeImage = store_image
    storeEnergyScan = store_energy_scan
    storeXfeSpectrum = store_xfe_spectrum
