import logging
import requests
import datetime
from datetime import timedelta
from datetime import datetime as dt
import voluptuous as vol
from dateutil import tz
from homeassistant.components.sensor import (
    PLATFORM_SCHEMA,
    SensorDeviceClass,
    SensorStateClass,
)
from homeassistant.const import UnitOfEnergy
import homeassistant.helpers.config_validation as cv
from homeassistant.helpers.entity import Entity
from homeassistant.util import Throttle
from homeassistant.helpers.entity_component import async_update_entity
from homeassistant.core import HomeAssistant
from urllib.parse import quote
from .const import DOMAIN, CONF_CLIENT_ID, CONF_CLIENT_SECRET, TOKEN_URL, DATA_URL

# Import for external statistics
from homeassistant.components.recorder.models import StatisticMetaData
from homeassistant.components.recorder.statistics import async_add_external_statistics

# ANSI escape codes for colored text
class Colors:
    RED = "ERROR "  # Red text
    GREEN = "SUCCESS "  # Green text
    YELLOW = ""  # Yellow text
    BLUE = ""  # Blue text
    MAGENTA = "WARN "  # Magenta text
    CYAN = "INFO "  # Cyan text
    RESET = ""  # Reset to default color

# Create a custom logger for the component
_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.DEBUG)

# File handler for writing logs to a file
file_handler = logging.FileHandler("/config/egddistribuce.log")
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(message)s")
file_handler.setFormatter(formatter)
_LOGGER.addHandler(file_handler)

MIN_TIME_BETWEEN_UPDATES = timedelta(hours=6)

CONF_DAYS = "days"
CONF_EAN = "ean"

PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend(
    {
        vol.Required(CONF_CLIENT_ID): cv.string,
        vol.Required(CONF_CLIENT_SECRET): cv.string,
        vol.Required(CONF_EAN): cv.string,
        vol.Optional(CONF_DAYS, default=1): cv.positive_int,
    }
)

def setup_platform(hass, config, add_entities, discovery_info=None):
    client_id = config[CONF_CLIENT_ID]
    client_secret = config[CONF_CLIENT_SECRET]
    ean = config[CONF_EAN]
    days = config[CONF_DAYS]

    status_sensor = EGDPowerDataStatusSensor(hass, client_id, client_secret, ean, days)
    consumption_sensor = EGDPowerDataConsumptionSensor(
        hass, client_id, client_secret, ean, days
    )
    production_sensor = EGDPowerDataProductionSensor(
        hass, client_id, client_secret, ean, days
    )

    add_entities([status_sensor, consumption_sensor, production_sensor], True)

class EGDPowerDataSensor(Entity):
    def __init__(self, hass, client_id, client_secret, ean, days, profile):
        self.hass = hass
        self.client_id = client_id
        self.client_secret = client_secret
        self.ean = ean
        self.days = days
        self.profile = profile
        self._state = None
        self._attributes = {}
        self._session = requests.Session()
        self._unique_id = f"egddistribuce_{ean}_{days}_{profile.lower()}"
        self.entity_id = f"sensor.egddistribuce_{ean}_{days}_{profile.lower()}"
        _LOGGER.debug(
            dt.now().strftime("%Y-%m-%d %H:%M:%S")
            + f": {Colors.CYAN}Initialized EGDPowerDataSensor with EAN: {self.ean}, Profile: {self.profile}{Colors.RESET}"
        )
        self.update()

    @property
    def name(self):
        return f"EGD Power Data Sensor {self.ean} {self.days} {self.profile}"

    @property
    def state(self):
        return self._state

    @property
    def unique_id(self):
        return self._unique_id

    @property
    def extra_state_attributes(self):
        return self._attributes

    @property
    def device_class(self):
        return SensorDeviceClass.ENERGY

    @property
    def state_class(self):
        return SensorStateClass.TOTAL_INCREASING

    @property
    def unit_of_measurement(self):
        return UnitOfEnergy.KILO_WATT_HOUR

    @Throttle(MIN_TIME_BETWEEN_UPDATES)
    def update(self, no_throttle=False):
        if not self.ean:
            _LOGGER.warning(
                dt.now().strftime("%Y-%m-%d %H:%M:%S")
                + f": {Colors.RED}EAN is not set. Skipping update.{Colors.RESET}"
            )
            return

        _LOGGER.debug(
            dt.now().strftime("%Y-%m-%d %H:%M:%S")
            + f": {Colors.CYAN}>>>>>>>>>>>Updating EGD Power Data Sensor for EAN: {self.ean}, Profile: {self.profile}{Colors.RESET}"
        )
        try:
            token = self._get_access_token()
            self._get_data(token)
        except Exception as e:
            _LOGGER.error(
                dt.now().strftime("%Y-%m-%d %H:%M:%S")
                + f": {Colors.RED}Error updating sensor: {e}{Colors.RESET}"
            )

    def _get_access_token(self):
        _LOGGER.debug(
            dt.now().strftime("%Y-%m-%d %H:%M:%S")
            + f": {Colors.CYAN}Retrieving access token{Colors.RESET}"
        )
        try:
            response = self._session.post(
                TOKEN_URL,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "scope": "namerena_data_openapi",
                },
            )
            response.raise_for_status()
            token = response.json().get("access_token")
            _LOGGER.debug(
                dt.now().strftime("%Y-%m-%d %H:%M:%S")
                + f": {Colors.GREEN}Access token retrieved: {token}{Colors.RESET}"
            )
            return token
        except requests.exceptions.RequestException as e:
            _LOGGER.error(
                dt.now().strftime("%Y-%m-%d %H:%M:%S")
                + f": {Colors.RED}Error retrieving access token: {e}{Colors.RESET}"
            )
            raise

    def _get_data(self, token):
        _LOGGER.debug(
            dt.now().strftime("%Y-%m-%d %H:%M:%S")
            + f": {Colors.CYAN}Retrieving data with token: {token}{Colors.RESET}"
        )

        # Define the CEST timezone
        local_tz = tz.gettz("Europe/Prague")

        # Define the start time and end time for 'self.days' days ago
        local_stime = (datetime.datetime.now() - timedelta(days=self.days)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        local_etime = (datetime.datetime.now() - timedelta(days=self.days)).replace(
            hour=23, minute=45, second=0, microsecond=0
        )

        # Assign the CEST timezone to the local time
        local_stime = local_stime.replace(tzinfo=local_tz)
        local_etime = local_etime.replace(tzinfo=local_tz)

        # Convert local time to UTC
        utc_stime = local_stime.astimezone(tz.tzutc())
        utc_etime = local_etime.astimezone(tz.tzutc())

        headers = {"Authorization": f"Bearer {token}"}
        params = {
            "ean": self.ean,
            "profile": self.profile,
            "from": utc_stime.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "to": utc_etime.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "pageSize": 3000,
        }

        try:
            _LOGGER.debug(
                dt.now().strftime("%Y-%m-%d %H:%M:%S")
                + f": {Colors.CYAN}Params: {params}{Colors.RESET}"
            )
            _LOGGER.debug(
                dt.now().strftime("%Y-%m-%d %H:%M:%S")
                + f": {Colors.CYAN}Data url: {DATA_URL}{Colors.RESET}"
            )
            response = self._session.get(DATA_URL, headers=headers, params=params)
            _LOGGER.debug(
                dt.now().strftime("%Y-%m-%d %H:%M:%S")
                + f": {Colors.MAGENTA}Response status code: {response.status_code}{Colors.RESET}"
            )
            _LOGGER.debug(
                dt.now().strftime("%Y-%m-%d %H:%M:%S")
                + f": {Colors.CYAN}Response content: {response.content}{Colors.RESET}"
            )
            response.raise_for_status()
            data = response.json()

            # Check if the response contains "No results"
            if isinstance(data, dict) and data.get("error") == "No results":
                _LOGGER.info(
                    dt.now().strftime("%Y-%m-%d %H:%M:%S")
                    + f": {Colors.MAGENTA}No data available for the requested period.{Colors.RESET}"
                )
                # Do not update the state or attributes
                return

            try:
                data_points = data[0]["data"]
                statistics_data = []
                total_energy_kWh = 0

                # Dictionary to hold hourly sums
                hourly_energy = {}

                for item in data_points:
                    timestamp_str = item["timestamp"]
                    value_kW = item["value"]
                    # Convert timestamp to datetime object in UTC
                    timestamp = datetime.datetime.fromisoformat(
                        timestamp_str.replace("Z", "+00:00")
                    )
                    # Round down to the start of the hour
                    hour_start = timestamp.replace(minute=0, second=0, microsecond=0)
                    # Calculate energy in kWh for the interval (15 minutes = 0.25 hours)
                    energy_kWh = value_kW * 0.25
                    # Add energy to the hourly total
                    hourly_energy.setdefault(hour_start, 0)
                    hourly_energy[hour_start] += energy_kWh
                    # Keep track of total energy for the day
                    total_energy_kWh += energy_kWh

                # Prepare statistics data points
                cumulative_energy = 0
                for hour in sorted(hourly_energy.keys()):
                    cumulative_energy += hourly_energy[hour]
                    statistics_data.append(
                        {
                            "start": hour,
                            "sum": cumulative_energy,  # Cumulative energy up to this hour
                        }
                    )

                # Extract object_id from self.entity_id
                object_id = self.entity_id.split(".", 1)[1]

                # Prepare metadata for the statistics
                metadata = StatisticMetaData(
                    has_mean=False,
                    has_sum=True,
                    name=self.name,
                    source="sensor",
                    statistic_id=f"sensor:{object_id}",
                    unit_of_measurement="kWh",
                )

                # Add the data to the recorder's statistics with proper historical timestamps
                self.hass.add_job(async_add_external_statistics, metadata, statistics_data)

                # Update the sensor's state and attributes
                self._state = total_energy_kWh
                self._attributes = {
                    "date": local_stime.date().isoformat(),
                    "total_energy_kWh": total_energy_kWh,
                    "data_points": len(data_points),
                }
                _LOGGER.debug(
                    dt.now().strftime("%Y-%m-%d %H:%M:%S")
                    + f": {Colors.CYAN}Total energy for {self._attributes['date']}: {total_energy_kWh} kWh{Colors.RESET}"
                )
            except Exception as e:
                self._state = None
                _LOGGER.error(
                    dt.now().strftime("%Y-%m-%d %H:%M:%S")
                    + f": {Colors.RED}Error processing data: {e}{Colors.RESET}"
                )
        except requests.exceptions.RequestException as e:
            _LOGGER.error(
                dt.now().strftime("%Y-%m-%d %H:%M:%S")
                + f": {Colors.RED}Error retrieving data: {e}{Colors.RESET}"
            )
            raise

class EGDPowerDataConsumptionSensor(EGDPowerDataSensor):
    def __init__(self, hass, client_id, client_secret, ean, days):
        # Call the parent constructor with profile 'ICC1' for consumption
        super().__init__(hass, client_id, client_secret, ean, days, "ICC1")

class EGDPowerDataProductionSensor(EGDPowerDataSensor):
    def __init__(self, hass, client_id, client_secret, ean, days):
        # Call the parent constructor with profile 'ISC1' for production
        super().__init__(hass, client_id, client_secret, ean, days, "ISC1")

class EGDPowerDataStatusSensor(Entity):
    def __init__(self, hass, client_id, client_secret, ean, days):
        self.hass = hass
        self.client_id = client_id
        self.client_secret = client_secret
        self.ean = ean
        self.days = days
        self._state = None
        self._attributes = {}
        self._session = requests.Session()
        self._unique_id = f"egddistribuce_status_{ean}_{days}"
        self.entity_id = f"sensor.egddistribuce_status_{ean}_{days}"
        _LOGGER.debug(
            dt.now().strftime("%Y-%m-%d %H:%M:%S")
            + f": {Colors.GREEN}Initialized EGDPowerDataStatusSensor with EAN: {self.ean}{Colors.RESET}"
        )
        self.update()

    @property
    def name(self):
        return f"EGD Power Data Status Sensor {self.ean} {self.days}"

    @property
    def state(self):
        return self._state

    @property
    def unique_id(self):
        return self._unique_id

    @property
    def extra_state_attributes(self):
        return self._attributes

    @Throttle(MIN_TIME_BETWEEN_UPDATES)
    def update(self):
        _LOGGER.debug(
            dt.now().strftime("%Y-%m-%d %H:%M:%S")
            + f": {Colors.CYAN}Updating EGD Power Data Status Sensor for EAN: {self.ean}{Colors.RESET}"
        )
        try:
            # Schedule the method without calling it
            self.hass.add_job(self._update_related_sensors)
            self._state = "updated"
        except Exception as e:
            _LOGGER.error(
                dt.now().strftime("%Y-%m-%d %H:%M:%S")
                + f": {Colors.RED}Error updating status sensor: {e}{Colors.RESET}"
            )

    def _update_related_sensors(self):
        _LOGGER.debug(
            dt.now().strftime("%Y-%m-%d %H:%M:%S")
            + f": {Colors.CYAN}Updating related sensors for EAN: {self.ean}{Colors.RESET}"
        )
        for entity_id in [
            f"sensor.egddistribuce_{self.ean}_{self.days}_icc1",
            f"sensor.egddistribuce_{self.ean}_{self.days}_isc1",
        ]:
            self.hass.add_job(async_update_entity, self.hass, entity_id)