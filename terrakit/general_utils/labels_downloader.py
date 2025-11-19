# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import os
from pathlib import Path
from zipfile import ZipFile
from huggingface_hub import hf_hub_download


from terrakit.general_utils.rest import get
from terrakit.general_utils.exceptions import TerrakitBaseException

# Define a list of example labels used for demonstration and test purposes.
EXAMPLE_LABEL_FILES = [
    "EMSR801_AOI01_DEL_MONIT02_observedEventA_v1_2025-04-23.json",
    "EMSR748_AOI01_DEL_MONIT05_observedEventA_v1_2024-08-30.json",
]


EXAMPLE_RASTER_LABEL_FILES = [
    "subsetted_512x512_HLS.S30.T10SEH.2018245.v1.4.mask.tif",
    "subsetted_512x512_HLS.S30.T10SGD.2021306.v1.4.mask.tif",
]


# Copernicus Emergency Mapping Service API
COPERNICUS_URL = "https://rapidmapping.emergency.copernicus.eu/backend"


def rapid_mapping_event_lookup(event_id) -> dict:
    """
    Event look up for a given event from Copernicus Rapid Mapping Service.

    Parameters:
        event_id (str): event id is a three digit code unique to each event. Provide either as "EMSR000" or "000".

    Returns:
        dict[str: any]: json response containing full details available for a given event.
    """
    event_id = event_id.upper().strip("EMSR")
    url = f"{COPERNICUS_URL}/dashboard-api/public-activations/?code=EMSR{event_id}"
    resp = get(url)
    resp.raise_for_status()
    resp_json: dict = resp.json()
    return resp_json


def rapid_mapping_acquisition_time_lookup(event_id, monitoring_number) -> str:
    """
    Look up acquisition time for a given event ID from Copernicus Rapid Mapping Service.

    Parameters:
        event_id (str): event id is a three digit code unique to each event. Provide either as "EMSR000" or "000".
        monitoring_number (str): monitoring number given by a two digit number. Provide either as "MONIT00" or "monit00" or "00".

    Returns:
        str: acquisition time with format '%Y-%m-%dT%H:%M:%S'
    """
    event_id = event_id.upper().strip("EMSR")
    monitoring_number = monitoring_number.upper().strip("MONIT")
    resp_json = rapid_mapping_event_lookup(event_id)
    products = resp_json["results"][0]["aois"][0]["products"]
    for product in products:
        if int(monitoring_number) == product["monitoringNumber"]:
            acquisitionTime: str = product["images"][0]["acquisitionTime"]
    return acquisitionTime


def rapid_mapping_event_date_time_lookup(event_id) -> str:
    """
    Look up event date and time for a given event ID from Copernicus Rapid Mapping Service.

    Parameters:
        event_id (str): event id is a three digit code unique to each event. Provide either as "EMSR000" or "000".

    Returns:
        str: event time with format '%Y-%m-%dT%H:%M:%S'
    """
    event_id = event_id.upper().strip("EMSR")
    url = f"{COPERNICUS_URL}/dashboard-api/public-activations/?code=EMSR{event_id}"
    resp = get(url)
    resp.raise_for_status()
    event_time: str = resp.json()["results"][0]["eventTime"]
    return event_time


def rapid_mapping_geojson_downloader(
    event_id, aoi, monitoring_number, version, dest
) -> str:
    """
    Download GeoJSON labels from Copernicus Rapid Mapping Service.

    Parameters:
        event_id (str): event id is a three digit code unique to each event. Provide either as "EMSR000" or "emsr000" or "000".
        aoi (str): The area of interest is a two digit code for the aoi of the given event. Provide either as "AOI00" or "aoi00" or "00".
        monitoring_number (str): The monitoring number for the event. Provide either as "MONIT00" or "monit00" or "00".
        version (str): The event version number. Provide either as "V1" or "v1" or "1".
        dest (str): The destination directory to save the downloaded GeoJSON files.

    Returns:
        str: downloaded GeoJSON path name

    Example:
        ```python
        rapid_mapping_geojson_downloader(
            event_id="748",
            aoi="01",
            monitoring_number="05",
            version="v1",
            dest=LABELS_FOLDER,
        )
        ```
    """
    dest = Path(dest)
    dest = Path.absolute(dest)
    event_id = event_id.upper().strip("EMSR")
    aoi = aoi.upper().strip("AOI")
    monitoring_number = monitoring_number.upper().strip("MONIT")
    version = version.upper().strip("V")
    zip_id = f"EMSR{event_id}_AOI{aoi}_DEL_MONIT{monitoring_number}_v{version}.zip"
    zip_file = f"{dest}/{zip_id}"
    geojson_file = f"EMSR{event_id}_AOI{aoi}_DEL_MONIT{monitoring_number}_observedEventA_v{version}.json"

    # Check if labels already exist
    acquisition_time = rapid_mapping_acquisition_time_lookup(
        event_id, monitoring_number
    )
    acquisition_date = acquisition_time.split("T")[0]

    # update label geojson to include date
    geojson_file_with_date = geojson_file.replace(".json", f"_{acquisition_date}.json")
    if Path(f"{dest}/{geojson_file_with_date}").is_file():
        print(
            f".\n..\n...\n>>> Skipping download.\n\t>>> File already exists: {dest}/{geojson_file_with_date} already exists."
        )
    else:
        print(
            f".\n..\n...\n>>> Downloading labels from Copernicus Emergency Management Service for: \n\t>>> EMSR{event_id} <<<\n\t>>> AOI{aoi} <<<\n\t>>> MONIT{monitoring_number} <<<\n\t>>> observedEventA <<<\n\t>>> v{version} <<<"
        )
        # Create directory to download results to
        try:
            dest = Path(dest)
            dest.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            raise TerrakitBaseException(
                f"An issue occurred created {dest}. Please check this is a valid dir: {e}"
            )

        # Download zip
        url = f"{COPERNICUS_URL}/EMSR{event_id}/AOI{aoi}/DEL_MONIT{monitoring_number}/{zip_id}"
        print(f".\n..\n...\n>>> Requesting event data from:\n\t>>> {url} ... <<<")
        resp = get(url)
        resp.raise_for_status()

        # Extract zip
        try:
            with open(zip_file, "wb") as f:
                f.write(resp.content)
        except Exception as e:
            raise TerrakitBaseException(
                f"An issue occurred while writting the contents to {zip_file}: {e}"
            )
        print(f".\n..\n...\n>>> Extracting event geojson to:\n\t>>> {dest} ... <<<")
        try:
            with ZipFile(zip_file, "r") as z_file:
                if geojson_file in z_file.namelist():
                    z_file.extract(geojson_file, dest)
                    os.remove(zip_file)
                else:
                    print(
                        f"{geojson_file} not found in zip. Zip contents includes: {z_file.filelist}"
                    )
        except Exception as e:
            raise TerrakitBaseException(
                f"An issue occurred while extracting {geojson_file} from {zip_file}: {e}"
            )

        # update label geojson to include date
        geojson_file_with_date = geojson_file.replace(
            ".json", f"_{acquisition_date}.json"
        )
        try:
            os.rename(f"{dest}/{geojson_file}", f"{dest}/{geojson_file_with_date}")
            print(
                f".\n..\n...\n>>> Label geojson successfully saved:\n\t>>> acquisition date: {acquisition_date} <<<\n\t>>> {dest}/{geojson_file_with_date} <<<"
            )
        except FileNotFoundError:
            raise TerrakitBaseException(f"Error: {dest}/{geojson_file} not found.")
        except PermissionError:
            raise TerrakitBaseException(
                f"Error: Check permission to rename {dest}/{geojson_file}."
            )
        except OSError as e:
            raise TerrakitBaseException(
                f"An error occurred append date to {dest}/{geojson_file}: {e}"
            )
        print(".\n..\n...\n>>> Downloaded completed successfully <<<")
    return f"{dest}/{geojson_file_with_date}"


def hugging_face_file_downloader(
    repo_id: str,
    filename: str,
    revision: str = "main",
    subfolder: str | None = None,
    dest: str = ".",
):
    """
    Downloads a label file from Hugging Face Hub.

    Parameters:
        repo_id (str): The Hugging Face Hub repository ID.
        filename (str): The name of the file to download.
        revision (str, optional): The revision or commit to download. Defaults to "main".
        subfolder (str, optional): The subfolder within the repository to download from. Defaults to None.
        dest (str, optional): The destination directory to save the downloaded file. Defaults to the current directory (.).

    Returns:
        str: The path to the downloaded file.

    Example:
        ```python
        hugging_face_file_downloader(
            repo_id="ibm-nasa-geospatial/hls_burn_scars",
            filename="subsetted_512x512_HLS.S30.T10SGD.2021306.v1.4.mask.tif",
            revision="e48662b31288f1d5f1fd5cf5ebb0e454092a19ce",
            subfolder="training",
            dest="./docs/exmamples/test_wildfire",
        )
        ```
    """
    # Create directory to download results to
    try:
        dest = Path(dest)  # type: ignore[assignment]
        dest.mkdir(parents=True, exist_ok=True)  # type: ignore[attr-defined]
    except Exception as e:
        raise TerrakitBaseException(
            f"An issue occurred created {dest}. Please check this is a valid dir: {e}"
        )

    tmp_download_dir = "tmp_hf_download"
    hf_hub_download(
        repo_id=repo_id,
        repo_type="dataset",
        subfolder=subfolder,
        filename=filename,
        revision=revision,
        local_dir="./tmp_hf_download",
    )
    try:
        if subfolder:
            os.rename(
                f"./{tmp_download_dir}/{subfolder}/{filename}", f"{dest}/{filename}"
            )
        else:
            os.rename(f"{tmp_download_dir}/{filename}", f"{dest}/{filename}")
        print(".\n..\n...\n>>> Label successfully saved<<<")
    except FileNotFoundError:
        raise TerrakitBaseException(f"Error: {tmp_download_dir}/{filename} not found.")
    except PermissionError:
        raise TerrakitBaseException(
            f"Error: Check permission to rename {dest}/{filename}."
        )
    except OSError as e:
        raise TerrakitBaseException(
            f"An error occurred append date to {dest}/{filename}: {e}"
        )
    print(".\n..\n...\n>>> Downloaded completed successfully <<<")
