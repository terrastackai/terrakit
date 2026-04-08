# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import logging
import os
import re
import numpy as np
import xarray as xr
import rioxarray  # noqa – registers the .rio accessor

from datetime import datetime
from typing import Any, Union

from ..connector import Connector
from ...general_utils.exceptions import TerrakitValueError
from terrakit.validate.helpers import (
    check_start_end_date,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Format registry
# ---------------------------------------------------------------------------

#: Maps every recognised file-/directory-extension to a canonical format name.
#: ``"geotiff"`` covers both plain GeoTIFF and Cloud-Optimised GeoTIFF (COG).
SUPPORTED_EXTENSIONS: dict[str, str] = {
    ".tif": "geotiff",
    ".tiff": "geotiff",
    ".nc": "netcdf",
    ".zarr": "zarr",      # may be a file *or* a directory store
    ".parquet": "geoparquet",
}

# Pattern that matches an ISO date (YYYY-MM-DD) anywhere in a filename stem.
_DATE_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2})")

######################################################################################################
###  Supporting functions
######################################################################################################


def _parse_date_from_filename(filename: str) -> Union[str, None]:
    """
    Extract the first ISO date string (YYYY-MM-DD) embedded in *filename*.

    Args:
        filename (str): Bare file name (not a full path).

    Returns:
        str | None: The date string if found, otherwise None.
    """
    match = _DATE_PATTERN.search(filename)
    return match.group(1) if match else None


def _detect_format(path: str) -> str:
    """
    Return the canonical format name for *path* based on its extension.

    Zarr stores may be directories; the directory name must end with ``".zarr"``.

    Args:
        path (str): Full path to the file or directory.

    Returns:
        str: One of the values in :data:`SUPPORTED_EXTENSIONS`.

    Raises:
        TerrakitValueError: If the extension is not recognised.
    """
    name = os.path.basename(path.rstrip("/"))
    _, ext = os.path.splitext(name)
    ext = ext.lower()
    if ext not in SUPPORTED_EXTENSIONS:
        raise TerrakitValueError(
            f"Unsupported file format: '{ext}'. "
            f"Supported extensions: {sorted(SUPPORTED_EXTENSIONS)}"
        )
    return SUPPORTED_EXTENSIONS[ext]


def _is_cog(path: str) -> bool:
    """
    Return *True* when *path* is a Cloud-Optimised GeoTIFF.

    A COG carries its image-data blocks before the IFD (index) and always has
    overview levels.  The check uses rasterio's tag inspection as a fast
    heuristic.

    Args:
        path (str): Path to a GeoTIFF file.

    Returns:
        bool: ``True`` if the file is a COG, ``False`` otherwise.
    """
    try:
        import rasterio

        with rasterio.open(path) as src:
            # GDAL ≥ 3.1 / rio-cogeo write an explicit layout tag
            for ns in (None, "MAIN_FILE"):
                tags = src.tags(ns=ns) if ns else src.tags()
                if tags.get("layout", "").lower() == "cog":
                    return True
                if tags.get("OVR_RESAMPLING_ALG"):
                    return True
            # Fallback: presence of overviews is the minimal COG requirement
            if src.overviews(1):
                return True
    except Exception:
        pass
    return False


def _scan_collection_dir(
    collection_dir: str,
    date_start: str,
    date_end: str,
    extensions: Union[tuple, None] = None,
) -> tuple[list[str], list[dict[str, Any]]]:
    """
    Walk *collection_dir* and collect files **and** ``.zarr`` directory stores
    whose embedded date falls within [date_start, date_end].

    Args:
        collection_dir (str): Path to the collection sub-directory.
        date_start (str): Inclusive start date in 'YYYY-MM-DD' format.
        date_end (str): Inclusive end date in 'YYYY-MM-DD' format.
        extensions (tuple[str, ...] | None): Extensions to include.  Defaults
            to all keys in :data:`SUPPORTED_EXTENSIONS`.

    Returns:
        tuple[list[str], list[dict[str, Any]]]: Sorted unique dates and list of
            dicts with keys ``"date"``, ``"path"``, ``"format"``.
    """
    if extensions is None:
        extensions = tuple(SUPPORTED_EXTENSIONS.keys())

    try:
        start_dt = datetime.strptime(date_start, "%Y-%m-%d")
        end_dt = datetime.strptime(date_end, "%Y-%m-%d")
    except ValueError as exc:
        raise TerrakitValueError(
            f"Invalid date format. Expected 'YYYY-MM-DD', got: {exc}"
        ) from exc

    results: list[dict[str, Any]] = []
    unique_dates: set[str] = set()

    for root, dirs, files in os.walk(collection_dir):
        # ---- Zarr directory stores — prune from traversal ----
        zarr_dirs = [d for d in sorted(dirs) if d.lower().endswith(".zarr")]
        for zd in zarr_dirs:
            dirs.remove(zd)
            date_str = _parse_date_from_filename(zd)
            if date_str is None:
                logger.debug("Skipping Zarr store '%s': no date in name.", zd)
                continue
            try:
                file_dt = datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                continue
            if start_dt <= file_dt <= end_dt:
                full_path = os.path.join(root, zd)
                results.append({"date": date_str, "path": full_path, "format": "zarr"})
                unique_dates.add(date_str)

        # ---- Regular files ----
        for fname in sorted(files):
            _, ext = os.path.splitext(fname)
            ext_lower = ext.lower()
            if ext_lower not in extensions:
                continue
            fmt = SUPPORTED_EXTENSIONS.get(ext_lower, "")
            if not fmt:
                continue
            date_str = _parse_date_from_filename(fname)
            if date_str is None:
                logger.debug("Skipping '%s': no date pattern found.", fname)
                continue
            try:
                file_dt = datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                continue
            if start_dt <= file_dt <= end_dt:
                full_path = os.path.join(root, fname)
                results.append({"date": date_str, "path": full_path, "format": fmt})
                unique_dates.add(date_str)

    return sorted(unique_dates), results


# ---------------------------------------------------------------------------
# Per-format readers  (each returns xr.DataArray with dims band, y, x)
# ---------------------------------------------------------------------------


def _read_geotiff(path: str, spec: dict) -> xr.DataArray:
    """
    Open a GeoTIFF or Cloud-Optimised GeoTIFF with :mod:`rioxarray`.

    Optional spec key:

    * ``"overview_level"`` *(int)*: Rasterio overview level (0 = coarsest).
      Ignored when the file has no overviews.

    Args:
        path (str): Path to the GeoTIFF / COG file.
        spec (dict): Connector spec.

    Returns:
        xr.DataArray: DataArray with dimensions ``(band, y, x)``.
    """
    overview_level = spec.get("overview_level", None)
    open_kwargs: dict[str, Any] = {}
    if overview_level is not None:
        open_kwargs["overview_level"] = int(overview_level)
    da: xr.DataArray = rioxarray.open_rasterio(path, **open_kwargs)
    logger.debug(
        "Opened GeoTIFF '%s' (COG=%s), shape=%s.",
        path,
        _is_cog(path),
        da.shape,
    )
    return da


def _read_netcdf(path: str, spec: dict) -> xr.DataArray:
    """
    Open a NetCDF-4 file as an :class:`xarray.DataArray`.

    Optional spec key:

    * ``"variable"`` *(str)*: Data variable to extract.  Defaults to the first
      variable in the file.

    Args:
        path (str): Path to the NetCDF file.
        spec (dict): Connector spec.

    Returns:
        xr.DataArray: DataArray with dimensions ``(band, y, x)``.
    """
    ds = xr.open_dataset(path, engine="netcdf4", mask_and_scale=True)
    return _dataset_to_dataarray(ds, spec, path)


def _read_zarr(path: str, spec: dict) -> xr.DataArray:
    """
    Open a Zarr store (file or directory) as an :class:`xarray.DataArray`.

    Optional spec key:

    * ``"variable"`` *(str)*: Data variable to extract.  Defaults to the first
      variable in the store.

    Args:
        path (str): Path to the ``.zarr`` file or directory.
        spec (dict): Connector spec.

    Returns:
        xr.DataArray: DataArray with dimensions ``(band, y, x)``.
    """
    ds = xr.open_zarr(path, consolidated=False)
    return _dataset_to_dataarray(ds, spec, path)


def _read_geoparquet(
    path: str,
    spec: dict,
    bbox: Union[list, None] = None,
) -> xr.DataArray:
    """
    Read a GeoParquet file and rasterize its numeric columns into a multi-band
    :class:`xarray.DataArray`.

    Optional spec keys:

    * ``"value_columns"`` *(list[str])*: Columns to rasterize as bands.
      Defaults to all numeric columns.
    * ``"resolution"`` *(float)*: Pixel size in CRS units (default ``0.001``).

    The spatial extent is taken from *bbox* when provided; otherwise the total
    bounds of the GeoDataFrame are used.

    Args:
        path (str): Path to the GeoParquet file.
        spec (dict): Connector spec.
        bbox (list | None): ``[minx, miny, maxx, maxy]``.

    Returns:
        xr.DataArray: DataArray with dimensions ``(band, y, x)``.

    Raises:
        TerrakitValueError: If no usable value columns are found.
    """
    import geopandas as gpd
    from rasterio.features import rasterize as _rasterize
    from rasterio.transform import from_bounds

    gdf: gpd.GeoDataFrame = gpd.read_parquet(path)

    value_columns: list[str] = list(spec.get("value_columns", []))
    if not value_columns:
        value_columns = [
            c
            for c in gdf.columns
            if c != gdf.geometry.name and gdf[c].dtype.kind in ("f", "i", "u")
        ]
    if not value_columns:
        raise TerrakitValueError(
            f"GeoParquet '{path}' has no numeric columns to rasterize. "
            "Specify 'value_columns' in data_connector_spec."
        )

    if bbox is not None:
        minx, miny, maxx, maxy = float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3])
    else:
        minx, miny, maxx, maxy = gdf.total_bounds

    resolution: float = float(spec.get("resolution", 0.001))
    width = max(1, int(round((maxx - minx) / resolution)))
    height = max(1, int(round((maxy - miny) / resolution)))
    transform = from_bounds(minx, miny, maxx, maxy, width, height)

    band_arrays: list[np.ndarray] = []
    for col in value_columns:
        shapes_gen = (
            (geom, float(val))
            for geom, val in zip(gdf.geometry, gdf[col])
            if geom is not None and val is not None
        )
        band_arrays.append(
            _rasterize(
                shapes_gen,
                out_shape=(height, width),
                transform=transform,
                fill=np.nan,
                dtype=np.float32,
            )
        )

    data = np.stack(band_arrays)
    xs = np.linspace(minx + resolution / 2, maxx - resolution / 2, width)
    ys = np.linspace(maxy - resolution / 2, miny + resolution / 2, height)

    da = xr.DataArray(
        data,
        dims=["band", "y", "x"],
        coords={"band": value_columns, "y": ys, "x": xs},
    )
    if gdf.crs is not None:
        da = da.rio.write_crs(gdf.crs.to_string())
    logger.debug(
        "Rasterized GeoParquet '%s': %d band(s), %dx%d grid.",
        path, len(value_columns), width, height,
    )
    return da


# ---------------------------------------------------------------------------
# Shared Dataset → DataArray normaliser (NetCDF / Zarr)
# ---------------------------------------------------------------------------


def _dataset_to_dataarray(ds: xr.Dataset, spec: dict, path: str) -> xr.DataArray:
    """
    Select a variable from *ds* and normalise dims to ``(band, y, x)``.

    Args:
        ds (xr.Dataset): Opened dataset.
        spec (dict): Connector spec; may contain ``"variable"``.
        path (str): File path (error messages only).

    Returns:
        xr.DataArray
    """
    data_vars = list(ds.data_vars)
    if not data_vars:
        raise TerrakitValueError(f"'{path}' contains no data variables.")

    var_name: str = spec.get("variable", data_vars[0])
    if var_name not in ds:
        raise TerrakitValueError(
            f"Variable '{var_name}' not found in '{path}'. "
            f"Available: {data_vars}"
        )

    da: xr.DataArray = ds[var_name]

    # Best-effort: set spatial dims so rioxarray CRS operations work
    try:
        x_dim = _find_dim(da, ("x", "lon", "longitude"))
        y_dim = _find_dim(da, ("y", "lat", "latitude"))
        da = da.rio.set_spatial_dims(x_dim=x_dim, y_dim=y_dim)
    except Exception:
        logger.debug("Could not auto-detect spatial dims for '%s'.", path)

    # Ensure a 'band' dimension exists
    if "band" not in da.dims:
        try:
            spatial_dims: set[str] = {da.rio.x_dim, da.rio.y_dim}
        except Exception:
            spatial_dims = set()
        extra = [d for d in da.dims if d not in spatial_dims and d != "time"]
        if extra:
            da = da.rename({extra[0]: "band"})
        else:
            da = da.expand_dims("band")

    return da


def _find_dim(da: xr.DataArray, candidates: tuple) -> str:
    """Return the first candidate dimension name present in *da*."""
    for name in candidates:
        if name in da.dims:
            return name
    raise ValueError(f"None of {candidates} found in dims {list(da.dims)}")


# ---------------------------------------------------------------------------
# Top-level format dispatcher
# ---------------------------------------------------------------------------


def _open_raster(
    path: str,
    fmt: str,
    spec: dict,
    bbox: Union[list, None] = None,
) -> xr.DataArray:
    """
    Dispatch to the correct format reader.

    Args:
        path (str): File or Zarr directory path.
        fmt (str): Format name from :data:`SUPPORTED_EXTENSIONS`.
        spec (dict): Full connector spec.
        bbox (list | None): Bounding box for GeoParquet rasterization.

    Returns:
        xr.DataArray
    """
    if fmt == "geotiff":
        return _read_geotiff(path, spec)
    elif fmt == "netcdf":
        return _read_netcdf(path, spec)
    elif fmt == "zarr":
        return _read_zarr(path, spec)
    elif fmt == "geoparquet":
        return _read_geoparquet(path, spec, bbox=bbox)
    else:
        raise TerrakitValueError(f"No reader registered for format '{fmt}'.")


######################################################################################################
###  Connector class
######################################################################################################


class LocalFileSystem(Connector):
    """
    A data connector that reads geospatial data from the **local file system**
    rather than an external API.

    Supported formats
    -----------------

    ========================  ===========  =============================================
    Format                    Extension    Notes
    ========================  ===========  =============================================
    GeoTIFF                   ``.tif``     Standard; uses :mod:`rioxarray`.
    Cloud-Optimised GeoTIFF   ``.tif``     Detected automatically; ``overview_level``
                                           controls which resolution to load.
    NetCDF-4                  ``.nc``      Variable selected via ``"variable"`` spec.
    Zarr                      ``.zarr``    Directory or single-file store; variable
                                           selected via ``"variable"`` spec.
    GeoParquet                ``.parquet`` Numeric columns rasterized to a grid;
                                           controlled by ``"value_columns"`` and
                                           ``"resolution"`` spec keys.
    ========================  ===========  =============================================

    Directory layout expected under ``base_path``::

        <base_path>/
            <collection_name>/
                2024-01-01_scene.tif
                2024-01-02_scene.nc
                2024-01-03_scene.zarr/   ← Zarr directory store
                2024-01-04_vectors.parquet
                …

    The date (YYYY-MM-DD) must appear somewhere in every file/directory name.

    ``data_connector_spec`` keys
    ----------------------------
    ========================  ==========  =============================================
    Key                       Type        Description
    ========================  ==========  =============================================
    ``base_path``             str         **Required.** Root directory.
    ``variable``              str         NetCDF / Zarr variable name.
    ``overview_level``        int         COG overview level (0 = coarsest).
    ``value_columns``         list[str]   GeoParquet columns to rasterize as bands.
    ``resolution``            float       GeoParquet pixel size in CRS units
                                          (default ``0.001``).
    ========================  ==========  =============================================

    Attributes:
        connector_type (str): ``"local_file_system"``
    """

    def __init__(self):
        self.connector_type: str = "local_file_system"

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def list_collections(self, base_path: str = ".") -> list[Any]:
        """
        Return names of all sub-directories inside *base_path*.

        Args:
            base_path (str): Root directory containing collection folders.

        Returns:
            list[str]: Sorted collection names.
        """
        if not os.path.isdir(base_path):
            raise TerrakitValueError(
                f"base_path '{base_path}' does not exist or is not a directory."
            )
        return sorted(
            entry.name for entry in os.scandir(base_path) if entry.is_dir()
        )

    def find_data(
        self,
        data_collection_name: str,
        date_start: str,
        date_end: str,
        area_polygon=None,
        bbox=None,
        bands: list = [],
        maxcc: int = 100,
        data_connector_spec: Union[dict, None] = None,
    ) -> Union[tuple[list[Any], list[dict[str, Any]]], tuple[None, None]]:
        """
        Discover supported raster files whose names contain a date within
        [date_start, date_end].

        Each result dict carries ``"date"``, ``"path"``, and ``"format"``.

        Args:
            data_collection_name (str): Collection sub-directory name.
            date_start (str): Inclusive start date 'YYYY-MM-DD'.
            date_end (str): Inclusive end date 'YYYY-MM-DD'.
            area_polygon: Ignored (API compatibility).
            bbox: Passed through to ``get_data`` for GeoParquet rasterization.
            bands (list): Ignored during discovery.
            maxcc (int): Ignored (local files have no cloud cover).
            data_connector_spec (dict | None): Must contain ``"base_path"``.

        Returns:
            tuple[list[str], list[dict]] | tuple[None, None]
        """
        check_start_end_date(date_start=date_start, date_end=date_end)

        base_path = self._get_base_path(data_connector_spec)
        collection_dir = os.path.join(base_path, data_collection_name)

        if not os.path.isdir(collection_dir):
            raise TerrakitValueError(
                f"Collection directory '{collection_dir}' does not exist."
            )

        unique_dates, results = _scan_collection_dir(
            collection_dir, date_start, date_end
        )

        if not results:
            logger.info(
                "No files found for collection '%s' between %s and %s.",
                data_collection_name, date_start, date_end,
            )
            return None, None

        logger.info(
            "Found %d file(s) across %d unique date(s) in collection '%s'.",
            len(results), len(unique_dates), data_collection_name,
        )
        return unique_dates, results

    def get_data(
        self,
        data_collection_name: str,
        date_start: str,
        date_end: str,
        area_polygon=None,
        bbox=None,
        bands: list = [],
        maxcc: int = 100,
        data_connector_spec: Union[dict, None] = None,
        save_file: Union[str, None] = None,
        working_dir: str = ".",
    ) -> Union[xr.DataArray, None]:
        """
        Load all matching files and stack them as ``(time, band, y, x)``.

        Each file is read by the reader appropriate for its format.  Mixed
        collections (e.g. some ``.tif`` and some ``.nc``) are supported as
        long as all files share the same spatial grid.

        Args:
            data_collection_name (str): Collection sub-directory name.
            date_start (str): Inclusive start date 'YYYY-MM-DD'.
            date_end (str): Inclusive end date 'YYYY-MM-DD'.
            area_polygon: Ignored (API compatibility).
            bbox (list | None): Spatial extent for GeoParquet rasterization.
            bands (list): Optional band label list (relabels ``band`` coord
                when length matches).
            maxcc (int): Ignored (API compatibility).
            data_connector_spec (dict | None): Must contain ``"base_path"``.
            save_file (str | None): Output GeoTIFF path.
            working_dir (str): Ignored (API compatibility).

        Returns:
            xr.DataArray | None
        """
        spec: dict = data_connector_spec if data_connector_spec is not None else {}

        unique_dates, results = self.find_data(
            data_collection_name=data_collection_name,
            date_start=date_start,
            date_end=date_end,
            area_polygon=area_polygon,
            bbox=bbox,
            bands=bands,
            maxcc=maxcc,
            data_connector_spec=data_connector_spec,
        )

        if unique_dates is None or results is None:
            return None

        da_list: list[xr.DataArray] = []
        for result in results:
            file_path = result["path"]
            date_str = result["date"]
            fmt = result["format"]
            logger.info("Loading '%s' (format=%s).", file_path, fmt)

            try:
                da: xr.DataArray = _open_raster(file_path, fmt, spec, bbox=bbox)
            except Exception as exc:
                logger.error("Failed to open '%s': %s", file_path, exc)
                raise TerrakitValueError(
                    f"Could not open '{fmt}' file '{file_path}': {exc}"
                ) from exc

            if bands and len(bands) == da.sizes.get("band", 0):
                da = da.assign_coords(band=bands)

            timestamp = datetime.strptime(date_str, "%Y-%m-%d")
            da = da.expand_dims(dim="time").assign_coords(time=[timestamp])
            da_list.append(da)

        if not da_list:
            return None

        stacked: xr.DataArray = xr.concat(da_list, dim="time")

        if save_file:
            from ..geodata_utils import save_data_array_to_file
            save_data_array_to_file(stacked, save_file)

        return stacked

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get_base_path(data_connector_spec: Union[dict, None]) -> str:
        """Extract and validate *base_path* from the connector spec."""
        if data_connector_spec is None or "base_path" not in data_connector_spec:
            raise TerrakitValueError(
                "data_connector_spec must be a dict containing key 'base_path'."
            )
        base_path = data_connector_spec["base_path"]
        if not os.path.isdir(base_path):
            raise TerrakitValueError(
                f"base_path '{base_path}' does not exist or is not a directory."
            )
        return base_path
