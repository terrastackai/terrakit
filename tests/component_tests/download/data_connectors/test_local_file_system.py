# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import os
import shutil

import numpy as np
import pytest
import xarray as xr

from pathlib import Path
from rasterio.crs import CRS

from terrakit import DataConnector
from terrakit.download.data_connectors.local_file_system import (
    SUPPORTED_EXTENSIONS,
    LocalFileSystem,
    _detect_format,
    _is_cog,
    _open_raster,
    _parse_date_from_filename,
    _read_geotiff,
    _read_geoparquet,
    _read_netcdf,
    _read_zarr,
    _scan_collection_dir,
)
from terrakit.general_utils.exceptions import TerrakitValueError


# ---------------------------------------------------------------------------
# Path to a real (but tiny) GeoTIFF bundled with the repository test resources
# ---------------------------------------------------------------------------
DUMMY_TIF = "tests/resources/component_test_data/download/dummy.tif"


# ---------------------------------------------------------------------------
# Helpers to create test data in various formats from dummy.tif
# ---------------------------------------------------------------------------


def _tif_to_netcdf(src_tif: str, dst_nc: str, var_name: str = "reflectance") -> None:
    """Convert *src_tif* to a NetCDF-4 file at *dst_nc*."""
    import rioxarray  # noqa

    da = xr.open_dataset(src_tif, engine="rasterio")
    da.to_netcdf(dst_nc)


def _tif_to_zarr(src_tif: str, dst_zarr: str, var_name: str = "reflectance") -> None:
    """Convert *src_tif* to a Zarr directory store at *dst_zarr*."""
    import rioxarray  # noqa

    da = xr.open_dataset(src_tif, engine="rasterio")
    da.to_zarr(dst_zarr, mode="w")


def _make_geoparquet(dst_parquet: str, bbox: list, n_points: int = 20) -> None:
    """Create a tiny GeoParquet file with random point geometries and two numeric columns."""
    import geopandas as gpd
    from shapely.geometry import Point

    minx, miny, maxx, maxy = bbox
    rng = np.random.default_rng(42)
    xs = rng.uniform(minx, maxx, n_points)
    ys = rng.uniform(miny, maxy, n_points)
    gdf = gpd.GeoDataFrame(
        {
            "value_a": rng.random(n_points).astype(np.float32),
            "value_b": rng.integers(0, 10, n_points).astype(np.float32),
            "geometry": [Point(x, y) for x, y in zip(xs, ys)],
        },
        crs="EPSG:4326",
    )
    gdf.to_parquet(dst_parquet)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def local_data_dir(tmp_path: Path) -> Path:
    """
    Create a temporary file-system layout for the local connector:

        <tmp_path>/
            sentinel2/
                2024-01-05_scene1.tif   (copy of dummy.tif)
                2024-01-15_scene2.tif   (copy of dummy.tif)
                2024-01-25_scene3.tif   (copy of dummy.tif)
            landsat8/
                2024-02-10_band.tif     (copy of dummy.tif)
            empty_collection/           (no files)

    Returns the ``tmp_path`` root.
    """
    sentinel2_dir = tmp_path / "sentinel2"
    sentinel2_dir.mkdir()
    landsat8_dir = tmp_path / "landsat8"
    landsat8_dir.mkdir()
    empty_dir = tmp_path / "empty_collection"
    empty_dir.mkdir()

    # Populate sentinel2 collection
    for fname in [
        "2024-01-05_scene1.tif",
        "2024-01-15_scene2.tif",
        "2024-01-25_scene3.tif",
    ]:
        shutil.copy(DUMMY_TIF, sentinel2_dir / fname)

    # Populate landsat8 collection
    shutil.copy(DUMMY_TIF, landsat8_dir / "2024-02-10_band.tif")

    return tmp_path


@pytest.fixture()
def data_connector_spec(local_data_dir: Path) -> dict:
    """Return a connector spec dict pointing at the temp data directory."""
    return {"base_path": str(local_data_dir)}


@pytest.fixture()
def multi_format_data_dir(tmp_path: Path) -> Path:
    """
    Collection directory containing one file in each supported format,
    all dated 2024-03-01 through 2024-03-05.

        <tmp_path>/
            mixed/
                2024-03-01_scene.tif
                2024-03-02_scene.nc
                2024-03-03_scene.zarr/
                2024-03-04_vectors.parquet
    """
    mixed_dir = tmp_path / "mixed"
    mixed_dir.mkdir()

    shutil.copy(DUMMY_TIF, mixed_dir / "2024-03-01_scene.tif")
    _tif_to_netcdf(DUMMY_TIF, str(mixed_dir / "2024-03-02_scene.nc"))
    _tif_to_zarr(DUMMY_TIF, str(mixed_dir / "2024-03-03_scene.zarr"))
    _make_geoparquet(
        str(mixed_dir / "2024-03-04_vectors.parquet"),
        bbox=[34.671, -0.091, 34.706, -0.087],
    )
    return tmp_path


@pytest.fixture()
def netcdf_data_dir(tmp_path: Path) -> Path:
    """Collection with three NetCDF files."""
    col = tmp_path / "netcdf_col"
    col.mkdir()
    for date in ["2024-05-01", "2024-05-10", "2024-05-20"]:
        _tif_to_netcdf(DUMMY_TIF, str(col / f"{date}_data.nc"))
    return tmp_path


@pytest.fixture()
def zarr_data_dir(tmp_path: Path) -> Path:
    """Collection with two Zarr directory stores."""
    col = tmp_path / "zarr_col"
    col.mkdir()
    for date in ["2024-06-01", "2024-06-15"]:
        _tif_to_zarr(DUMMY_TIF, str(col / f"{date}_store.zarr"))
    return tmp_path


@pytest.fixture()
def geoparquet_data_dir(tmp_path: Path) -> Path:
    """Collection with two GeoParquet files."""
    col = tmp_path / "parquet_col"
    col.mkdir()
    for date in ["2024-07-04", "2024-07-14"]:
        _make_geoparquet(
            str(col / f"{date}_vectors.parquet"),
            bbox=[34.671, -0.091, 34.706, -0.087],
        )
    return tmp_path


# ---------------------------------------------------------------------------
# Unit tests for private helpers
# ---------------------------------------------------------------------------


class TestParseDate:
    def test_date_at_start_of_filename(self):
        assert _parse_date_from_filename("2024-01-05_scene.tif") == "2024-01-05"

    def test_date_embedded_in_filename(self):
        assert _parse_date_from_filename("scene_2024-07-20_v2.tif") == "2024-07-20"

    def test_no_date_in_filename(self):
        assert _parse_date_from_filename("no_date_here.tif") is None

    def test_partial_date_not_matched(self):
        # Only "01-05" without year should not match
        assert _parse_date_from_filename("01-05_scene.tif") is None


class TestScanCollectionDir:
    def test_returns_files_within_date_range(self, local_data_dir: Path):
        collection_dir = str(local_data_dir / "sentinel2")
        unique_dates, results = _scan_collection_dir(
            collection_dir, "2024-01-01", "2024-01-20"
        )
        assert unique_dates == ["2024-01-05", "2024-01-15"]
        assert len(results) == 2

    def test_returns_all_files_when_range_covers_all(self, local_data_dir: Path):
        collection_dir = str(local_data_dir / "sentinel2")
        unique_dates, results = _scan_collection_dir(
            collection_dir, "2024-01-01", "2024-01-31"
        )
        assert len(unique_dates) == 3
        assert len(results) == 3

    def test_returns_empty_when_no_match(self, local_data_dir: Path):
        collection_dir = str(local_data_dir / "sentinel2")
        unique_dates, results = _scan_collection_dir(
            collection_dir, "2023-01-01", "2023-12-31"
        )
        assert unique_dates == []
        assert results == []

    def test_results_contain_expected_keys(self, local_data_dir: Path):
        collection_dir = str(local_data_dir / "sentinel2")
        _, results = _scan_collection_dir(collection_dir, "2024-01-01", "2024-01-31")
        for item in results:
            assert "date" in item
            assert "path" in item
            assert "format" in item
            assert os.path.isfile(item["path"])

    def test_format_key_is_geotiff_for_tif(self, local_data_dir: Path):
        collection_dir = str(local_data_dir / "sentinel2")
        _, results = _scan_collection_dir(collection_dir, "2024-01-01", "2024-01-31")
        assert all(r["format"] == "geotiff" for r in results)

    def test_zarr_directory_store_is_detected(self, zarr_data_dir: Path):
        collection_dir = str(zarr_data_dir / "zarr_col")
        unique_dates, results = _scan_collection_dir(
            collection_dir, "2024-06-01", "2024-06-30"
        )
        assert len(results) == 2
        assert all(r["format"] == "zarr" for r in results)
        # Zarr stores are directories, not files
        assert all(os.path.isdir(r["path"]) for r in results)

    def test_mixed_formats_all_indexed(self, multi_format_data_dir: Path):
        collection_dir = str(multi_format_data_dir / "mixed")
        _, results = _scan_collection_dir(
            collection_dir, "2024-03-01", "2024-03-31"
        )
        formats = {r["format"] for r in results}
        assert formats == {"geotiff", "netcdf", "zarr", "geoparquet"}

    def test_invalid_date_format_raises(self, local_data_dir: Path):
        collection_dir = str(local_data_dir / "sentinel2")
        with pytest.raises(TerrakitValueError, match="Invalid date format"):
            _scan_collection_dir(collection_dir, "01/01/2024", "31/01/2024")


# ---------------------------------------------------------------------------
# Tests for LocalFileSystem.list_collections()
# ---------------------------------------------------------------------------


class TestLocalFileSystemListCollections:
    def test_returns_sorted_collection_names(self, local_data_dir: Path):
        connector = LocalFileSystem()
        collections = connector.list_collections(base_path=str(local_data_dir))
        assert collections == ["empty_collection", "landsat8", "sentinel2"]

    def test_returns_empty_list_for_empty_base_dir(self, tmp_path: Path):
        connector = LocalFileSystem()
        assert connector.list_collections(base_path=str(tmp_path)) == []

    def test_raises_on_nonexistent_base_path(self):
        connector = LocalFileSystem()
        with pytest.raises(TerrakitValueError, match="does not exist"):
            connector.list_collections(base_path="/nonexistent/path/xyz")

    def test_via_data_connector_factory(self):
        """Ensure the connector is accessible through the high-level DataConnector."""
        dc = DataConnector(connector_type="local_file_system")
        assert hasattr(dc.connector, "list_collections")
        assert dc.connector.connector_type == "local_file_system"


# ---------------------------------------------------------------------------
# Tests for LocalFileSystem.find_data()
# ---------------------------------------------------------------------------


class TestLocalFileSystemFindData:
    def test_finds_files_within_date_range(
        self, local_data_dir: Path, data_connector_spec: dict
    ):
        connector = LocalFileSystem()
        unique_dates, results = connector.find_data(
            data_collection_name="sentinel2",
            date_start="2024-01-01",
            date_end="2024-01-20",
            data_connector_spec=data_connector_spec,
        )
        assert unique_dates == ["2024-01-05", "2024-01-15"]
        assert len(results) == 2

    def test_returns_none_when_no_files_in_range(
        self, local_data_dir: Path, data_connector_spec: dict
    ):
        connector = LocalFileSystem()
        unique_dates, results = connector.find_data(
            data_collection_name="sentinel2",
            date_start="2023-01-01",
            date_end="2023-12-31",
            data_connector_spec=data_connector_spec,
        )
        assert unique_dates is None
        assert results is None

    def test_returns_none_for_empty_collection(
        self, local_data_dir: Path, data_connector_spec: dict
    ):
        connector = LocalFileSystem()
        unique_dates, results = connector.find_data(
            data_collection_name="empty_collection",
            date_start="2024-01-01",
            date_end="2024-12-31",
            data_connector_spec=data_connector_spec,
        )
        assert unique_dates is None
        assert results is None

    def test_raises_when_collection_dir_missing(self, data_connector_spec: dict):
        connector = LocalFileSystem()
        with pytest.raises(TerrakitValueError, match="does not exist"):
            connector.find_data(
                data_collection_name="nonexistent_collection",
                date_start="2024-01-01",
                date_end="2024-01-31",
                data_connector_spec=data_connector_spec,
            )

    def test_raises_without_base_path_in_spec(self):
        connector = LocalFileSystem()
        with pytest.raises(TerrakitValueError, match="base_path"):
            connector.find_data(
                data_collection_name="sentinel2",
                date_start="2024-01-01",
                date_end="2024-01-31",
                data_connector_spec={"other_key": "value"},
            )

    def test_raises_without_data_connector_spec(self):
        connector = LocalFileSystem()
        with pytest.raises(TerrakitValueError, match="base_path"):
            connector.find_data(
                data_collection_name="sentinel2",
                date_start="2024-01-01",
                date_end="2024-01-31",
                data_connector_spec=None,
            )

    def test_raises_on_invalid_start_date(
        self, local_data_dir: Path, data_connector_spec: dict
    ):
        connector = LocalFileSystem()
        with pytest.raises(Exception):
            connector.find_data(
                data_collection_name="sentinel2",
                date_start="2024-31-01",  # invalid month/day swap
                date_end="2024-01-31",
                data_connector_spec=data_connector_spec,
            )

    def test_results_have_date_path_and_format_keys(
        self, local_data_dir: Path, data_connector_spec: dict
    ):
        connector = LocalFileSystem()
        _, results = connector.find_data(
            data_collection_name="sentinel2",
            date_start="2024-01-01",
            date_end="2024-01-31",
            data_connector_spec=data_connector_spec,
        )
        for item in results:
            assert "date" in item
            assert "path" in item
            assert "format" in item

    def test_via_data_connector_factory(
        self, local_data_dir: Path, data_connector_spec: dict
    ):
        dc = DataConnector(connector_type="local_file_system")
        unique_dates, results = dc.connector.find_data(
            data_collection_name="sentinel2",
            date_start="2024-01-01",
            date_end="2024-01-31",
            data_connector_spec=data_connector_spec,
        )
        assert isinstance(unique_dates, list)
        assert len(unique_dates) == 3


# ---------------------------------------------------------------------------
# Tests for LocalFileSystem.get_data()
# ---------------------------------------------------------------------------


class TestLocalFileSystemGetData:
    def test_returns_xarray_dataarray(
        self, local_data_dir: Path, data_connector_spec: dict
    ):
        connector = LocalFileSystem()
        da = connector.get_data(
            data_collection_name="sentinel2",
            date_start="2024-01-01",
            date_end="2024-01-10",
            data_connector_spec=data_connector_spec,
        )
        assert isinstance(da, xr.DataArray)

    def test_result_has_time_dimension(
        self, local_data_dir: Path, data_connector_spec: dict
    ):
        connector = LocalFileSystem()
        da = connector.get_data(
            data_collection_name="sentinel2",
            date_start="2024-01-01",
            date_end="2024-01-20",
            data_connector_spec=data_connector_spec,
        )
        assert "time" in da.dims
        assert len(da.time) == 2  # 2024-01-05 and 2024-01-15

    def test_full_date_range_returns_three_timestamps(
        self, local_data_dir: Path, data_connector_spec: dict
    ):
        connector = LocalFileSystem()
        da = connector.get_data(
            data_collection_name="sentinel2",
            date_start="2024-01-01",
            date_end="2024-01-31",
            data_connector_spec=data_connector_spec,
        )
        assert len(da.time) == 3

    def test_returns_none_when_no_files_match(
        self, local_data_dir: Path, data_connector_spec: dict
    ):
        connector = LocalFileSystem()
        result = connector.get_data(
            data_collection_name="sentinel2",
            date_start="2023-01-01",
            date_end="2023-12-31",
            data_connector_spec=data_connector_spec,
        )
        assert result is None

    def test_returns_none_for_empty_collection(
        self, local_data_dir: Path, data_connector_spec: dict
    ):
        connector = LocalFileSystem()
        result = connector.get_data(
            data_collection_name="empty_collection",
            date_start="2024-01-01",
            date_end="2024-12-31",
            data_connector_spec=data_connector_spec,
        )
        assert result is None

    def test_saves_file_when_save_file_provided(
        self,
        local_data_dir: Path,
        data_connector_spec: dict,
        tmp_path: Path,
    ):
        out_file = str(tmp_path / "output.tif")
        connector = LocalFileSystem()
        da = connector.get_data(
            data_collection_name="sentinel2",
            date_start="2024-01-05",
            date_end="2024-01-05",
            data_connector_spec=data_connector_spec,
            save_file=out_file,
        )
        assert isinstance(da, xr.DataArray)
        assert os.path.isfile(out_file)

    def test_band_names_applied_when_count_matches(
        self, local_data_dir: Path, data_connector_spec: dict
    ):
        """
        dummy.tif has 3 bands; passing 3 band names should relabel the
        'band' coordinate accordingly.
        """
        import rioxarray  # noqa

        import rasterio

        # Discover how many bands are in the dummy tif
        with rasterio.open(DUMMY_TIF) as src:
            nb = src.count

        custom_bands = [f"B{i:02d}" for i in range(1, nb + 1)]

        connector = LocalFileSystem()
        da = connector.get_data(
            data_collection_name="sentinel2",
            date_start="2024-01-05",
            date_end="2024-01-05",
            bands=custom_bands,
            data_connector_spec=data_connector_spec,
        )
        assert list(da.coords["band"].values) == custom_bands

    def test_via_data_connector_factory(
        self, local_data_dir: Path, data_connector_spec: dict
    ):
        dc = DataConnector(connector_type="local_file_system")
        da = dc.connector.get_data(
            data_collection_name="sentinel2",
            date_start="2024-01-01",
            date_end="2024-01-31",
            data_connector_spec=data_connector_spec,
        )
        assert isinstance(da, xr.DataArray)
        assert "time" in da.dims

    def test_raises_when_collection_dir_missing(self, data_connector_spec: dict):
        connector = LocalFileSystem()
        with pytest.raises(TerrakitValueError, match="does not exist"):
            connector.get_data(
                data_collection_name="nonexistent",
                date_start="2024-01-01",
                date_end="2024-01-31",
                data_connector_spec=data_connector_spec,
            )

    def test_raises_without_base_path_in_spec(self):
        connector = LocalFileSystem()
        with pytest.raises(TerrakitValueError, match="base_path"):
            connector.get_data(
                data_collection_name="sentinel2",
                date_start="2024-01-01",
                date_end="2024-01-31",
            )


# ---------------------------------------------------------------------------
# Tests for format detection helpers
# ---------------------------------------------------------------------------


class TestDetectFormat:
    def test_tif_is_geotiff(self):
        assert _detect_format("/path/to/file.tif") == "geotiff"

    def test_tiff_is_geotiff(self):
        assert _detect_format("/path/to/file.tiff") == "geotiff"

    def test_nc_is_netcdf(self):
        assert _detect_format("/data/2024-01-01.nc") == "netcdf"

    def test_zarr_file_is_zarr(self):
        assert _detect_format("/data/store.zarr") == "zarr"

    def test_zarr_directory_is_zarr(self):
        assert _detect_format("/data/2024-01-01_store.zarr/") == "zarr"

    def test_parquet_is_geoparquet(self):
        assert _detect_format("/data/vectors.parquet") == "geoparquet"

    def test_unsupported_extension_raises(self):
        with pytest.raises(TerrakitValueError, match="Unsupported file format"):
            _detect_format("/data/image.jpg")

    def test_case_insensitive(self):
        assert _detect_format("/data/FILE.TIF") == "geotiff"
        assert _detect_format("/data/FILE.NC") == "netcdf"

    def test_supported_extensions_dict_completeness(self):
        expected_keys = {".tif", ".tiff", ".nc", ".zarr", ".parquet"}
        assert set(SUPPORTED_EXTENSIONS.keys()) == expected_keys


class TestIsCog:
    def test_plain_geotiff_is_not_cog(self):
        # dummy.tif has no overviews → not a COG
        assert _is_cog(DUMMY_TIF) is False

    def test_nonexistent_path_returns_false(self):
        assert _is_cog("/nonexistent/path.tif") is False


# ---------------------------------------------------------------------------
# Tests for per-format readers
# ---------------------------------------------------------------------------


class TestReadGeotiff:
    def test_returns_dataarray(self):
        da = _read_geotiff(DUMMY_TIF, {})
        assert isinstance(da, xr.DataArray)

    def test_has_band_dim(self):
        da = _read_geotiff(DUMMY_TIF, {})
        assert "band" in da.dims

    def test_overview_level_ignored_when_no_overviews(self):
        # overview_level=0 on a file with no overviews should still open
        da = _read_geotiff(DUMMY_TIF, {"overview_level": 0})
        assert isinstance(da, xr.DataArray)


class TestReadNetcdf:
    def test_returns_dataarray(self, tmp_path: Path):
        nc_path = str(tmp_path / "test.nc")
        _tif_to_netcdf(DUMMY_TIF, nc_path)
        da = _read_netcdf(nc_path, {})
        assert isinstance(da, xr.DataArray)

    def test_has_band_dim(self, tmp_path: Path):
        nc_path = str(tmp_path / "test.nc")
        _tif_to_netcdf(DUMMY_TIF, nc_path)
        da = _read_netcdf(nc_path, {})
        assert "band" in da.dims

    def test_explicit_variable_selection(self, tmp_path: Path):
        nc_path = str(tmp_path / "test.nc")
        _tif_to_netcdf(DUMMY_TIF, nc_path)
        # Find the first variable name
        ds = xr.open_dataset(nc_path)
        first_var = list(ds.data_vars)[0]
        da = _read_netcdf(nc_path, {"variable": first_var})
        assert isinstance(da, xr.DataArray)

    def test_missing_variable_raises(self, tmp_path: Path):
        nc_path = str(tmp_path / "test.nc")
        _tif_to_netcdf(DUMMY_TIF, nc_path)
        from terrakit.general_utils.exceptions import TerrakitValueError
        with pytest.raises(TerrakitValueError, match="not found"):
            _read_netcdf(nc_path, {"variable": "__nonexistent_var__"})


class TestReadZarr:
    def test_returns_dataarray(self, tmp_path: Path):
        zarr_path = str(tmp_path / "test.zarr")
        _tif_to_zarr(DUMMY_TIF, zarr_path)
        da = _read_zarr(zarr_path, {})
        assert isinstance(da, xr.DataArray)

    def test_has_band_dim(self, tmp_path: Path):
        zarr_path = str(tmp_path / "test.zarr")
        _tif_to_zarr(DUMMY_TIF, zarr_path)
        da = _read_zarr(zarr_path, {})
        assert "band" in da.dims

    def test_explicit_variable_selection(self, tmp_path: Path):
        zarr_path = str(tmp_path / "test.zarr")
        _tif_to_zarr(DUMMY_TIF, zarr_path)
        ds = xr.open_zarr(zarr_path, consolidated=False)
        first_var = list(ds.data_vars)[0]
        da = _read_zarr(zarr_path, {"variable": first_var})
        assert isinstance(da, xr.DataArray)

    def test_missing_variable_raises(self, tmp_path: Path):
        zarr_path = str(tmp_path / "test.zarr")
        _tif_to_zarr(DUMMY_TIF, zarr_path)
        with pytest.raises(TerrakitValueError, match="not found"):
            _read_zarr(zarr_path, {"variable": "__nonexistent__"})


class TestReadGeoparquet:
    _BBOX = [34.671, -0.091, 34.706, -0.087]

    def test_returns_dataarray(self, tmp_path: Path):
        p = str(tmp_path / "data.parquet")
        _make_geoparquet(p, self._BBOX)
        da = _read_geoparquet(p, {}, bbox=self._BBOX)
        assert isinstance(da, xr.DataArray)

    def test_has_band_dim(self, tmp_path: Path):
        p = str(tmp_path / "data.parquet")
        _make_geoparquet(p, self._BBOX)
        da = _read_geoparquet(p, {}, bbox=self._BBOX)
        assert "band" in da.dims

    def test_band_count_equals_value_columns(self, tmp_path: Path):
        p = str(tmp_path / "data.parquet")
        _make_geoparquet(p, self._BBOX)
        da = _read_geoparquet(p, {"value_columns": ["value_a"]}, bbox=self._BBOX)
        assert da.sizes["band"] == 1
        assert list(da.coords["band"].values) == ["value_a"]

    def test_auto_detects_numeric_columns(self, tmp_path: Path):
        p = str(tmp_path / "data.parquet")
        _make_geoparquet(p, self._BBOX)
        # _make_geoparquet creates value_a and value_b
        da = _read_geoparquet(p, {}, bbox=self._BBOX)
        assert da.sizes["band"] == 2

    def test_uses_total_bounds_when_no_bbox(self, tmp_path: Path):
        p = str(tmp_path / "data.parquet")
        _make_geoparquet(p, self._BBOX)
        da = _read_geoparquet(p, {}, bbox=None)
        assert isinstance(da, xr.DataArray)

    def test_custom_resolution_changes_grid_size(self, tmp_path: Path):
        p = str(tmp_path / "data.parquet")
        _make_geoparquet(p, self._BBOX)
        da_coarse = _read_geoparquet(p, {"resolution": 0.01}, bbox=self._BBOX)
        da_fine = _read_geoparquet(p, {"resolution": 0.001}, bbox=self._BBOX)
        # Finer resolution → larger grid
        assert da_fine.sizes["x"] > da_coarse.sizes["x"]

    def test_no_numeric_columns_raises(self, tmp_path: Path):
        import geopandas as gpd
        from shapely.geometry import Point

        p = str(tmp_path / "text_only.parquet")
        gdf = gpd.GeoDataFrame(
            {"label": ["a", "b"], "geometry": [Point(0, 0), Point(1, 1)]},
            crs="EPSG:4326",
        )
        gdf.to_parquet(p)
        with pytest.raises(TerrakitValueError, match="no numeric columns"):
            _read_geoparquet(p, {}, bbox=self._BBOX)


class TestOpenRasterDispatch:
    """Integration tests for the _open_raster dispatcher."""

    def test_dispatches_geotiff(self):
        da = _open_raster(DUMMY_TIF, "geotiff", {})
        assert isinstance(da, xr.DataArray)

    def test_dispatches_netcdf(self, tmp_path: Path):
        nc = str(tmp_path / "data.nc")
        _tif_to_netcdf(DUMMY_TIF, nc)
        da = _open_raster(nc, "netcdf", {})
        assert isinstance(da, xr.DataArray)

    def test_dispatches_zarr(self, tmp_path: Path):
        zarr = str(tmp_path / "data.zarr")
        _tif_to_zarr(DUMMY_TIF, zarr)
        da = _open_raster(zarr, "zarr", {})
        assert isinstance(da, xr.DataArray)

    def test_dispatches_geoparquet(self, tmp_path: Path):
        p = str(tmp_path / "data.parquet")
        bbox = [34.671, -0.091, 34.706, -0.087]
        _make_geoparquet(p, bbox)
        da = _open_raster(p, "geoparquet", {}, bbox=bbox)
        assert isinstance(da, xr.DataArray)

    def test_unknown_format_raises(self):
        with pytest.raises(TerrakitValueError, match="No reader"):
            _open_raster("/dummy", "unknown_format", {})


# ---------------------------------------------------------------------------
# Multi-format get_data tests
# ---------------------------------------------------------------------------


class TestGetDataNetcdf:
    def test_returns_dataarray_from_netcdf(self, netcdf_data_dir: Path):
        spec = {"base_path": str(netcdf_data_dir)}
        da = LocalFileSystem().get_data(
            data_collection_name="netcdf_col",
            date_start="2024-05-01",
            date_end="2024-05-31",
            data_connector_spec=spec,
        )
        assert isinstance(da, xr.DataArray)
        assert "time" in da.dims
        assert len(da.time) == 3

    def test_subset_by_date_range(self, netcdf_data_dir: Path):
        spec = {"base_path": str(netcdf_data_dir)}
        da = LocalFileSystem().get_data(
            data_collection_name="netcdf_col",
            date_start="2024-05-01",
            date_end="2024-05-05",
            data_connector_spec=spec,
        )
        assert len(da.time) == 1


class TestGetDataZarr:
    def test_returns_dataarray_from_zarr(self, zarr_data_dir: Path):
        spec = {"base_path": str(zarr_data_dir)}
        da = LocalFileSystem().get_data(
            data_collection_name="zarr_col",
            date_start="2024-06-01",
            date_end="2024-06-30",
            data_connector_spec=spec,
        )
        assert isinstance(da, xr.DataArray)
        assert len(da.time) == 2


class TestGetDataGeoparquet:
    _BBOX = [34.671, -0.091, 34.706, -0.087]

    def test_returns_dataarray_from_geoparquet(self, geoparquet_data_dir: Path):
        spec = {
            "base_path": str(geoparquet_data_dir),
            "resolution": 0.005,
        }
        da = LocalFileSystem().get_data(
            data_collection_name="parquet_col",
            date_start="2024-07-01",
            date_end="2024-07-31",
            bbox=self._BBOX,
            data_connector_spec=spec,
        )
        assert isinstance(da, xr.DataArray)
        assert len(da.time) == 2

    def test_explicit_value_columns_applied(self, geoparquet_data_dir: Path):
        spec = {
            "base_path": str(geoparquet_data_dir),
            "value_columns": ["value_a"],
            "resolution": 0.005,
        }
        da = LocalFileSystem().get_data(
            data_collection_name="parquet_col",
            date_start="2024-07-04",
            date_end="2024-07-04",
            bbox=self._BBOX,
            data_connector_spec=spec,
        )
        assert da.sizes["band"] == 1


class TestGetDataMixedFormats:
    """A collection folder containing GeoTIFF, NetCDF, Zarr, and GeoParquet."""

    _BBOX = [34.671, -0.091, 34.706, -0.087]

    def test_find_data_discovers_all_formats(self, multi_format_data_dir: Path):
        spec = {"base_path": str(multi_format_data_dir)}
        _, results = LocalFileSystem().find_data(
            data_collection_name="mixed",
            date_start="2024-03-01",
            date_end="2024-03-31",
            data_connector_spec=spec,
        )
        formats = {r["format"] for r in results}
        assert formats == {"geotiff", "netcdf", "zarr", "geoparquet"}
        assert len(results) == 4
