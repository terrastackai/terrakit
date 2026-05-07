# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import pytest
import xarray as xr
from unittest.mock import patch, MagicMock
from datetime import datetime

from terrakit import DataConnector
from terrakit.general_utils.exceptions import TerrakitValueError


class TestPlanetaryComputer:
    """Test suite for Planetary Computer data connector."""
    
    connector_type = "planetary_computer"
    collection = "sentinel-2-l2a"
    bands = ["B04", "B03", "B02"]  # RGB bands
    
    def test_list_collections(self):
        """Test that list_collections returns expected collections."""
        dc = DataConnector(connector_type=self.connector_type)
        collections = dc.connector.list_collections()
        
        assert isinstance(collections, list)
        assert len(collections) > 0
        assert self.collection in collections
    
    def test_connector_initialization(self):
        """Test that the connector initializes correctly."""
        dc = DataConnector(connector_type=self.connector_type)
        
        assert dc.connector.connector_type == self.connector_type
        assert dc.connector.stac_url is not None
        assert dc.connector.client is not None
    
    @pytest.mark.parametrize("start_date,end_date,bbox", [
        ("2024-01-01", "2024-01-31", (-122.5, 37.7, -122.3, 37.9)),
    ])
    def test_find_data_valid_params(self, start_date, end_date, bbox):
        """Test find_data with valid parameters."""
        dc = DataConnector(connector_type=self.connector_type)
        
        # Mock the STAC client search
        mock_item = MagicMock()
        mock_item.datetime = datetime(2024, 1, 15)
        mock_item.properties = {"eo:cloud_cover": 10}
        mock_item.to_dict.return_value = {
            "id": "test-item",
            "properties": {
                "datetime": "2024-01-15T10:00:00Z",
                "eo:cloud_cover": 10
            }
        }
        
        with patch.object(dc.connector.client, 'search') as mock_search:
            mock_search.return_value.items.return_value = [mock_item]
            
            unique_dates, results = dc.connector.find_data(
                data_collection_name=self.collection,
                date_start=start_date,
                date_end=end_date,
                bbox=bbox,
                bands=self.bands,
                maxcc=50
            )
            
            assert unique_dates is not None
            assert results is not None
            assert isinstance(unique_dates, list)
            assert isinstance(results, list)
            assert len(unique_dates) > 0
            assert len(results) > 0
    
    def test_find_data_invalid_collection(self):
        """Test find_data with invalid collection name."""
        dc = DataConnector(connector_type=self.connector_type)
        
        with pytest.raises(TerrakitValueError):
            dc.connector.find_data(
                data_collection_name="invalid-collection",
                date_start="2024-01-01",
                date_end="2024-01-31",
                bbox=(-122.5, 37.7, -122.3, 37.9)
            )
    
    def test_find_data_invalid_dates(self):
        """Test find_data with invalid date range."""
        dc = DataConnector(connector_type=self.connector_type)
        
        with pytest.raises(TerrakitValueError):
            dc.connector.find_data(
                data_collection_name=self.collection,
                date_start="2024-01-31",
                date_end="2024-01-01",  # End before start
                bbox=(-122.5, 37.7, -122.3, 37.9)
            )
    
    def test_find_data_invalid_bbox(self):
        """Test find_data with invalid bounding box."""
        dc = DataConnector(connector_type=self.connector_type)
        
        with pytest.raises(TerrakitValueError):
            dc.connector.find_data(
                data_collection_name=self.collection,
                date_start="2024-01-01",
                date_end="2024-01-31",
                bbox=(-122.3, 37.7, -122.5, 37.9)  # West > East
            )
    
    def test_find_data_cloud_filtering(self):
        """Test that cloud cover filtering works correctly."""
        dc = DataConnector(connector_type=self.connector_type)
        
        # Create mock items with different cloud covers
        mock_item_low_cc = MagicMock()
        mock_item_low_cc.datetime = datetime(2024, 1, 15)
        mock_item_low_cc.properties = {"eo:cloud_cover": 10}
        mock_item_low_cc.to_dict.return_value = {
            "id": "low-cc-item",
            "properties": {"datetime": "2024-01-15T10:00:00Z", "eo:cloud_cover": 10}
        }
        
        mock_item_high_cc = MagicMock()
        mock_item_high_cc.datetime = datetime(2024, 1, 20)
        mock_item_high_cc.properties = {"eo:cloud_cover": 80}
        mock_item_high_cc.to_dict.return_value = {
            "id": "high-cc-item",
            "properties": {"datetime": "2024-01-20T10:00:00Z", "eo:cloud_cover": 80}
        }
        
        with patch.object(dc.connector.client, 'search') as mock_search:
            mock_search.return_value.items.return_value = [mock_item_low_cc, mock_item_high_cc]
            
            # Filter with maxcc=50
            unique_dates, results = dc.connector.find_data(
                data_collection_name=self.collection,
                date_start="2024-01-01",
                date_end="2024-01-31",
                bbox=(-122.5, 37.7, -122.3, 37.9),
                maxcc=50
            )
            
            # Should only return the low cloud cover item
            assert len(results) == 1
            assert results[0]["properties"]["eo:cloud_cover"] == 10
    
    def test_find_data_no_results(self):
        """Test find_data when no data is found."""
        dc = DataConnector(connector_type=self.connector_type)
        
        with patch.object(dc.connector.client, 'search') as mock_search:
            mock_search.return_value.items.return_value = []
            
            unique_dates, results = dc.connector.find_data(
                data_collection_name=self.collection,
                date_start="2024-01-01",
                date_end="2024-01-31",
                bbox=(-122.5, 37.7, -122.3, 37.9)
            )
            
            assert unique_dates is None
            assert results is None
    
    @pytest.mark.parametrize("start_date,end_date,bbox", [
        ("2024-01-01", "2024-01-05", (-122.5, 37.7, -122.3, 37.9)),
    ])
    def test_get_data_mock(self, start_date, end_date, bbox, tmp_path):
        """Test get_data with mocked STAC search and stackstac."""
        dc = DataConnector(connector_type=self.connector_type)
        save_file = str(tmp_path / "test_output.tif")
        
        # Mock find_data
        mock_item_dict = {
            "id": "test-item",
            "type": "Feature",
            "stac_version": "1.0.0",
            "properties": {
                "datetime": "2024-01-15T10:00:00Z",
                "eo:cloud_cover": 10
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[-122.5, 37.7], [-122.3, 37.7], [-122.3, 37.9], [-122.5, 37.9], [-122.5, 37.7]]]
            },
            "assets": {
                "B04": {"href": "https://example.com/B04.tif"},
                "B03": {"href": "https://example.com/B03.tif"},
                "B02": {"href": "https://example.com/B02.tif"}
            }
        }
        
        with patch.object(dc.connector, 'find_data') as mock_find:
            mock_find.return_value = (["2024-01-15"], [mock_item_dict])
            
            # Mock stackstac.stack
            mock_data = xr.DataArray(
                data=[[[1, 2], [3, 4]]] * 3,  # 3 bands
                dims=["band", "y", "x"],
                coords={
                    "band": self.bands,
                    "y": [37.9, 37.7],
                    "x": [-122.5, -122.3]
                }
            )
            mock_data = mock_data.expand_dims({"time": [datetime(2024, 1, 15)]})
            
            with patch('terrakit.download.data_connectors.planetary_computer.stackstac.stack') as mock_stack:
                mock_stack.return_value.compute.return_value = mock_data
                
                with patch('terrakit.download.data_connectors.planetary_computer.save_data_array_to_file'):
                    data = dc.connector.get_data(
                        data_collection_name=self.collection,
                        date_start=start_date,
                        date_end=end_date,
                        bbox=bbox,
                        bands=self.bands,
                        maxcc=50,
                        save_file=save_file
                    )
                    
                    assert data is not None
                    assert isinstance(data, xr.DataArray)
                    assert "time" in data.dims
                    assert "band" in data.dims or "band" in data.coords
    
    def test_get_data_no_results(self):
        """Test get_data when no data is found."""
        dc = DataConnector(connector_type=self.connector_type)
        
        with patch.object(dc.connector, 'find_data') as mock_find:
            mock_find.return_value = (None, None)
            
            data = dc.connector.get_data(
                data_collection_name=self.collection,
                date_start="2024-01-01",
                date_end="2024-01-31",
                bbox=(-122.5, 37.7, -122.3, 37.9)
            )
            
            assert data is None
    
    def test_validate_bbox_valid(self):
        """Test bbox validation with valid coordinates."""
        # Should not raise an exception
        dc = DataConnector(connector_type=self.connector_type)
        dc.connector._validate_bbox((-122.5, 37.7, -122.3, 37.9))
    
    def test_validate_bbox_invalid_longitude(self):
        """Test bbox validation with invalid longitude."""
        dc = DataConnector(connector_type=self.connector_type)
        
        with pytest.raises(TerrakitValueError):
            dc.connector._validate_bbox((-122.3, 37.7, -122.5, 37.9))  # West > East
    
    def test_validate_bbox_invalid_latitude(self):
        """Test bbox validation with invalid latitude."""
        dc = DataConnector(connector_type=self.connector_type)
        
        with pytest.raises(TerrakitValueError):
            dc.connector._validate_bbox((-122.5, 37.9, -122.3, 37.7))  # South > North
    
    def test_validate_dates_valid(self):
        """Test date validation with valid dates."""
        dc = DataConnector(connector_type=self.connector_type)
        # Should not raise an exception
        dc.connector._validate_dates("2024-01-01", "2024-01-31")
    
    def test_validate_dates_invalid(self):
        """Test date validation with invalid dates."""
        dc = DataConnector(connector_type=self.connector_type)
        
        with pytest.raises(TerrakitValueError):
            dc.connector._validate_dates("2024-01-31", "2024-01-01")

# Made with Bob
