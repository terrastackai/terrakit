## CORDEX Regional Climate Model Data on Single Levels - Domain Reference

Full details of dataset: https://cds.climate.copernicus.eu/datasets/projections-cordex-domains-single-levels?tab=overview

CORDEX (Coordinated Regional Climate Downscaling Experiment) defines specific regional domains for climate projections. Each domain covers a geographic region at one or more resolutions.

### Available CORDEX Domains

| Domain Code | Region Name | Bounding Box (min_lon, min_lat, max_lon, max_lat) | Resolution | Approx. Grid Spacing |
|-------------|-------------|---------------------------------------------------|------------|---------------------|
| **AFR-44** | Africa | -24.64, -45.76, 60.28, 42.24 | 0.44° | ~50 km |
| **AFR-22** | Africa | -24.64, -45.76, 60.28, 42.24 | 0.22° | ~25 km |
| **ANT-44** | Antarctica | -180.0, -89.5, 180.0, -60.0 | 0.44° | ~50 km |
| **ARC-44** | Arctic | -180.0, 60.0, 180.0, 90.0 | 0.44° | ~50 km |
| **AUS-44** | Australasia | 89.5, -52.36, 179.99, 12.21 | 0.44° | ~50 km |
| **AUS-22** | Australasia | 89.5, -52.36, 179.99, 12.21 | 0.22° | ~25 km |
| **CAM-44** | Central America | -122.0, -19.76, -59.52, 34.24 | 0.44° | ~50 km |
| **CAM-22** | Central America | -122.0, -19.76, -59.52, 34.24 | 0.22° | ~25 km |
| **CAS-44** | Central Asia | 34.0, 18.0, 115.0, 70.0 | 0.44° | ~50 km |
| **CAS-22** | Central Asia | 34.0, 18.0, 115.0, 70.0 | 0.22° | ~25 km |
| **EAS-44** | East Asia | 65.0, -15.0, 155.0, 65.0 | 0.44° | ~50 km |
| **EAS-22** | East Asia | 65.0, -15.0, 155.0, 65.0 | 0.22° | ~25 km |
| **EUR-44** | Europe | -44.0, 22.0, 65.0, 72.0 | 0.44° | ~50 km |
| **EUR-22** | Europe | -44.0, 22.0, 65.0, 72.0 | 0.22° | ~25 km |
| **EUR-11** | Europe | -44.0, 22.0, 65.0, 72.0 | 0.11° | ~12.5 km |
| **MED-44** | Mediterranean | -10.0, 30.0, 40.0, 48.0 | 0.44° | ~50 km |
| **MED-22** | Mediterranean | -10.0, 30.0, 40.0, 48.0 | 0.22° | ~25 km |
| **MNA-44** | Middle East & North Africa | -25.0, 0.0, 75.0, 50.0 | 0.44° | ~50 km |
| **MNA-22** | Middle East & North Africa | -25.0, 0.0, 75.0, 50.0 | 0.22° | ~25 km |
| **NAM-44** | North America | -172.0, 12.0, -35.0, 76.0 | 0.44° | ~50 km |
| **NAM-22** | North America | -172.0, 12.0, -35.0, 76.0 | 0.22° | ~25 km |
| **SAM-44** | South America | -93.0, -56.0, -25.0, 18.0 | 0.44° | ~50 km |
| **SAM-22** | South America | -93.0, -56.0, -25.0, 18.0 | 0.22° | ~25 km |
| **WAS-44** | South Asia (West Asia) | 20.0, -15.0, 115.0, 45.0 | 0.44° | ~50 km |
| **WAS-22** | South Asia (West Asia) | 20.0, -15.0, 115.0, 45.0 | 0.22° | ~25 km |
| **SEA-44** | Southeast Asia | 89.0, -15.0, 146.0, 27.0 | 0.44° | ~50 km |
| **SEA-22** | Southeast Asia | 89.0, -15.0, 146.0, 27.0 | 0.22° | ~25 km |

### Domain Resolution Notes

- **0.44°** (~50 km): Standard resolution, available for all domains
- **0.22°** (~25 km): High resolution, available for most domains
- **0.11°** (~12.5 km): Very high resolution, currently only available for Europe (EUR-11)

## Query Parameters

The CDS connector supports additional query parameters to customize CORDEX data retrieval. These parameters can be passed via the `query_params` dictionary in the `get_data()` method.

### Default Values

When downloading CORDEX data, the following default parameters are automatically applied:

| Parameter | Default Value | Description |
|-----------|---------------|-------------|
| `experiment` | `"historical"` | Type of CORDEX experiment |
| `horizontal_resolution` | `"0_44_degree_x_0_44_degree"` | Spatial resolution (~50 km) |
| `temporal_resolution` | `"daily_mean"` | Temporal aggregation |
| `ensemble_member` | `"r1i1p1"` | Ensemble member identifier |
| `format` | `"netcdf"` | Output file format |
| `start_year` | Derived from `date_start` | Start year for data |
| `end_year` | Derived from `date_end` | End year for data |

### Valid Parameter Values

#### `experiment`
Type of CORDEX experiment:

- `"historical"` (default) - Historical period (typically 1950-2005)
- `"evaluation"` - Evaluation runs driven by ERA-Interim reanalysis (typically 1980-2010)
- `"rcp_2_6"` - RCP 2.6 scenario (low emissions)
- `"rcp_4_5"` - RCP 4.5 scenario (medium emissions)
- `"rcp_8_5"` - RCP 8.5 scenario (high emissions)

#### `horizontal_resolution`
Spatial resolution of the model grid:

- `"0_44_degree_x_0_44_degree"` (default) - ~50 km grid spacing
- `"0_22_degree_x_0_22_degree"` - ~25 km grid spacing (high resolution)
- `"0_11_degree_x_0_11_degree"` - ~12.5 km grid spacing (very high resolution, EUR-11 only)

#### `temporal_resolution`
Temporal aggregation of the data:

- `"daily_mean"` (default) - Daily mean values
- `"3_hourly"` - 3-hour intervals
- `"6_hourly"` - 6-hour intervals
- `"monthly_mean"` - Monthly mean values
- `"fixed"` - Time-invariant fields (e.g., orography, land mask)

#### `ensemble_member`
Ensemble member identifier (format: rXiYpZ):

- `"r1i1p1"` (default) - First realization, first initialization, first physics
- `"r0i0p0"` - Used for evaluation runs
- `"r12i1p1"`, `"r2i1p1"`, `"r3i1p1"` - Additional ensemble members
- **r** = realization (different initial conditions)
- **i** = initialization method
- **p** = physics parameterization

#### `gcm_model`
Global Climate Model providing boundary conditions (examples):

- `"mohc_hadgem2_es"` - Met Office Hadley Centre HadGEM2-ES
- `"mpi_m_mpi_esm_lr"` - Max Planck Institute MPI-ESM-LR
- `"ichec_ec_earth"` - EC-EARTH consortium model
- `"cnrm_cerfacs_cm5"` - CNRM-CERFACS CNRM-CM5
- `"ncc_noresm1_m"` - Norwegian Climate Centre NorESM1-M
- `"era_interim"` - ERA-Interim reanalysis (for evaluation runs)
- Many others available

#### `rcm_model`
Regional Climate Model (examples):

- `"clmcom_clm_cclm4_8_17"` - CLM-Community CCLM4-8-17
- `"smhi_rca4"` - SMHI RCA4
- `"knmi_racmo22t"` - KNMI RACMO22T
- `"gerics_remo2009"` - GERICS REMO2009
- `"dmi_hirham5"` - DMI HIRHAM5
- Many others available

## Usage Examples

### Basic Usage - Download with Defaults

```python
from terrakit import DataConnector

# Initialize CDS connector
dc = DataConnector(connector_type="climate_data_store")

# Download CORDEX data with default parameters
# (historical experiment, 0.44° resolution, daily_mean, r1i1p1 ensemble)
data = dc.connector.get_data(
    data_collection_name="projections-cordex-domains-single-levels",
    date_start="2000-01-01",
    date_end="2000-12-31",
    bbox=[-10, 35, 30, 60],  # Europe region (automatically mapped to EUR-44)
    bands=["2m_air_temperature", "mean_precipitation_flux"]
)

print(f"Data shape: {data.shape}")
print(f"Dimensions: {data.dims}")
```

### Override Default Parameters with query_params

```python
from terrakit import DataConnector

dc = DataConnector(connector_type="climate_data_store")

# Example 1: Get RCP 8.5 scenario data (future projections)
data_rcp85 = dc.connector.get_data(
    data_collection_name="projections-cordex-domains-single-levels",
    date_start="2050-01-01",
    date_end="2050-12-31",
    bbox=[-10, 35, 30, 60],
    bands=["2m_air_temperature"],
    query_params={"experiment": "rcp_8_5"}
)

# Example 2: Get high-resolution data (0.22°)
data_hires = dc.connector.get_data(
    data_collection_name="projections-cordex-domains-single-levels",
    date_start="2000-01-01",
    date_end="2000-12-31",
    bbox=[-10, 35, 30, 60],
    bands=["2m_air_temperature"],
    query_params={"horizontal_resolution": "0_22_degree_x_0_22_degree"}
)

# Example 3: Get 3-hourly data instead of daily mean
data_3hourly = dc.connector.get_data(
    data_collection_name="projections-cordex-domains-single-levels",
    date_start="2000-01-01",
    date_end="2000-01-07",
    bbox=[-10, 35, 30, 60],
    bands=["10m_wind_speed"],
    query_params={"temporal_resolution": "3_hourly"}
)

# Example 4: Specify GCM and RCM models
data_custom_models = dc.connector.get_data(
    data_collection_name="projections-cordex-domains-single-levels",
    date_start="2000-01-01",
    date_end="2000-12-31",
    bbox=[-10, 35, 30, 60],
    bands=["2m_air_temperature", "mean_precipitation_flux"],
    query_params={
        "gcm_model": "mohc_hadgem2_es",
        "rcm_model": "knmi_racmo22t"
    }
)

# Example 5: Multiple parameter overrides for future scenario
data_future = dc.connector.get_data(
    data_collection_name="projections-cordex-domains-single-levels",
    date_start="2080-01-01",
    date_end="2080-12-31",
    bbox=[-10, 35, 30, 60],
    bands=["2m_air_temperature", "mean_precipitation_flux"],
    query_params={
        "experiment": "rcp_4_5",
        "horizontal_resolution": "0_22_degree_x_0_22_degree",
        "temporal_resolution": "monthly_mean",
        "ensemble_member": "r12i1p1",
        "gcm_model": "ichec_ec_earth",
        "rcm_model": "smhi_rca4"
    }
)
```

### Working with Different CORDEX Domains

```python
from terrakit import DataConnector

dc = DataConnector(connector_type="climate_data_store")

# List all available CORDEX domains
domains = dc.connector.list_cordex_domains()
print(f"Available domains: {list(domains.keys())}")

# Get information for a specific domain
eur_info = dc.connector.get_cordex_domain_info("EUR-44")
print(f"Europe domain: {eur_info}")

# Download data for different regions
# North America
data_nam = dc.connector.get_data(
    data_collection_name="projections-cordex-domains-single-levels",
    date_start="2000-01-01",
    date_end="2000-12-31",
    bbox=[-120, 25, -70, 50],  # Automatically mapped to NAM-44
    bands=["2m_air_temperature"]
)

# Africa
data_afr = dc.connector.get_data(
    data_collection_name="projections-cordex-domains-single-levels",
    date_start="2000-01-01",
    date_end="2000-12-31",
    bbox=[10, -10, 40, 20],  # Automatically mapped to AFR-44
    bands=["2m_air_temperature"]
)
```

### Comparing Historical and Future Scenarios

```python
from terrakit import DataConnector

dc = DataConnector(connector_type="climate_data_store")

# Get historical baseline (1980-2010)
data_historical = dc.connector.get_data(
    data_collection_name="projections-cordex-domains-single-levels",
    date_start="1990-01-01",
    date_end="1990-12-31",
    bbox=[-10, 35, 30, 60],
    bands=["2m_air_temperature"],
    query_params={"experiment": "historical"}
)

# Get future projection under RCP 8.5 (2070-2100)
data_future_rcp85 = dc.connector.get_data(
    data_collection_name="projections-cordex-domains-single-levels",
    date_start="2090-01-01",
    date_end="2090-12-31",
    bbox=[-10, 35, 30, 60],
    bands=["2m_air_temperature"],
    query_params={"experiment": "rcp_8_5"}
)

# Compare the two periods
print(f"Historical mean: {data_historical.mean().values}")
print(f"Future RCP 8.5 mean: {data_future_rcp85.mean().values}")
print(f"Temperature change: {data_future_rcp85.mean().values - data_historical.mean().values} K")
```

## Notes

- **Domain Selection**: TerraKit automatically maps your bounding box to the appropriate CORDEX domain
- **Model Availability**: Not all GCM-RCM combinations are available for all domains, experiments, and time periods
- **Resolution**: Higher resolutions (0.22°, 0.11°) may not be available for all domains
- **Time Periods**:
  - Historical: typically 1950-2005
  - Evaluation: typically 1980-2010
  - Scenarios (RCP): typically 2006-2100
- **Ensemble Members**: Different ensemble members represent uncertainty in initial conditions and model physics

## See Also

- [CORDEX regional climate model data on single levels](https://cds.climate.copernicus.eu/datasets/projections-cordex-domains-single-levels?tab=overview) - further details on CORDEX  regional climate model data on single levels dataset.
- [ERA5 Parameters Guide](../../era5_bands_table.md) - Parameters for ERA5 reanalysis data
- [Climate Data Store Documentation](../climate_data_store.md) - Full CDS connector API reference