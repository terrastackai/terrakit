# TerraKit Data Connectors
Data connectors are classes which enable a user to search for data and query data from a particular data source using a common set of functions.  The TerraKit Pipeline makes use of the Data Connectors, but they can also be used independently to explore and retrieve EO data. 

Each data connector has the following mandatory methods:

* list_collections()
* find_data()
* get_data()


## Available collections
The following data connectors and associated collections are available:

| Connectors        | Collections |
| ----------------- | ----------- |
| sentinelhub       | s2_l1c, dem, s1_grd, hls_l30, s2_l2a, hls_s30 |
| nasa_earthdata    | HLSL30_2.0, HLSS30_2.0  |
| sentinel_aws      | sentinel-2-l2a  |
| climate_data_store| derived-era5-single-levels-daily-statistics, projections-cordex-domains-single-levels |
| IBMResearchSTAC | 'HLSS30', 'esa-sentinel-2A-msil1c', 'HLS_S30',, 'atmospheric-weather-era5', 'deforestation-umd', 'Radar-10min', 'tasmax-rcp85-land-cpm-uk-2.2km', 'vector-osm-power', 'ukcp18-land-cpm-uk-2.2km', 'treecovermaps-eudr', 'ch4' + more|
| TheWeatherCompany | weathercompany-daily-forecast |

## Data connector access
Each data connector has a different access requirements. For example, connecting to SentinelHub and NASA EarthData, you will need to obtain credentials from each provider. Once these have been obtained, they can be added to a `.env` file at the root directory level using the following syntax:

```.env
SH_CLIENT_ID="<SentinelHub Client ID>"
SH_CLIENT_SECRET="<SentinelHub Client Secret>"
NASA_EARTH_BEARER_TOKEN="<NASA EarthData Bearer Token>"
CDSAPI_KEY="<Climate Data Store API Key>"
```

### NASA Earthdata
To access NASA Earthdata, register for an Earthdata Login profile and requests a bearer token. [https://urs.earthdata.nasa.gov/profile](https://urs.earthdata.nasa.gov/profile)

### Sentinel Hub
To access sentinel hub, register for an account and requests an OAuth client using the Sentinel Hub dashboard [https://www.planet.com](https://www.planet.com)

### Sentinel AWS
Access sentinel AWS data is open and does not require any credentials.

### Climate Data Store
Create an account at [https://cds.climate.copernicus.eu/](https://cds.climate.copernicus.eu/). Once created, find your API  key under the `Profile` section. Each dataset may also require accepting the licence agreement. If this is the case, the first time a request is made, an error will be returned with the url to visit to accept the terms.

Available collections include:
- [ERA5 post-processed daily statistics on single levels from 1940 to present](https://cds.climate.copernicus.eu/datasets/derived-era5-single-levels-daily-statistics?tab=overview)
- [CORDEX regional climate model data on single levels](https://cds.climate.copernicus.eu/datasets/projections-cordex-domains-single-levels?tab=overview)

### The Weather Company
To access The Weather Company, register for an account and requests an API Key [https://www.weathercompany.com/weather-data-apis/](https://www.weathercompany.com/weather-data-apis/). Once you have an API key, set the following environment variable:

```
THE_WEATHER_COMPANY_API_KEY="<The Weather Company API key>"
```

### IBM Research STAC
Access IBM Research STAC is currently restricted to IBMers and partners. If you're elegible, you need to register for an IBM AppID account and set the following environment variables:

```
APPID_ISSUER=<issuer>
APPID_USERNAME=<user-email>
APPID_PASSWORD=<user-password>
CLIENT_ID=<client-id>
CLIENT_SECRET=<client-secret>
```

Please reach out the maintainers of this repo.

IBMers don't need credentials to access the internal instance of the STAC service.

## Try out
Data Connectors can be used outside the TerraKit Pipeline. Take a look at the [TerraKit: Easy geospatial data search and query](examples/terrakit_download.ipynb) notebook for more help getting started with TerraKit Data Connectors.