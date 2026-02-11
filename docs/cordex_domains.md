// Copyright (c) 2026 Copyright 2024 IBM Corp
// 
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

## CORDEX Domain Reference

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

### Usage with TerraKit

When using CORDEX data with TerraKit's Climate Data Store connector, you can either:

1. **Specify a bounding box** - TerraKit will automatically map it to the appropriate CORDEX domain:
   ```python
   dc.connector.find_data(
       collection="projections-cordex-domains-single-levels",
       date_start="2020-01-01",
       date_end="2020-12-31",
       bbox=[-10, 35, 30, 60]  # Automatically mapped to EUR-44 or EUR-22
   )
   ```

2. **List available domains programmatically**:

    ```python
    domains = dc.connector.list_cordex_domains()
    ```