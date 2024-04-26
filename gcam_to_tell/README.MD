# GCAM to TELL

## Description

Documentation and scripts for data extraction, formatting, and calculating inputs to TELL from GCAM-USA.

## Model Versions

|Model|Version|
|:-:|:-:|
| GCAM-USA | v.5.3 |
| CERF | v.1.1 |
| gcamreader | v.1.4 |

**_NOTE:_** The scripts provided in this repository are only applicable to the listed GCAM-USA to TELL versions 
together. Alternative model versions will require modification of queries and calculations.

## Data Requirements for TELL from GCAM-USA

For every:
* Scenario (scenario)
* Year (x)
* State (region)

TELL needs the following information:

| Source | Description | Output Variable Name | GCAM Query/File Path(s) | Original Units | Output Units |
|:-:|:-:|:-:|:-:|:-:|:-:|
| GCAM Output | Annual Total Electricity Demand by State | energyFinalConsumBySecEJ | "TBD" | EJ | EJ |

## Notes
* The GCAM output database has total annual state-level electricity demand by sector. Either GCAM-reader or the 
associated python script should sum the sector-specific values to create a single total annual state-level electricity 
demand value for each year.
* We should double check the units. GCAM seems to simulate demand in EJ, but the output of the GCAM reader is in TWh. We
need to check to make sure the unit conversion is done correctly.

## Expected Output Data Format
A single .csv file is the expected format of this data extraction effort. The csv should contain (at a minimum) the 
following column names:

| Column | Description | dtype | Example |
|:-:|:-:|:-:|:-:|
| scenario  | Name of gcam scenario | str | rcp85cooler_ssp3         |
| region    | GCAM region name      | str | USA                      |
| subRegion | State acronym         | str | AL                       |
| param     | Parameter name        | str | elecFinalBySecTWh        |
| x         | Year                  | int | 1975                     |
| units     | Variable units        | str | Final Electricity by Sector (TWh) |
| value     | Annual state-level total electricity demand | flt | 0.510867688888893 |

