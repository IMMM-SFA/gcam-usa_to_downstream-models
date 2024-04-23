# GCAM to TELL

## Description

Documentation and scripts for data extraction, formatting, and calculating inputs to TELL from GCAM-USA.

## Model Versions

|Model|Version|
|:-:|:-:|
| GCAM-USA | v.5.3 |
| CERF | v.1.1 |
| gcamreader | v1.4 |

**_NOTE:_**  The scripts provided in this repository are only applicable to the listed GCAM-USA to TELL versions 
together. Alternative model versions will require modification of queries and calculations.

## Data Requirements for TELL from GCAM-USA

For every:
* Scenario (scenario)
* Year (x)
* State (region)

TELL needs the following information:

| Source | Description | Output Variable Name | GCAM Query/File Path(s) | Original Units | Output Units |
|:-:|:-:|:-:|:-:|:-:|:-:|
| GCAM Output | Annual Total Electricity Consumption by State and Sector | energyFinalConsumBySecEJ | "TBD" | EJ | EJ |


### Notes

* TBD


## Expected Output Data Format

A single .csv file is the expected format of this data extraction effort. The csv should contain a heavily nested data structure with the following column names:


|column|description|dtype| example |
|----------------|--------------------------|-----|-------------------|
| scenario       | name of gcam scenario             | str | rcp85cooler_ssp3          |
| region         | gcam region name                  | str | USA                        |
| subRegion      | state acronym                     | str | AL                         |
| param          | parameter name                    | str | elec_heat_rate_BTUperkWh   |



## Output Variable Details

### elec_cap_usa_GW

* **Description:** Electricity capacity by technology, vintage, subregion, and year. In the dispatch model, GCAM explicitly tracks capacity.  The "elec capacity by tech and vintage" query is this endogenous capacity.  Generation is a function of capacity within the model - this capacity is dispatched for each dispatch segment to determine generation. (Generation can't exceed capacity * hours in segment * segment capacity factor, but it can be less than that if the capacity is not fully utilized).  So technology capacity factors at each time step are dynamic, although there is a maximum they can't exceed. Previously, GCAM only tracked energy in the model, and used generation to back-calculate capacity. But that's no longer the case with the dispatch model.  Capacity is endogenous - calculated by the model and written to the database (albeit in 'EJ-capacity' units). The unit "EJ-capacity" is energy for the year at 100% capacity factor to represent capacity. This can be converted to other units by dividing by the number of hours in the year.
* **gcam query:** "elec capacity by tech and vintage"
* **Original units:** EJ-capacity
* **Output units:** GW
* **Calculation:**

        1 EJ = 277777.7777777778 GWh per year

        conv_EJ_to_GW = 277777.77777777778 / 8760



