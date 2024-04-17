# GCAM to GO



## Description

Documentation and scripts for data extraction, formatting, and calculation inputs to GO from GCAM-USA

## Model Versions

|Model|Version|
|---------|------|
| GCAM-USA | v.5.3 |
| GO       | dev |


**_NOTE:_**  The scripts provided in this repository are only applicable to the listed GCAM-USA to GO versions together. Alternative model versions may require modification of queries and calculations.

## Data Requirements for GO from GCAM-USA

For every:
* Scenario 
* Vintage (Vint_{year})
* Year (x)
* Technology (class2)
* subregion (state)

GO needs the following information:

|Source|Description|Output Variable Name|GCAM Query/File Path(s)|Original Units|Output Units|
|------------------|-------------------|--|--|--|--|
|GCAM output        |Technology Heat Rate           |elec_heat_rate_BTUperkWh       |"elec coeff"                       |?|btu/kWh|
|GCAM output        |Technology Fuel Price          |elec_fuel_price_2015USDperMBTU |"prices by sector"                 |1975USD/GJ|$2015USD/MBTU|


## Expected Output Data Format

A single .csv file is the expected format of this data extraction effort. The csv should contain a heavily nested data structure with the following column names:


|column|description|dtype| example |
|----------------|--------------------------|-----|-------------------|
| scenario       | name of gcam scenario             | str | rcp85cooler_ssp3          |
| region         | gcam region name                  | str | USA                        |
| subRegion      | state acronym                     | str | AL                         |
| param          | parameter name                    | str | elec_heat_rate_BTUperkWh   |
| classLabel1    | describes class1 variable         | str | subsector                |
| class1         | tech category                     | str | biomass                |
| classLabel2    | describes class2 variable         | str | technology                |
| class2         | full tech name with cooling type  | str | biomass (conv) (cooling pond) |
| xlabel         | describes x variable              | str | Year |
| x              | year of parameter value           | int | 2025 |
| vintage        | gcam simulation year of parameter | str | Vint_2020 |
| units          | units of parameter value          | str | Heat Rate (BTU per kWh) |
| value          | value of specific param           | flt | 3412 |

## GCAM Regions to States

Some parameters in GCAM are provided on a electricity region basis.

* Alaska grid: AK
* California grid: CA
* Central East grid: IN, KY, MI, OH, WV
* Central Northeast grid: IL, MO, "WI
* Central Northwest grid: IA, MN, ND, NE, SD
* Central Southwest grid: KS, OK
* Florida grid: FL
* Hawaii grid: HI
* Mid-Atlantic grid: DC, DE, MD, NJ, PA
* New England grid: CT, MA, ME, NH, RI, VT
* New York grid: NY
* Northwest grid: ID, MT, NV, OR, UT, WA
* Southeast grid: AL, AR, GA, LA, MS, NC, SC, TN, VA
* Southwest grid: AZ, CO, NM, WY
* Texas grid: TX

## GDP deflator

Allows for conversion between price years of GCAM parameters. Equation to convert between a base year USD to a desired year USD based on a deflator range that starts in a starting year:

$$(year_{desired} - year_{start}) /(year_{base} - year_{start})  $$

This time series is the BEA "A191RD3A086NBEA" product from https://fred.stlouisfed.org/series/A191RD3A086NBEA

    ```
    def deflate_gdp(year, base_year):
        start_year = 1929
        factors = np.array([
            9.896, 9.535, 8.555, 7.553, 7.345, 7.749, 7.908, 8.001, 8.347,
            8.109, 8.033, 8.131, 8.68, 9.369, 9.795, 10.027, 10.288, 11.618,
            12.887, 13.605, 13.581, 13.745, 14.716, 14.972, 15.157, 15.298,
            15.559, 16.091, 16.625, 17.001, 17.237, 17.476, 17.669, 17.886,
            18.088, 18.366, 18.702, 19.227, 19.786, 20.627, 21.642, 22.784,
            23.941, 24.978, 26.337, 28.703, 31.361, 33.083, 35.135, 37.602,
            40.706, 44.377, 48.52, 51.53, 53.565, 55.466, 57.24, 58.395,
            59.885, 61.982, 64.392, 66.773, 68.996, 70.569, 72.248, 73.785,
            75.324, 76.699, 78.012, 78.859, 80.065, 81.887, 83.754, 85.039,
            86.735, 89.12, 91.988, 94.814, 97.337, 99.246, 100, 101.221,
            103.311, 105.214, 106.913, 108.828, 109.998, 111.445, 113.545,
            116.311, 118.339
        ])
        return factors[year - start_year] / factors[base_year - start_year]
    ```




## Output Variable Details

### elec_heat_rate_BTUperkWh

* **Description:** Heat rate by technology, vintage, subregion, and year. Solar and wind heat rates should have a value of 0.
* **gcam query:** "elec coeff"    
* **Original units:** 
* **Output units:** btu/kWh
* **Calculation:** 



### elec_fuel_price_2015USDperMBTU

* **Description:** Fuel price for natural gas, oil, coal, and biomass technologies by vintage and region. Biomass ("regional biomass") is provided at the regional level, fossil fuels ("refined liquids industrial", "regional coal", and "wholesale gas") are provided at the grid region level. These each need to be mapped to states and to all appropriate technologies. GCAM prices are provided in 1975 USD and need to be scaled to 2015 USD. Last, prices must be converted from $/GJ to $/MBTU. Unit conversion from https://www.unitconverters.net/energy/gigajoule-to-mega-btu-it.htm
* **gcam query:** "prices by sector"
* **Original units:** 1975USD/GJ
* **Output units:** $2015USD/MBTU
* **Calculation:**

        1 MBTU = 10e6 BTU 
        conv_GJ_to_MBTU = 0.947804

        elec_fuel_price_2015USDperMBTU = (value * gdp_deflator(2015,1975) / conv_GJ_to_MBTU)

