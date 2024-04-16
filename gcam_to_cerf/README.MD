# GCAM to CERF



## Description

Documentation and scripts for data extraction, formatting, and calculation inputs to CERF from GCAM-USA

## Model Versions

|Model|Version|
|---------|------|
| GCAM-USA | v.5.3 |
| CERF     | v.2.3.2 |


**_NOTE:_**  The scripts provided in this repository are only applicable to the listed GCAM-USA to CERF versions together. Alternative model versions will require modification of queries and calculations.

## Data Requirements for CERF from GCAM-USA

For every:
* Vintage (Vint_{year})
* Year (x)
* Technology (class2)
* subregion (state)

CERF needs the following information:

|Source|Description|Output Variable Name|GCAM Query/File Path(s)|Original Units|Output Units|
|------------------|-------------------|--|--|--|--|
|GCAM output        |Technology Capacity by State   |elec_cap_usa_GW                |"elec capacity by tech and vintage"|EJ-Capacity|GW|
|GCAM output        |Technology Capacity Factor     |elec_capacity_factor_usa_in    |"elec investment capacity factor"  |Fraction|fraction|
|GCAM output        |Technology Heat Rate           |elec_heat_rate_BTUperkWh       |"elec coeff"                       |?|btu/kWh|
|GCAM output        |Technology Fuel Price          |elec_fuel_price_2015USDperMBTU |"prices by sector"                 |1975USD/GJ|$2015USD/MBTU|
|GCAM output        |Variable O&M                   |elec_variable_om_2015USDperMWh | "elec operating costs by tech and vintage"|1975USD/GJ|$2015USD/MWh|
|GCAM input         |Technology Lifetime            |elec_lifetime_yr               | `input/gcamdata/outputs/L2233.GlobalTechLifetime_elecS_cool_USA.csv`, `input/gcamdata/outputs/L2233.GlobalIntTechLifetime_elecS_cool_USA.csv`  |n_years|n_years|
|GCAM input         |Carbon Capture Rate            |elec_carbon_capture_rate_fraction| `input/gcamdata/outputs/L2233.GlobalTechCapture_elec_cool.csv`  |Fraction |Fraction|
|GCAM input         |Fuel CO2 Content               |elec_fuel_co2_content_tonsperMBTU| `input/gcamdata/outputs/L202.CarbonCoef.csv`, `input/gcamdata/outputs/L222.CarbonCoef_en_USA.csv`, `input/gcamdata/outputs/L2261.CarbonCoef_bio_USA` |kg carbon/GJ|tons/MBTU|
|Calculation        |% change in fuel price         |elec_fuel_price_escl_rate_fraction|N/A                             |N/A|N/A|
|Calculation        |% change in variable O&M       |elec_variable_om_escl_rate_fraction|N/A                            |N/A|N/A|

### Notes

* Any variables that are provided regionally or nationally need to be mapped to every state and technology type
* Any variables that are provided by a generic fuel category need to be mapped to each appropriate technology type
* If a variable does not vary across year then it still needs to appear in each year and vintage


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
| value          | value of specific param           | flt | 13239.413 |


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

### elec_cap_usa_GW

* **Description:** Electricity capacity by technology, vintage, subregion, and year. In the dispatch model, GCAM explicitly tracks capacity.  The "elec capacity by tech and vintage" query is this endogenous capacity.  Generation is a function of capacity within the model - this capacity is dispatched for each dispatch segment to determine generation. (Generation can't exceed capacity * hours in segment * segment capacity factor, but it can be less than that if the capacity is not fully utilized).  So technology capacity factors at each time step are dynamic, although there is a maximum they can't exceed. Previously, GCAM only tracked energy in the model, and used generation to back-calculate capacity. But that's no longer the case with the dispatch model.  Capacity is endogenous - calculated by the model and written to the database (albeit in 'EJ-capacity' units). The unit "EJ-capacity" is energy for the year at 100% capacity factor to represent capacity. This can be converted to other units by dividing by the number of hours in the year.
* **gcam query:** "elec capacity by tech and vintage"
* **Original units:** EJ-capacity
* **Output units:** GW
* **Calculation:**

        1 EJ = 277777.7777777778 GWh per year

        conv_EJ_to_GW = 277777.77777777778 / 8760

* **Example:**

$$ 10 GW * 8760 hours * (1 / 277777.77777777778)= .315 EJ  $$

.315 EJ is the EJ-capacity value in the gcam database. By reversing this equation we can determine the starting capacity.


### elec_capacity_factor_usa_in

* **Description:**  Capacity factor by technology, vintage, subregion, and year. GCAM tracks both investment level and actual capacity factor for each technology. The investment-level capacity factor is required for CERF.
* **gcam query:** "elec investment capacity factor"
* **Original units:** fraction
* **Output units:** fraction
* **Calculation:** N/A


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



### elec_variable_om_2015USDperMWh

* **Description:** Variable operations and maintenance costs by technology, vintage, year, and state. GCAM prices are provided in 1975 USD and need to be scaled to 2015 USD. Last, prices must be converted from dollars/GJ to dollars/MWh.
* **gcam query:** "elec operating costs by tech and vintage"
* **Original units:** 1975USD/GJ
* **Output units:** $2015USD/MWh
* **Calculation:**

        conv_GJ_to_MWh = 0.277777778

        elec_variable_om_2015USDperMWh = (value * gdp_deflator(2015,1975) / conv_GJ_to_MWh)



### elec_lifetime_yr               

* **Description:** Expected technology lifetime in years at investment. 
* **gcam exogenous file paths:** 
    * `input/gcamdata/outputs/L2233.GlobalTechLifetime_elecS_cool_USA.csv` 
    * `input/gcamdata/outputs/L2233.GlobalIntTechLifetime_elecS_cool_USA.csv`
* **Original units:** years
* **Output units:** years


### elec_carbon_capture_rate_fraction

* **Description:** Rate of carbon capture for technology.
* **gcam exogenous file path:**
    * `input/gcamdata/outputs/L2233.GlobalTechCapture_elec_cool.csv`
* **Original units:** fraction
* **Output units:** fraction


### elec_fuel_co2_content_tonsperMBTU

* **Description:** CO2 content for fuel type used in specific technology (e.g., natural gas). Values must be converted from kilograms of carbon per GJ to tons per MBTU.
* **gcam exogenous file path:** 
    * `input/gcamdata/outputs/L202.CarbonCoef.csv` 
    * `input/gcamdata/outputs/L222.CarbonCoef_en_USA.csv` 
    * `input/gcamdata/outputs/L2261.CarbonCoef_bio_USA`
* **Original units:** kg carbon / GJ
* **Output units:** tonsperMBTU
* **Calculation:**
        
        A = ATOMIC_WEIGHT_OF_CO2_PER_ATOMIC_WEIGHT_OF_CARBON = 44 / 12
        B = TONS_PER_KG = 1e-3
        C = MEGAWATT_HOURS_PER_GIGAJOULE = 0.277777778
        D = MEGA_BRITISH_THERMAL_UNITS_PER_MEGAWATT_HOUR = 3.412e6

        tons_per_MBTU = KG_carbon_per_GJ * A * B / (C * D)


### elec_fuel_price_escl_rate_fraction

* **Description:** The escalation rate of fuel prices for each technology between years. This is calculated directly from the fuel prices extracted previously.
* **Original units:** N/A
* **Output units:** fraction
* **Calculation:**

     Escalation Rate = (fuelprice_t - fuelprice_t-5) / fuelprice_t-5


### elec_variable_om_escl_rate_fraction

* **Description:** The escalation rate of variable O&M prices for each technology between years. This is calculated directly from the variable O&M prices extracted previously.
* **Original units:** N/A
* **Output units:** fraction
* **Calculation:**

     Escalation Rate = (VOM_t - VOM_t-5) / VOM_t-5


