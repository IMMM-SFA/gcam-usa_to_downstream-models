# GCAM to CERF



## Description

Documentation and scripts for data extraction, formatting, and calculation inputs to CERF from GCAM-USA

## Model Versions

**_NOTE:_**  The scripts provided in this repository are only applicable to the listed GCAM-USA to CERF versions together. Alternative model versions will require modification of queries and calculations.

|Model|Version|
|---------|------|
| GCAM-USA | v.5.3 |
| CERF     | v.2.3.2 |


## Data Requirements for CERF from GCAM-USA

CERF requires the following variables:

| CERF Variable | CERF Variable Name | Description | CERF Required Units |
|---------------|--------------------|-------------|---------------------|
| Annual capacity by tech| elec_capacity_usa_GW | Annual capacity installed by technology (inclusive of cooling type), state, vintage, year, and scenario | GW | | |
| Capacity factor by tech | elec_capacity_factor_usa_in | Investment-level capacity factor by technology (inclusive of cooling type), state, vintage, and scenario | fraction | | |
| Heat rate by tech | elec_heat_rate_BTUperkWh | Heat rate by technology (inclusive of cooling type), state, vintage, and scenario | btu/kWh | | |
| Fuel price by tech | elec_fuel_price_2015USDperMBTU | Fuel price by technology (inclusive of cooling type), state, vintage,and scenario | $2015USD/MBTU | | |
| Variable O&M by tech | elec_variable_om_2015USDperMWh | Variable O&M cost by technology (inclusive of cooling type), state, vintage, and scenario | $2015USD/MWh | | |
|Investment Level Technology Lifetime            |elec_lifetime_yr               | Technology lifetime by technology (inclusive of cooling type), state, vintage, and scenario |n_years|
|Carbon Capture Rate by tech          |elec_carbon_capture_rate_fraction| Carbon capture rate by technology (inclusive of cooling type), state, vintage, and scenario  |Fraction |
|Fuel CO2 Content by tech         | elec_fuel_co2_content_tonsperMBTU| Fuel CO2 content by technology (inclusive of cooling type), state, vintage, and scenario  |tons/MBTU|
|% change in fuel price         |elec_fuel_price_escl_rate_fraction| Percent change in fuel price between years | Fraction | 
|% change in variable O&M       |elec_variable_om_escl_rate_fraction|Percent change in variable O&M cost between years |Fraction|

### Notes

* Any variables that are provided regionally or nationally need to be mapped to every state and technology type
* Any variables that are provided by a generic fuel category need to be mapped to each appropriate technology type


## Expected Output Data Format

A single .csv file is the expected format of this data extraction effort. The csv should contain a heavily nested data structure with the following column names:


|column|description|dtype| example |
|----------------|--------------------------|-----|-------------------|
| scenario       | name of gcam scenario             | str | rcp85cooler_ssp3          |
| region         | gcam region name                  | str | USA                        |
| subRegion      | state acronym                     | str | AL                         |
| param          | parameter name                    | str | elec_heat_rate_BTUperkWh   |
| classLabel2    | describes class2 variable         | str | technology                |
| class2         | full tech name with cooling type  | str | biomass (conv) (cooling pond) |
| xlabel         | describes x variable              | str | Year |
| x              | year of parameter value           | int | 2025 |
| vintage        | gcam simulation year of parameter | str | Vint_2020 |
| units          | units of parameter value          | str | Heat Rate (BTU per kWh) |
| value          | value of specific param           | flt | 13239.413 |


## GCAM Regions to States

Some parameters in GCAM are provided on a electricity region basis and need to be mapped to the appropriate states.

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

Some GCAM-USA variables are provided in 1975$ and need to be converted to 2015$. The function below allows for conversion between price years of GCAM parameters. The following equation converts between a base year USD to a desired year USD based on a deflator range that starts in a starting year:

$$(year_{desired} - year_{start}) /(year_{base} - year_{start})  $$

This time series is the BEA "A191RD3A086NBEA" product from https://fred.stlouisfed.org/series/A191RD3A086NBEA

    ```python
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

### 1. Capacity by Technology Type
Electricity capacity by technology (inclusive of cooling type), vintage, subregion, and year. Note that new projected GCAM-USA capacity additions begin in vintage 2020. All pre-existing capacity (i.e., anything installed prior to GCAM-USA simulations) is redeployed as part of the 2015 vintage. Vintages <= 2015 should not be interpreted the same way as vintages >= 2020

#### CERF Variable Name: 
* elec_cap_usa_GW
#### CERF required units:
* Gigawatts (GW)
#### GCAM-USA Query: 
* "elec capacity by tech and vintage"
#### Calculations & Transformations Required:
 In the dispatch model, GCAM explicitly tracks capacity. The "elec capacity by tech and vintage" query is this endogenous capacity. Generation is a function of capacity within the model - this capacity is dispatched for each dispatch segment to determine generation. (Generation can't exceed capacity * hours in segment * segment capacity factor, but it can be less than that if the capacity is not fully utilized). So technology capacity factors at each time step are dynamic, although there is a maximum they can't exceed. 
 
 Previously, GCAM only tracked energy in the model, and used generation to back-calculate capacity. But that's no longer the case with the dispatch model. Capacity is endogenous - calculated by the model and written to the database (albeit in 'EJ-capacity' units). The unit "EJ-capacity" is energy for the year at 100% capacity factor to represent capacity. This can be converted to other units by dividing by the number of hours in the year.

 **Unit Conversion:** 

        1 EJ = 277777.7777777778 GWh per year

         conv_EJ_to_GW = 277777.77777777778 / 8760


<br>

_____________

<br>

### 2. Capacity Factor
Capacity factor by technology and subregion for any vintage that has capacity investment for that technology and subregion combination. Capacity factor represents the annual power generated divided by the potential output if the plant operated at its rated capacity for a year. The required capacity factor represents the investment-level expected capacity factor, not the actual realized capacity factor from actual generation output.
#### CERF Variable Name: 
* elec_capacity_factor_usa_in
#### CERF required units:
* fraction
#### GCAM-USA Query: 
* "elec investment capacity factor"
#### Calculations & Transformations Required:
 **Unit Conversion:** 

None

 **Transformations:**

Capacity factors are provided at the investment-segment level. The output capacity factor for CERF takes the average value across all investment segments for each technology and subregion combination.

<br>

_____________

<br>

### 3. Heat Rate by Technology 
Heat rate by technology, vintage, subregion, and year. Heat rate is the amount of energy (BTU) used by a power plant to generate one kilowatt-hour of electricity. Solar, wind, and geothermal heat rates should have a value of 0. Heat rates should be collected for the following fuel types: 

* nuclearFuelGenII
* nuclearFuelGenIII
* refined liquids industrial
* regional biomass
* regional coal
* wholesale gas
#### CERF Variable Name: 
* elec_heat_rate_BTUperkWh
#### CERF required units:
* BTU/kWh
#### GCAM-USA Queries: 
* 'elec coeff'
#### Calculations & Transformations Required:
GCAM-USA reports heat rates in EJ in / EJ out ratios. These need to be converted to BTU in / kWh out.

 **Unit Conversion:** 

        BRITISH_THERMAL_UNITS_PER_EXAJOULE = 9.48e14
        KWH_PER_EXAJOULE = 2.77778e11

        Example:

        1.2 EJ/EJ = (1.2 * BRITISH_THERMAL_UNITS_PER_EXAJOULE) / KWH_PER_EXAJOULE

<br>

_____________

<br>

### 4. Fuel Price by Technology
Fuel price for natural gas, oil, coal, nuclear, and biomass for gas-, oil-, coal-, and biomass- fired technologies by region and vintage. 
#### CERF Variable Name: 
* elec_fuel_price_2015USDperMBTU
#### CERF required units:
* btu/kWh
#### GCAM-USA Queries: 
* 'prices of all markets'

#### Calculations & Transformations Required:
Biomass ("regional biomass") is provided at the state level, fossil fuels ("refined liquids industrial", "regional coal", and "wholesale gas") are provided at the grid region level, and nuclear fuels (nuclearFuelGenII, nuclearFuelGenIII) are provided at the aggregate US level. These each need to be mapped to states and to all appropriate technologies. 

Prices must be converted from dollars/GJ to dollars/MMBTU (million BTUs). Unit conversion from https://www.unitconverters.net/energy/gigajoule-to-mega-btu-it.htm. GCAM prices are provided in 1975 USD and need to be scaled to 2015 USD. 
**Unit Conversion** 
        
        1 MMBTU = 10e6 BTU 
        1 GJ = .947817 MMBTU
        1 MMBTU = (1 / .947817) GJ
        conv_GJ_to_MMBTU = 1.0550559

        elec_fuel_price_1975USDperMBTU = elec_fuel_price_1975USDperGJ * conv_GJ_to_MMBTU

**1975 to 2015 Dollar Year Conversion** 

        elec_fuel_price_2015USDperMBTU = (elec_fuel_price_1975USDperMBTU * gdp_deflator(2015,1975))


<br>

_____________

<br>

### 5. Variable O&M Cost by Technology
Variable operations and maintenance costs by technology, vintage, year, and state. Variable O&M represents the costs to operate and maintain a power plant that depend on generation output in megawatt-hours (MWh)

#### CERF Variable Name: 
* elec_variable_om_2015USDperMWh
#### CERF required units:
* btu/kWh
#### GCAM-USA Query: 
* 'elec costs by tech and vintage and input'

#### Calculations & Transformations Required:
Prices must be converted from dollars/GJ to dollars/MWh. GCAM prices are provided in 1975 USD and need to be converted to 2015 USD. 
* **Unit Conversion** 
        
        1 GJ = 0.277778 MWh
        1 MWh = (1 / 0.277778) GJ
        conv_GJ_to_MWh = 3.59999712

        elec_variable_om_1975USDperMWh = elec_variable_om_1975USDperGJ * conv_GJ_to_MWh

* **1975 to 2015 Dollar Year Conversion** 

        elec_variable_om_2015USDperMWh = (elec_variable_om_1975USDperMWh * gdp_deflator(2015,1975))

<br>

_____________

<br>

### 6. Technology Lifetime   
Expected technology lifetime in years at investment. 

#### CERF Variable Name: 
* elec_lifetime_yr
#### CERF required units:
* n_years
#### GCAM-USA Exogenous File Path(s): 
* `/pic/projects/im3/gcamusa/gcam-usa-im3/input/gcamdata/outputs/L223.TechLifetime_Dispatch.csv`
#### Calculations & Transformations Required:
None
<br>

_____________

<br>

### 7. Carbon Capture Rate by Technology
Percent of the carbon dioxide from the power plant that is captured and stored.
#### CERF Variable Name: 
* elec_carbon_capture_rate_fraction
#### CERF required units:
* fraction
#### GCAM-USA Exogenous File Path(s): 
* `pic/projects/im3/gcamusa/gcam-usa-im3/input/gcamdata/outputs/L223.TechCarbonCapture_Dispatch.csv`

#### Calculations & Transformations Required:
No transformations required. Technologies without CCS need to be filled in with a rate of 0.

<br>

_____________

<br>

### 8. Fuel CO2 Content by Technology
The amount of carbon dioxide produced by a fuel in relation to the energy produced when the fuel is burned.
#### CERF Variable Name: 
* elec_fuel_co2_content_tonsperMBTU
#### CERF required units:
* tonsperMBTU
#### GCAM-USA Exogenous File Path(s): 
* `/pic/projects/im3/gcamusa/gcam-usa-im3/input/gcamdata/outputs/L202.CarbonCoef.csv`

#### Calculations & Transformations Required:
Values must be converted from kilograms of carbon per GJ to tons per MBTU. Values are available on a fuel type basis at the USA level. These need to be mapped to individual technologies by fuel type and to each US state that sees new capacity in each vintage.

* **Unit Conversion** 

        A = ATOMIC_WEIGHT_OF_CO2_PER_ATOMIC_WEIGHT_OF_CARBON = 44 / 12
        B = TONS_PER_KG = 1e-3
        C = MEGAWATT_HOURS_PER_GIGAJOULE = 0.277777778
        D = MEGA_BRITISH_THERMAL_UNITS_PER_MEGAWATT_HOUR = 3.412e6

        tons_per_MBTU = KG_carbon_per_GJ * A * B / (C * D)

<br>


<br>

_____________

<br>

### 9. Fuel Price Escalation Rate by Technology
The escalation rate of fuel prices for each technology between years. This is calculated directly from the fuel prices extracted previously.
#### CERF Variable Name: 
* elec_fuel_price_escl_rate_fraction
#### CERF required units:
* fraction
#### Calculations & Transformations Required:

     Escalation Rate = (fuelprice_t - fuelprice_t-5) / fuelprice_t-5

<br>

_____________

<br>

### 9. Variable O&M Cost Escalation Rate by Technology
The escalation rate of variable O&M prices for each technology between years. This is calculated directly from the variable O&M prices extracted previously.
#### CERF Variable Name: 
* elec_variable_om_escl_rate_fraction
#### CERF required units:
* fraction
#### Calculations & Transformations Required:

     Escalation Rate = (VOM_t - VOM_t-5) / VOM_t-5

_____________

<br>

### 10. Carbon Price by Year
The cost incurred per ton of carbon for fossil-fuel generation technologies.
#### CERF Variable Name: 
* carbon_price_2015USDperTonCarbon
#### CERF required units:
* $2015/tonCarbon
#### Calculations & Transformations Required:
GCAM prices are provided in 1990 USD and need to be converted to 2015 USD. Carbon price is provided as a global variable by year. This needs to be mapped to all technologies, states, and years with new capacity.
* **Unit Conversion** 

None
* **1990 to 2015 Dollar Year Conversion** 

        carbon_price_2015USDperTonCarbon = (carbon_price_1990USDperTonCarbon * gdp_deflator(2015,1990))