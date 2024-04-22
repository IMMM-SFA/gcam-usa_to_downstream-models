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

CERF requires the following variables:

| CERF Variable | CERF Variable Name | Description | CERF Required Units |
|---------------|--------------------|-------------|---------------------|
| Annual capacity by tech| elec_capacity_usa_GW | Annual capacity installed by technology (inclusive of cooling type), state, vintage, year, and scenario | GW | | |
| Capacity factor by tech | elec_capacity_factor_usa_in | Investment-level capacity factor by technology (inclusive of cooling type), state, vintage, year, and scenario | fraction | | |
| Heat rate by tech | elec_heat_rate_BTUperkWh | Heat rate by technology (inclusive of cooling type), state, vintage, year, and scenario | btu/kWh | | |
| Fuel price by tech | elec_fuel_price_2015USDperMBTU | Fuel price by technology (inclusive of cooling type), state, vintage, year, and scenario | $2015USD/MBTU | | |
| Variable O&M by tech | elec_variable_om_2015USDperMWh | Variable O&M cost by technology (inclusive of cooling type), state, vintage, year, and scenario | $2015USD/MWh | | |
|Investment Level Technology Lifetime            |elec_lifetime_yr               | Technology lifetime by technology (inclusive of cooling type), state, vintage, year, and scenario |n_years|
|Carbon Capture Rate by tech          |elec_carbon_capture_rate_fraction| Carbon capture rate by technology (inclusive of cooling type), state, vintage, year, and scenario  |Fraction |
|Fuel CO2 Content by tech         | elec_fuel_co2_content_tonsperMBTU| Fuel CO2 content by technology (inclusive of cooling type), state, vintage, year, and scenario  |tons/MBTU|
|% change in fuel price         |elec_fuel_price_escl_rate_fraction| Percent change in fuel price between year t and t+5 | Fraction | 
|% change in variable O&M       |elec_variable_om_escl_rate_fraction|Percent change in variable O&M cost between year t and t+5 |Fraction|

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

Some GCAM-USA variables are provided in 1975$ and need to be converted to 2015$. The function below allows for conversion between price years of GCAM parameters. The following equation converts between a base year USD to a desired year USD based on a deflator range that starts in a starting year:

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

### 1. Capacity by Technology Type
Electricity capacity by technology (inclusive of cooling type), vintage, subregion, and year. 
#### CERF Variable Name: 
* elec_cap_usa_GW
#### CERF required units:
* Gigawatts (GW)
#### GCAM-USA Queries: 
* 
* 
#### Calculations & Transformations Required:
* **Unit Conversion** - GCAM-USA provides energy in Exajoules (EJ) which need to be converted to GWh.

        1 GWh = 1 / 277778 EJ 



* **Derive Capacity from Energy** - Energy is power over time. Therefore, energy output over a given year is a function of power capacity (GW) multiplied by the number of hours a unit is operating. The number of hours a plant operates in a given year is determined by the product of the plant's capacity factor (cf) and the number of hours in a year (8760). A plant's capacity factor is a value between 0 and 1, where a value of 1 means that it is operating all hours of the year.

        capacity = generation / ( cf * 8760)

* **Example:**

        100000 GWh / (.20 * 8760) = 57 GW

#### Repository Script:


<br>

_____________

<br>

### 2. Capacity Factor by Technology
Capacity factor by technology, vintage, subregion, and year. Capacity factor represents the annual power generated divided by the potential output if the plant operated at its rated capacity for a year. GCAM tracks both investment level and actual capacity factor for each technology. The investment-level capacity factor is required for CERF.
#### CERF Variable Name: 
* elec_capacity_factor_usa_in
#### CERF required units:
* fraction
#### GCAM-USA Queries: 
* 
* 
#### Calculations & Transformations Required:
#### Repository Script:

<br>

_____________

<br>

### 3. Heat Rate by Technology 
Heat rate by technology, vintage, subregion, and year. Heat rate is the amount of energy used by a power plant to generate one kilowatt-hour of electricity. Solar and wind heat rates should have a heat rate value of 0.
#### CERF Variable Name: 
* elec_heat_rate_BTUperkWh
#### CERF required units:
* btu/kWh
#### GCAM-USA Queries: 
* 
* 
#### Calculations & Transformations Required:
#### Repository Script:


<br>

_____________

<br>

### 4. Fuel Price by Technology
Fuel price for natural gas, oil, coal, and biomass for gas-, oil-, coal-, and biomass- fired technologies. 
#### CERF Variable Name: 
* elec_fuel_price_2015USDperMBTU
#### CERF required units:
* btu/kWh
#### GCAM-USA Queries: 
* 
* 
#### Calculations & Transformations Required:
Biomass ("regional biomass") is provided at the regional level, fossil fuels ("refined liquids industrial", "regional coal", and "wholesale gas") are provided at the grid region level. These each need to be mapped to states and to all appropriate technologies. 

Prices must be converted from dollars/GJ to dollars/MMBTU (million BTUs). Unit conversion from https://www.unitconverters.net/energy/gigajoule-to-mega-btu-it.htm. GCAM prices are provided in 1975 USD and need to be scaled to 2015 USD. 
* **Unit Conversion** 
        
        1 MMBTU = 10e6 BTU 
        1 GJ = .947817 MMBTU
        1 MMBTU = (1 / .947817) GJ
        conv_GJ_to_MMBTU = 1.0550559

        elec_fuel_price_1975USDperMBTU = elec_fuel_price_1975USDperGJ * conv_GJ_to_MMBTU

* **1975 to 2015 Dollar Year Conversion** 

        elec_fuel_price_2015USDperMBTU = (elec_fuel_price_1975USDperMBTU * gdp_deflator(2015,1975))

#### Repository Script:

<br>

_____________

<br>

### 5. Variable O&M Cost by Technology
Variable operations and maintenance costs by technology, vintage, year, and state. Variable O&M represents the costs to operate and maintain a power plant that depend on generation output in megawatt-hours (MWh)

#### CERF Variable Name: 
* elec_variable_om_2015USDperMWh
#### CERF required units:
* btu/kWh
#### GCAM-USA Queries: 
* 
* 
#### Calculations & Transformations Required:
Prices must be converted from dollars/GJ to dollars/MWh. GCAM prices are provided in 1975 USD and need to be converted to 2015 USD. 
* **Unit Conversion** 
        
        1 GJ = 0.277778 MWh
        1 MWh = (1 / 0.277778) GJ
        conv_GJ_to_MWh = 3.59999712

        elec_variable_om_1975USDperMWh = elec_variable_om_1975USDperGJ * conv_GJ_to_MWh

* **1975 to 2015 Dollar Year Conversion** 

        elec_variable_om_2015USDperMWh = (elec_variable_om_1975USDperMWh * gdp_deflator(2015,1975))

#### Repository Script:
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
* 
* 
#### Calculations & Transformations Required:

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
* 
* 
#### Calculations & Transformations Required:

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
* 
* 
#### Calculations & Transformations Required:
Values must be converted from kilograms of carbon per GJ to tons per MBTU.

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



