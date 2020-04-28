view: county_demographics {

derived_table: {
    sql:
with county_area AS (
  SELECT
    geo_id,
    county_name,
    state_fips_code,
    ST_AREA(county_geom)/(1609*1609) AS county_area_sq_mi
   FROM
    `bigquery-public-data.geo_us_boundaries.counties` counties)

,county_density AS
  (SELECT
    area.geo_id,
    county_name,
    state_name,
    total_pop,
    county_area_sq_mi,
    SAFE_DIVIDE(total_pop,area.county_area_sq_mi) AS population_density
   FROM
    `bigquery-public-data.census_bureau_acs.county_2017_5yr` acs
   JOIN
    county_area area USING (geo_id)
   JOIN
    `bigquery-public-data.census_utility.fips_codes_states` USING (state_fips_code))

,county_income_buckets AS (
  SELECT
    geo_id, SAFE_DIVIDE((income_less_10000)+(income_10000_14999)+(income_15000_19999)+(income_20000_24999)+(income_25000_29999)+(income_30000_34999)+(income_35000_39999)+(income_40000_44999)+(income_45000_49999),(total_pop))*100 AS percent_income_under_50k,
    SAFE_DIVIDE((income_50000_59999)+(income_60000_74999)+(income_75000_99999),(total_pop))*100 AS percent_income_50k_to_100k,
    SAFE_DIVIDE((income_100000_124999)+(income_125000_149999)+(income_150000_199999)+(income_200000_or_more), (total_pop))*100 AS percent_income_over_100k
  FROM
    `bigquery-public-data.census_bureau_acs.county_2017_5yr` )

,county_ages AS (
  SELECT
    geo_id,
    SAFE_DIVIDE((male_65_to_66)+( male_67_to_69) + (male_70_to_74)+ (male_75_to_79) + (male_80_to_84) + (male_85_and_over),(male_pop))*100 AS percent_males_over_65,
    SAFE_DIVIDE(( female_65_to_66)+ (female_67_to_69)+(female_70_to_74)+ (female_75_to_79)+ (female_80_to_84)+(female_85_and_over),(female_pop))*100 AS percent_females_over_65,
    SAFE_DIVIDE( (pop_25_64), (total_pop))*100 AS percent_working_age
  FROM
    `bigquery-public-data.census_bureau_acs.county_2017_5yr`)

,county_demographics AS (
  SELECT
    geo_id,
    SAFE_DIVIDE( male_pop, total_pop)*100 AS percent_male,
    SAFE_DIVIDE(female_pop, total_pop)*100 AS percent_female,
    SAFE_DIVIDE( white_pop , total_pop)*100 AS percent_white,
    SAFE_DIVIDE( black_pop, total_pop)*100 AS percent_black,
    SAFE_DIVIDE( asian_pop, total_pop)*100 AS percent_asian,
    SAFE_DIVIDE( hispanic_pop, total_pop)*100 AS percent_hispanic,
    SAFE_DIVIDE( amerindian_pop, total_pop)*100 AS percent_americanindian
  FROM
    `bigquery-public-data.census_bureau_acs.county_2017_5yr`)

SELECT
  dense.*,
  income.* EXCEPT(geo_id),
  ages.* EXCEPT(geo_id),
  demographics.* EXCEPT(geo_id),
  CAST(covid_facts.date AS TIMESTAMP) AS date,
  covid_facts.confirmed_cases,
  covid_facts.deaths
FROM
  county_density dense
FULL JOIN
  county_income_buckets income USING (geo_id)
FULL JOIN
  county_ages ages USING (geo_id)
FULL JOIN
  county_demographics demographics USING (geo_id)
FULL JOIN
  `bigquery-public-data.covid19_usafacts.summary` covid_facts ON dense.geo_id = covid_facts.county_fips_code

      ;;
  }

  # Define your dimensions and measures here, like this:
  dimension: county_fips_code {
    description: "Unique ID for each county"
    type: string
    sql: ${TABLE}.geo_id;;
    map_layer_name: us_counties_fips
  }

  dimension: county_name {
    description: "County name"
    type: string
    sql: ${TABLE}.county_name ;;
  }

  dimension: state_name {
    description: "State name"
    type: string
    sql: ${TABLE}.state_name ;;
  }

  dimension: total_pop {
    description: "Total population within each county"
    type: number
    sql: ${TABLE}.total_pop ;;
  }

  dimension: county_area_sq_mi {
    description: "The date when each user last ordered"
    type: number
    sql: ${TABLE}.county_area_sq_mi ;;
  }

  measure: population_density {
    description: "Population density for a given county"
    type: sum
    sql: ${TABLE}.population_density ;;
  }

  measure: percent_of_income_under_50k {
    description: "Percent of households with income below $50k"
    type: sum
    sql: ${TABLE}.percent_income_under_50k ;;
  }

  measure: percent_of_income_between_50k_and_100k {
    description: "Percent of households with income above $50k, but below $100k"
    type: sum
    sql: ${TABLE}.percent_income_50k_to_100k ;;
  }

  measure: percent_of_income_over_100k {
    description: "Percent of households with income over $100k"
    type: sum
    sql: ${TABLE}.percent_income_over_100k ;;
  }

  measure: percent_of_males_over_65 {
    description: "Percent of males over the age of 65"
    type: average
    sql: ${TABLE}.percent_males_over_65 ;;
  }

  measure: percent_of_females_over_65 {
    description: "Percent of females over the age of 65"
    type: average
    sql: ${TABLE}.percent_females_over_65 ;;
  }

  measure: percent_male {
    description: "Percent of the total population that is male"
    type: average
    sql: ${TABLE}.percent_male ;;
  }

  measure: percent_female {
    description: "Percent of the total population that is female"
    type: average
    sql: ${TABLE}.percent_female ;;
  }

  measure: percent_white {
    description: "Percent of the total population that are white"
    type: sum
    sql: ${TABLE}.percent_white ;;
  }

  measure: percent_black {
    description: "Percent of the total population that are black"
    type: sum
    sql: ${TABLE}.percent_black ;;
  }

  measure: percent_asian {
    description: "Percent of the total population that are of Asian decent"
    type: sum
    sql: ${TABLE}.percent_asian ;;
  }

  measure: percent_hispanic {
    description: "Percent of the total population that are of Hispanic decent"
    type: sum
    sql: ${TABLE}.percent_hispanic ;;
  }

  measure: percent_american_indian {
    description: "Percent of the total population that are of American Indian decent"
    type: sum
    sql: ${TABLE}.percent_americanindian ;;
  }

  dimension: date {
    description: "Date of COVID-19 confirmed cases and death totals"
    type: date
    sql: ${TABLE}.date ;;
  }

  measure: confirmed_covid_cases {
    description: "Confirmed COVID-19 cases"
    type: sum
    sql: ${TABLE}.confirmed_cases ;;
  }

  measure: confirmed_covid_deaths {
    description: "Confirmed COVID-19 deaths"
    type: sum
    sql: ${TABLE}.deaths ;;
  }

  measure: confirmed_covid_cases_per_100000 {
    description: "Confirmed COVID-19 deaths"
    type: sum
    sql: ROUND(${TABLE}.confirmed_cases/${TABLE}.total_pop * 100000,2);;
    }

  measure: confirmed_covid_deaths_per_100000 {
    description: "Confirmed COVID-19 deaths"
    type: sum
    sql: ROUND(${TABLE}.deaths/${TABLE}.total_pop * 100000,2) ;;
  }
}
