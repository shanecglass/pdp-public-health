view: county_demographics {

derived_table: {
    sql:
    CREATE TEMPORARY FUNCTION miles_to_meters(x FLOAT64) AS (x * 1609);
CREATE TEMPORARY FUNCTION sqm_to_sqmi (x FLOAT64) AS (x / (1609*1609));


#Defining the list of blockgroups within 1 mile of the store
with county_area AS (
  SELECT
    geo_id,
    county_name,
    ST_AREA(county_geom)/(1609*1609) AS county_area_sq_mi
   FROM
    `bigquery-public-data.geo_us_boundaries.counties` counties)

#Determining the population density of the blockgroups within 1 mile of the store, using the previous WITH clause to define the blockgroups of which we are finding the population density
,county_density AS
  (SELECT
    area.geo_id,
    county_name,
    total_pop,
    county_area_sq_mi,
    SAFE_DIVIDE(total_pop,area.county_area_sq_mi) AS population_density
   FROM
    `bigquery-public-data.census_bureau_acs.county_2017_5yr` acs
   JOIN
    county_area area USING (geo_id))

,county_income_buckets AS (
  SELECT
    geo_id, SAFE_DIVIDE(SUM(income_less_10000)+SUM(income_10000_14999)+SUM(income_15000_19999)+SUM(income_20000_24999)+SUM(income_25000_29999)+SUM(income_30000_34999)+SUM(income_35000_39999)+SUM(income_40000_44999)+SUM(income_45000_49999),SUM( households ))*100 AS percent_income_under_50k,
    SAFE_DIVIDE(SUM(income_50000_59999)+SUM(income_60000_74999)+SUM(income_75000_99999),SUM(households))*100 AS percent_income_50k_to_100k,
    SAFE_DIVIDE(SUM(income_100000_124999)+SUM(income_125000_149999)+SUM(income_150000_199999)+SUM(income_200000_or_more), SUM(households))*100 AS percent_income_over_100k
  FROM
    `bigquery-public-data.census_bureau_acs.county_2017_5yr`
  GROUP BY
    geo_id)

,county_ages AS (
  SELECT
    geo_id,
    SAFE_DIVIDE(SUM(male_65_to_66)+SUM( male_67_to_69) + SUM(male_70_to_74)+ SUM(male_75_to_79) + SUM(male_80_to_84) + SUM(male_85_and_over),SUM(male_pop))*100 AS percent_males_over_65,
    SAFE_DIVIDE(SUM( female_65_to_66)+ SUM(female_67_to_69)+SUM(female_70_to_74)+ SUM(female_75_to_79)+ SUM(female_80_to_84)+SUM(female_85_and_over),SUM(female_pop))*100 AS percent_females_over_65,
    SAFE_DIVIDE( SUM(pop_25_64), SUM(total_pop))*100 AS percent_working_age
  FROM
    `bigquery-public-data.census_bureau_acs.county_2017_5yr`
  GROUP BY
    geo_id)

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
  *
FROM
  county_density
FULL JOIN
  county_income_buckets USING (geo_id)
FULL JOIN
  county_ages USING (geo_id)
FULL JOIN
  county_demographics USING (geo_id)

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
    sql: ${TABLE}.percent_income_under_50k ;;
  }

  measure: percent_of_income_over_100k {
    description: "Percent of households with income over $100k"
    type: sum
    sql: ${TABLE}.percent_income_over_100k ;;
  }

  measure: percent_of_males_over_65 {
    description: "Percent of males over the age of 65"
    type: sum
    sql: ${TABLE}.percent_males_over_65 ;;
  }

  measure: percent_of_females_over_65 {
    description: "Percent of females over the age of 65"
    type: sum
    sql: ${TABLE}.percent_females_over_65 ;;
  }

  measure: percent_male {
    description: "Percent of the total population that is male"
    type: sum
    sql: ${TABLE}.percent_male ;;
  }

  measure: percent_female {
    description: "Percent of the total population that is female"
    type: sum
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
}
