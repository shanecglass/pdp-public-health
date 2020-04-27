view: snap_qcew_acs {

derived_table: {
  sql: WITH us_counties AS(
  SELECT
    geo_id, county_name, state_name, counties.state_fips_code
  FROM
    `bigquery-public-data.geo_us_boundaries.counties` counties
  JOIN
    `bigquery-public-data.census_utility.fips_codes_states` USING (state_fips_code)),

acs_2017 AS (
  SELECT geo_id, county_name, state_fips_code, state_name, total_pop, pop_25_64
  FROM `bigquery-public-data.census_bureau_acs.county_2017_5yr`
  JOIN us_counties USING (geo_id)
 ),

snap_2017_Jan AS (
  SELECT FIPS, SNAP_All_Participation_Persons AS snap_total
  FROM `bigquery-public-data.sdoh_snap_enrollment.snap_enrollment`
  WHERE Date='2017-01-01'
),

qcew_2017_q2 AS(
  SELECT
    geoid,
    SAFE_DIVIDE(month3_emplvl_11_agriculture_forestry_fishing_and_hunting, pop_25_64)*100   AS percent_agriculture,
    SAFE_DIVIDE(month3_emplvl_1024_professional_and_business_services,pop_25_64)*100  AS percent_professional_services,
    SAFE_DIVIDE(month3_emplvl_61_educational_services,pop_25_64)*100   AS percent_education,
    SAFE_DIVIDE(month3_emplvl_53_real_estate_and_rental_and_leasing,pop_25_64)*100   AS percent_real_estate,
    SAFE_DIVIDE(month3_emplvl_1023_financial_activities,pop_25_64)*100   AS percent_financial_activities,
    SAFE_DIVIDE(month3_emplvl_31_33_manufacturing,pop_25_64)*100   AS percent_manufacturing,
    SAFE_DIVIDE(month3_emplvl_23_construction,pop_25_64)*100   AS percent_construction,
    SAFE_DIVIDE(month3_emplvl_1029_unclassified,pop_25_64)*100   AS percent_unclassified,
    SAFE_DIVIDE(month3_emplvl_71_arts_entertainment_and_recreation,pop_25_64)*100   AS percent_arts_entertainment_recreation,
    SAFE_DIVIDE(month3_emplvl_1011_natural_resources_and_mining,pop_25_64)*100   AS percent_natural_resources_mining,
    SAFE_DIVIDE(month3_emplvl_54_professional_and_technical_services,pop_25_64)*100   AS percent_professional_technical,
    SAFE_DIVIDE(month3_emplvl_1026_leisure_and_hospitality,pop_25_64)*100   AS percent_leisure_hospitality,
    SAFE_DIVIDE(month3_emplvl_1022_information,pop_25_64)*100   AS percent_information,
    SAFE_DIVIDE(month3_emplvl_72_accommodation_and_food_services,pop_25_64)*100   AS percent_accomodation_food_services,
    SAFE_DIVIDE(month3_emplvl_22_utilities,pop_25_64)*100   AS percent_utilities,
    SAFE_DIVIDE(month3_emplvl_42_wholesale_trade,pop_25_64)*100   AS percent_wholesale_trade,
    SAFE_DIVIDE(month3_emplvl_81_other_services_except_public_administration,pop_25_64)*100   AS percent_other_services,
    SAFE_DIVIDE(month3_emplvl_52_finance_and_insurance,pop_25_64)*100   AS percent_finance_insurance,
    SAFE_DIVIDE(month3_emplvl_44_45_retail_trade,pop_25_64)*100   AS percent_retail
  FROM
    `bigquery-public-data.bls_qcew.2017_q2` qcew
  JOIN
    acs_2017 ON qcew.geoid = acs_2017.geo_id),

snap_pcnt AS (
  SELECT acs_2017.geo_id, acs_2017.county_name, acs_2017.state_name, acs_2017.state_fips_code, acs_2017.total_pop, acs_2017.pop_25_64, snap_2017_Jan.snap_total,
  ROUND((snap_2017_Jan.snap_total / acs_2017.total_pop) * 100,1) AS snap_pop_pcnt
  FROM acs_2017
  JOIN snap_2017_Jan
  ON acs_2017.geo_id = snap_2017_Jan.FIPS
)

SELECT
  snap_pcnt.*,
  qcew.* EXCEPT (geoid)
FROM
  snap_pcnt
JOIN
  qcew_2017_q2 qcew ON snap_pcnt.geo_id = qcew.geoid
    ;;
}
#
#   # Define your dimensions and measures here, like this:
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

  dimension: state_fips {
    description: "State FIPS code"
    type: string
    sql: ${TABLE}.state_fips_code ;;
    map_layer_name: us_states
  }

  dimension: state_name {
    description: "State name"
    type: string
    sql: ${TABLE}.state_name ;;
  }

  measure: county_population {
    description: "Total population of the county"
    type: average
    sql: ${TABLE}.total_pop ;;
  }

  measure: county_population_25_64 {
    description: "Total population of the county between 25 and 64"
    type: average
    sql: ${TABLE}.pop_25_64 ;;
  }

  dimension: snap_participants_total {
    description: "Total population of the county participating in the SNAP program"
    type: number
    sql: ${TABLE}.snap_total ;;
  }

  dimension: snap_pop_pcnt {
    description: "Percentage of the total population of the county participating in the SNAP program"
    type: number
    sql: ${TABLE}.snap_pop_pcnt ;;
  }

  measure: percent_agriculture {
    description: "Percentage of the working age population employed in the argriculture industry"
    type: average
    sql: ${TABLE}.percent_agriculture ;;
  }

  measure: percent_professional_services {
    description: "Percentage of the working age population employed in the professional services industry"
    type: average
    sql: ${TABLE}.percent_professional_services  ;;
  }

  measure: percent_education {
    description: "Percentage of the working age population employed in the education industry"
    type: average
    sql: ${TABLE}.percent_education  ;;
  }

  measure: percent_real_estate {
    description: "Percentage of the working age population employed in the industry"
    type: average
    sql: ${TABLE}.percent_real_estate  ;;
  }

  measure: percent_financial_activities {
    description: "Percentage of the working age population employed in the industry"
    type: average
    sql: ${TABLE}.percent_financial_activities   ;;
  }

  measure: percent_manufacturing {
    description: "Percentage of the working age population employed in the industry"
    type: average
    sql: ${TABLE}.percent_manufacturing  ;;
  }

  measure: percent_construction {
    description: "Percentage of the working age population employed in the industry"
    type: average
    sql: ${TABLE}.percent_construction   ;;
  }

  measure: percent_unclassified {
    description: "Percentage of the working age population employed in the industry"
    type: average
    sql: ${TABLE}.percent_unclassified   ;;
  }

  measure: percent_arts_entertainment_recreation {
    description: "Percentage of the working age population employed in the industry"
    type: average
    sql: ${TABLE}.percent_arts_entertainment_recreation  ;;
  }

  measure: percent_natural_resources_mining {
    description: "Percentage of the working age population employed in the industry"
    type: average
    sql: ${TABLE}.percent_natural_resources_mining   ;;
  }

  measure: percent_professional_technical {
    description: "Percentage of the working age population employed in the industry"
    type: average
    sql: ${TABLE}.percent_professional_technical   ;;
  }

  measure: percent_leisure_hospitality {
    description: "Percentage of the working age population employed in the industry"
    type: average
    sql: ${TABLE}.percent_leisure_hospitality  ;;
  }

  measure: percent_information {
    description: "Percentage of the working age population employed in the industry"
    type: average
    sql: ${TABLE}.percent_information  ;;
  }

  measure: percent_accomodation_food_services {
    description: "Percentage of the working age population employed in the industry"
    type: average
    sql: ${TABLE}.percent_accomodation_food_services   ;;
  }

  measure: percent_utilities {
    description: "Percentage of the working age population employed in the industry"
    type: average
    sql: ${TABLE}.percent_utilities  ;;
  }

  measure: percent_wholesale_trade {
    description: "Percentage of the working age population employed in the industry"
    type: average
    sql: ${TABLE}.percent_wholesale_trade  ;;
  }

  measure: percent_other_services {
    description: "Percentage of the working age population employed in the industry"
    type: average
    sql: ${TABLE}.percent_other_services   ;;
  }

  measure: percent_finance_insurance {
    description: "Percentage of the working age population employed in the industry"
    type: average
    sql: ${TABLE}.percent_finance_insurance  ;;
  }

  measure: percent_retail {
    description: "Percentage of the working age population employed in the industry"
    type: average
    sql: ${TABLE}.percent_retail   ;;
  }
}
