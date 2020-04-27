view: birth_weight_air_quality {
  derived_table: {
    sql:
      SELECT
        epa.year,
        geo_id AS county_fips_code,
        county_of_residence,
        AVG(Ave_OE_Gestational_Age_Wks) AS Ave_OE_Gestational_Age_Wks,
        AVG(Ave_Birth_Weight_gms) AS Ave_Birth_Weight_gms,
        ROUND(AVG(fifty_percentile),2) AS pm25_50_percentile,
        ROUND(AVG(seventy_five_percentile),2) AS pm25_75_percentile,
        ROUND(AVG(ninety_nine_percentile),2) AS pm25_99_percentile,
        ROUND(AVG(first_max_value),2) AS pm25_annual_maximum,
        ROUND(AVG(income_per_capita),2) AS income_per_capita,
        RANK() OVER (ORDER BY AVG(Ave_Birth_Weight_gms) desc) AS national_rank
      FROM
        `bigquery-public-data.epa_historical_air_quality.air_quality_annual_summary` epa
      RIGHT JOIN
        `bigquery-public-data.sdoh_cdc_wonder_natality.county_natality` cdc ON CONCAT(epa.state_code, epa.county_code) = cdc.County_of_Residence_FIPS AND epa.year = EXTRACT(year from cdc.Year)
      JOIN
        `bigquery-public-data.census_bureau_acs.county_2017_5yr` acs ON cdc.County_of_Residence_FIPS = acs.geo_id
      WHERE
        pollutant_standard LIKE "PM25 Annual 2012"
        AND births > 100
        AND epa.year = 2017
      GROUP BY
        geo_id,county_of_residence, epa.year
      ORDER BY
        ave_birth_weight_gms asc
        ;;
}

  dimension: observation_year {
    description: "Year of the observations"
    type: string
    sql: ${TABLE}.year ;;
  }

  dimension: county_fips_code {
    description: "County in which the births occurred"
    type: string
    sql: ${TABLE}.county_fips_code ;;
    map_layer_name: us_counties_fips
  }

  dimension: county_name {
    description: "County name"
    type:  string
    sql: ${TABLE}.county_of_residence ;;
  }

  measure: birth_age_in_weeks {
    description: "Average birth age of the county in gestational weeks"
    type: average
    sql: ${TABLE}.Ave_OE_Gestational_Age_Wks ;;
  }

  measure: birth_weight_in_grams {
    description: "Average birth weight of the county in grams"
    type: average
    sql: ${TABLE}.Ave_Birth_Weight_gms ;;
  }

  measure: pm25_50th_percentile {
    description: "Median value of PM 2.5 measurements within the county"
    type: average
    sql: ${TABLE}.pm25_50_percentile ;;
  }

  measure: pm25_75_percentile {
    description: "75th percentile of PM 2.5 measurements within the county"
    type: average
    sql: ${TABLE}.  pm25_75_percentile ;;
  }

  measure: pm25_99_percentile{
    description: "99th percentile of PM 2.5 measurements within the county"
    type: average
    sql: ${TABLE}.  pm25_99_percentile ;;
  }

  measure: pm25_annual_maximum{
    description: "Annual maximum of PM 2.5 measurements within the county"
    type: max
    sql: ${TABLE}.  pm25_annual_maximum ;;
  }

  measure: income_per_capita{
    description: "Per capita income for the referenced county"
    type: average
    sql: ${TABLE}.income_per_capita ;;
  }

  measure: national_rank {
    description: "The counties position relative to all other available counties"
    type: average
    sql:  ${TABLE}.national_rank ;;
  }
}
