connection:"pdp_demos"

include: "/view/*.view.lkml"                # include all views in the views/ folder in this project

explore: birth_weight_air_quality {
  view_name: birth_weight_air_quality
}

explore: snap_qcew_acs {
  view_name: snap_qcew_acs
}

explore: county_demographics {
  view_name: county_demographics
}
