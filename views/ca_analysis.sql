SELECT
  age,
  california_ns_region as california_region,
  SUM(population) as s_population,
  AVG(median_house_value) as m_median_house_value
FROM ca_housing
GROUP BY california_region, age
ORDER BY m_median_house_value DESC