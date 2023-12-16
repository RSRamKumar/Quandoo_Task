CREATE TABLE restaurant_data (
    Restaurant_name TEXT,
    Restaurant_location TEXT,
    Restaurant_cuisine TEXT,
    Restaurant_score FLOAT,
    Number_of_reviews INT,

);

--COPY restaurant_data
--FROM '/docker-entrypoint-initdb.d/quandoo_berlin_results.csv'
--DELIMITER ','
--CSV HEADER;
