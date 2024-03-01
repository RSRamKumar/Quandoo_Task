import unittest
import pandas as pd
from quandoo_webscraper_app import QuandooRestaurantsWebScraper


class TestQuandooRestaurantsWebScraper(unittest.TestCase):

    def setUp(self) -> None:
        self.webscrap_result_frankfurt = QuandooRestaurantsWebScraper(city_name='FRANKFURT').obtain_scraped_data()


    def test_scrapped_result(self):
        self.assertIsInstance(self.webscrap_result_frankfurt, pd.DataFrame)

        result_columns = ['Restaurant_name', 'Restaurant_location', 'Restaurant_cuisine',
                          'Restaurant_score', 'Number_of_reviews']
        self.assertListEqual(self.webscrap_result_frankfurt.columns.tolist(), result_columns)
        self.assertEqual(self.webscrap_result_frankfurt['Restaurant_name'].isna().sum(), 0)
        self.assertEqual(self.webscrap_result_frankfurt['Restaurant_score'].lt(0).sum(), 0)

if __name__ == '__main__':
    unittest.main()
