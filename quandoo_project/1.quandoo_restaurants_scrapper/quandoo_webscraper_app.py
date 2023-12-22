import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
from typing import Union


# Create a logger
logger = logging.getLogger(__name__)
# Set the logging level
logger.setLevel(logging.INFO)
# Create a stream handler
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)

# Create a formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(stream_handler)


class QuandooRestaurantsWebScraper:
    """
    Class to Webscrape the Restaurant Data of a given city from the Quandoo Webpage
    """

    # Initializing the constants used in parsing
    MERCHANT_CARD_WRAPPER = 'merchant-card-wrapper'
    MERCHANT_NAME = 'merchant-name'
    MERCHANT_LOCATION = 'merchant-location'
    MERCHANT_CARD_CUISINE = 'merchant-card-cuisine'
    REVIEWS_SCORE = 'reviews-score'
    PAGINATION_BOX = 'pagination-box'

    # Variables Initialized for the results
    RESTAURANT_NAME = 'Restaurant_name'
    RESTAURANT_LOCATION = 'Restaurant_location'
    RESTAURANT_CUISINE = 'Restaurant_cuisine'
    RESTAURANT_SCORE = 'Restaurant_score'
    NUMBER_OF_REVIEWS = 'Number_of_reviews'
    UNKNOWN_ATTRIBUTE = 'Unknown'

    def __init__(self, city_name: str):
        self.city_name = city_name.lower()
        self.url = f'https://www.quandoo.de/en/result?destination={self.city_name}'
        logger.info(f'The city chosen for webscraping is {self.city_name.title()}')

        self.final_result_dataframe = pd.DataFrame()

    @staticmethod
    def extract_soup_from_webpage(url) -> BeautifulSoup:
        """
        Method for extracting the soup (webpage content)
        url : webpage url
        return: the soup required for the downstream process
        """
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        return soup

    def find_last_page(self, soup: BeautifulSoup) -> Union[int, None]:
        """
        Method for determining the last page of the pagination
        soup: soup object from the webpage url
        return: last page number (for example 'Berlin') or none (for example 'Rostock')
        """
        pagination_info = soup.find('div', {'data-qa': self.PAGINATION_BOX})
        if pagination_info:
            *_, last_page_info = pagination_info.find_all('a')
            last_page_available = int(last_page_info.text)
            logger.info(
                f'There are {last_page_available} pages of results for the {self.city_name.title()}'
            )
            return last_page_available
        logger.info(f'We have just one page for the {self.city_name}.')
        return None

    def parse_required_data_from_soup(self, soup: BeautifulSoup) -> pd.DataFrame:
        """
        Method for extracting the relevant restaurant related information from each page's soup
        soup: soup object
        return: results in the form of dataframe of that specific webpage after parsing
        """
        restaurant_results_raw = soup.find_all('div', {'data-qa': self.MERCHANT_CARD_WRAPPER})
        restaurant_list = []
        for result in restaurant_results_raw:

            # Error handling for different attributes

            # Restaurant Name
            try:
                restaurant_name = result.find('h3', {'data-qa': self.MERCHANT_NAME}).text
            except AttributeError:
                restaurant_name = self.UNKNOWN_ATTRIBUTE

            # Restaurant Location
            try:
                restaurant_location = result.find(
                    'span', {'data-qa': self.MERCHANT_LOCATION}
                ).text
            except AttributeError:
                restaurant_location = self.UNKNOWN_ATTRIBUTE

            # Restaurant Cuisine
            try:
                restaurant_cuisine = result.find(
                    'span', {'data-qa': self.MERCHANT_CARD_CUISINE}
                ).text
            except AttributeError:
                restaurant_cuisine = self.UNKNOWN_ATTRIBUTE

            # Restaurant Review score
            try:
                restaurant_score = float(
                    result.find('div', {'data-qa': self.REVIEWS_SCORE}).text.strip('/6')
                )
            except AttributeError:
                restaurant_score = None

            # Number of Reviews
            all_span_tag = result.find_all('span')
            number_of_review_tags = [tag for tag in all_span_tag if 'reviews' in tag.text]
            if not number_of_review_tags:
                number_of_reviews = None
            else:
                number_of_reviews = int(number_of_review_tags[-1].text.strip('reviews'))

            parsed_restaurant_data = {
                self.RESTAURANT_NAME: restaurant_name,
                self.RESTAURANT_LOCATION: restaurant_location,
                self.RESTAURANT_CUISINE: restaurant_cuisine,
                self.RESTAURANT_SCORE: restaurant_score,
                self.NUMBER_OF_REVIEWS: number_of_reviews,
            }
            restaurant_list.append(parsed_restaurant_data)
        return pd.DataFrame(restaurant_list)

    def obtain_scraped_data(self) -> Union[pd.DataFrame, None]:
        """
        Method for creating the final dataframe by parsing relevant data obtained from all pages
        Returns DataFrame (For scenario like Berlin/Frankfurt/Muenchen
        and None (For scenario like Paris/Rome)
        """

        first_page_soup = self.extract_soup_from_webpage(self.url)
        if first_page_soup.title.text == 'Not found':
            logger.info(f'Unfortunately, There is no data for {self.city_name.title()}!')
            return None
        first_page_result = self.parse_required_data_from_soup(first_page_soup)
        self.final_result_dataframe = pd.concat(
            [self.final_result_dataframe, first_page_result], ignore_index=True
        )

        determined_last_page = self.find_last_page(first_page_soup)
        if determined_last_page:
            for page in range(2, determined_last_page + 1):
                logger.info(
                    f'Now Parsing the page number: {page} for {self.city_name.title()} ..........'
                )
                nextpage_url = self.url + f'&page={page}'
                result_df = self.parse_required_data_from_soup(
                    self.extract_soup_from_webpage(nextpage_url)
                )
                self.final_result_dataframe = pd.concat(
                    [self.final_result_dataframe, result_df], ignore_index=True
                )
        logger.info('All the pages are successfully parsed!')
        return self.final_result_dataframe


if __name__ == '__main__':

    # Best Case Scenario 1 - Berlin - Multiple Page Results
    webscraper_berlin = QuandooRestaurantsWebScraper(city_name='berlin').obtain_scraped_data()
    webscraper_berlin.to_csv('scraped_data_results/quandoo_berlin_restaurants.csv', index=False)
    
    # Best Case Scenario 2 - Frankfurt - Multiple Page Results
    webscraper_frankfurt = QuandooRestaurantsWebScraper(
        city_name='FRANKFURT'
    ).obtain_scraped_data()
    webscraper_frankfurt.to_csv('scraped_data_results/quandoo_frankfurt_restaurants.csv', index=False)
    
    # Edge case Scenario 3 - Rostock - Just 1 Page Result
    webscraper_rostock = QuandooRestaurantsWebScraper(
        city_name='rostock'
    ).obtain_scraped_data()
    webscraper_rostock.to_csv('scraped_data_results/quandoo_rostock_restaurants.csv', index=False)
    
    # Worst case Scenario 4 - Paris - No result available
    webscraper_paris = QuandooRestaurantsWebScraper(city_name='paris').obtain_scraped_data()

     
