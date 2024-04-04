"""Script for scraping the Restaurant data using BeautifulSoup and
creating Pydantic Models"""

import json
import logging
from enum import Enum
import time
from typing import Optional, Union, List
#from typing_extensions import URL

import requests
from bs4 import BeautifulSoup, Tag
from pydantic import BaseModel, Field, field_serializer, field_validator, AnyHttpUrl

# Create a logger
logger = logging.getLogger(__name__)
# Set the logging level
logger.setLevel(logging.INFO)
# Create a stream handler
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)

# Create a formatter
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
stream_handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(stream_handler)


class SoupAttribute(Enum):
    """
    Initializing the constants used in parsing
    """

    MERCHANT_CARD_WRAPPER = "merchant-card-wrapper"
    MERCHANT_NAME = "merchant-name"
    MERCHANT_LOCATION = "merchant-location"
    MERCHANT_CARD_CUISINE = "merchant-card-cuisine"
    REVIEWS_SCORE = "reviews-score"
    PAGINATION_BOX = "pagination-box"
    RESTAURANT_TAGS = "restaurant-tags"
    MERCHANT_ADDRESS = "merchant-address"


class RestaurantMetaData(BaseModel):
    """
    Initalizing the Pydantic data model for the restaurant scraped meta data
    """
    restaurant_tags: List[str]
    restaurant_address: str

    @field_validator("restaurant_address", mode="before")
    @classmethod
    def get_restaurant_address(cls, address: str) -> str:
        """
        Method for cleaning the raw restaurant address field
        """
        if address:
            return ",".join(address)
        return None


class RestaurantData(BaseModel):
    """
    Initalizing the Pydantic data model for the restaurant scraped data
    """

    # restaurant_name: str = Field(alias="Restaurant_name")
    restaurant_name: str
    restaurant_location: str
    restaurant_cuisine: str
    restaurant_score: Optional[float] = Field(default=None, ge=0, lt=6, strict=True)
    number_of_reviews: Optional[int] = Field(default=None)
    restaurant_url: AnyHttpUrl
    restaurant_meta_data: RestaurantMetaData

    @field_serializer("restaurant_location")
    @classmethod
    def get_parsed_location(cls, location: str) -> str:
        """
        Method for cleaning the raw location field
        """
        return location.replace("Located at", "").strip()

    @field_validator("restaurant_score", mode="before")
    @classmethod
    def get_parsed_restaurant_score(cls, score: str) -> Union[float, None]:
        """
        Method for cleaning the raw restaurant score field
        """
        if score and score.endswith("/6"):
            return float(score.strip("/6"))
        return None

    @field_validator("number_of_reviews", mode="before")
    @classmethod
    def get_parsed_number_of_reviews(cls, number_of_reviews: str) -> Union[int, None]:
        """
        Method for cleaning the number of reviews field
        """
        if number_of_reviews:
            return int(number_of_reviews.strip("reviews"))
        return None

    # @computed_field(alias="parsed_number_of_reviews")
    # def get_parsed_number_of_reviews(self) -> Union[int, None]:
    #     if self.number_of_reviews:
    #         return int(self.number_of_reviews.strip("reviews"))
    #     return None


class QuandooRestaurantsWebScraper:
    """
    Class to Webscrape the Restaurant Data of a given city from the Quandoo Webpage
    """

    def __init__(self, city_name: str):
        self.city_name = city_name.lower()
        self.base_url = "https://www.quandoo.de"
        self.search_url = f"{self.base_url}/en/result?destination={self.city_name}"
        logger.info("The city chosen for webscraping is %s", self.city_name.title())

        self.final_result_list = []
        self.restaurant_url = None

    @staticmethod
    def extract_soup_from_webpage(url) -> BeautifulSoup:
        """
        Method for extracting the soup (webpage html content)
        url : webpage url
        return: the soup required for the downstream scraping step
        """
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.content, "html.parser")
        return soup
    
    @staticmethod
    def combine_json_strings(json_strings: List[str]) -> List[dict]:
         """
         Method for converting list containing json strings 
         to a list containing json dict
         """
         data_list = list(map(json.loads, json_strings))
         return data_list
          
 
    def find_last_page(self, soup: BeautifulSoup) -> Union[int, None]:
        """
        Method for determining the last page of the available results
        soup: soup object from the webpage url
        return: last page number (for example 'Berlin') or None (for example 'Rostock')
        """
        pagination_info = soup.find(
            "div", {"data-qa": SoupAttribute.PAGINATION_BOX.value}
        )
        if pagination_info:
            *_, last_page_info = pagination_info.find_all("a")
            last_page_available = int(last_page_info.text)
            logger.info(
                "There are %s pages of results for the city %s",
                last_page_available,
                self.city_name.title(),
            )
            return last_page_available
        logger.info("We have just one page for the %s.", self.city_name)
        return None

    def parse_individual_restaurant_data_from_scrape(
        self, raw_restaurant_html: Tag
    ) -> dict:
        """
        Method for extracting the required restaurant information from each restaurant's html tag
        raw_restaurant_html: beautifulsoup tag element containing the individual restaurant element
        return: restaurant name, location, cuisine, review score,
        number of reviews for an individual
        restaurant in a dict
        """
        restaurant_data_dict = {}
        # Restaurant Name
        restaurant_data_dict["restaurant_name"] = raw_restaurant_html.find(
            "h3", {"data-qa": SoupAttribute.MERCHANT_NAME.value}
        ).text

        # Restaurant Location
        restaurant_data_dict["restaurant_location"] = raw_restaurant_html.find(
            "span", {"data-qa": SoupAttribute.MERCHANT_LOCATION.value}
        ).text

        # Restaurant Cuisine
        restaurant_data_dict["restaurant_cuisine"] = raw_restaurant_html.find(
            "span", {"data-qa": SoupAttribute.MERCHANT_CARD_CUISINE.value}
        ).text

        # Restaurant Review score
        try:
            restaurant_data_dict["restaurant_score"] = raw_restaurant_html.find(
                "div", {"data-qa": SoupAttribute.REVIEWS_SCORE.value}
            ).text
        except AttributeError:
            restaurant_data_dict["restaurant_score"] = None

        # Number of Reviews
        all_span_tag = raw_restaurant_html.find_all("span")
        number_of_review_tags = [tag for tag in all_span_tag if "reviews" in tag.text]
        if not number_of_review_tags:
            restaurant_data_dict["number_of_reviews"] = None
        else:
            restaurant_data_dict["number_of_reviews"] = number_of_review_tags[-1].text

        # Restaurant URL
        self.restaurant_url = (
            f"{self.base_url}{raw_restaurant_html.find('a').get('href')}"
        )
        restaurant_data_dict["restaurant_url"] = self.restaurant_url
        restaurant_data_dict["restaurant_meta_data"] = self.parse_restaurant_meta_data() 
        return restaurant_data_dict

    def parse_restaurant_meta_data(self) -> dict:
        """
        Method for extracting the restaurant meta data namely tags and address
        """
        meta_data_response = self.extract_soup_from_webpage(self.restaurant_url)
        restaurant_tag_list = [
            tag.text
            for tag in meta_data_response.find(
                "div", {"data-qa": SoupAttribute.RESTAURANT_TAGS.value}
            ).find_all("span")
        ]
        restaurant_address = [
            tag.text
            for tag in meta_data_response.find(
                "a", {"data-qa": SoupAttribute.MERCHANT_ADDRESS.value}
            ).find_all("span")
        ]
        return {
            "restaurant_address": restaurant_address,
            "restaurant_tags": restaurant_tag_list
        }

    def parse_all_restaurant_data_from_single_page(
        self, soup: BeautifulSoup
    ) -> List:
        """
        Method for extracting the restaurant data for all the restaurants
        listed in a page
        soup: soup object
        return: results in the form of list of that specific webpage after parsing
        """
        restaurant_raw_results_list = soup.find_all(
            "div", {"data-qa": SoupAttribute.MERCHANT_CARD_WRAPPER.value}
        )

        restaurant_list = [
            RestaurantData(
                **self.parse_individual_restaurant_data_from_scrape(raw_result)
            ).model_dump_json()
            for raw_result in restaurant_raw_results_list
        ]
        return restaurant_list 

    def obtain_scraped_result_for_city(self) -> None:
        """
        Method for creating the final dataframe by parsing relevant data obtained from all pages
        JSON file (For scenario like Berlin/Frankfurt/Muenchen)
        and None (For scenario like Paris/Rome)
        """

        first_page_soup = self.extract_soup_from_webpage(self.search_url)
        if first_page_soup.title.text == "Not found":
            logger.info(
                "Unfortunately, There is no data for %s!", self.city_name.title()
            )
            return None
        first_page_result = self.parse_all_restaurant_data_from_single_page(
            first_page_soup
        )
        logger.info("First page results are successfully parsed for %s", self.city_name.title())
        self.final_result_list.extend(self.combine_json_strings(first_page_result))

        determined_last_page = self.find_last_page(first_page_soup)
        if determined_last_page:
            for page in range(2, determined_last_page + 1):
                logger.info(
                    "Now Parsing the page number: %s for %s ..........",
                    page,
                    self.city_name.title(),
                )
                nextpage_url = self.search_url + f"&page={page}"
                result_list = self.parse_all_restaurant_data_from_single_page(
                    self.extract_soup_from_webpage(nextpage_url)
                )
                self.final_result_list.extend(self.combine_json_strings(result_list))
                time.sleep(2)
        
        with open('combined_data_result.json', 'w', encoding='utf-8') as outfile:
                json.dump(self.final_result_list, outfile, indent=4, ensure_ascii=False)

        logger.info("All the pages are successfully parsed!")
         


if __name__ == "__main__":

    # Scraping for - Frankfurt - Multiple Page Results
    # QuandooRestaurantsWebScraper(city_name="rome").obtain_scraped_result_for_city()

    r = RestaurantData(
        restaurant_name='Ram Restaurant', restaurant_location='Ram Nagar',
    restaurant_score= '5.55/6',
        restaurant_cuisine='Indian',   number_of_reviews='678',
        restaurant_url= 'https://www.ramrestaurant.com/',
        restaurant_meta_data = RestaurantMetaData(
            restaurant_name='Ram Restaurant',
            restaurant_tags= ['Family-friendly ✨', 'Good for groups 🎉'],
            restaurant_address= ['Ramstraße 1', 'Ram Nagar'])
    )
    print(type(r.model_dump()))
    print("*********************************")
    print(type(r.model_dump_json()))

    
