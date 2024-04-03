"""Script for scraping the Restaurant data using BeautifulSoup and
creating Pydantic Models"""

import logging
from enum import Enum
from typing import Optional, Union

import requests
import pandas as pd
from bs4 import BeautifulSoup, Tag
from pydantic import BaseModel, Field, field_serializer, field_validator

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


class RestaurantInfo(BaseModel):
    """
    Initalizing the Pydantic data model for the restaurant scraped data
    """

    # restaurant_name: str = Field(alias="Restaurant_name")
    restaurant_name: str
    restaurant_location: str
    restaurant_cuisine: str
    restaurant_score: Optional[float] = Field(default=None, ge=0, lt=6, strict=True)
    number_of_reviews: Optional[int] = Field(default=None)

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
        self.url = f"https://www.quandoo.de/en/result?destination={self.city_name}"
        logger.info("The city chosen for webscraping is %s", self.city_name.title())

        self.final_result_dataframe = pd.DataFrame()

    @staticmethod
    def extract_soup_from_webpage(url) -> BeautifulSoup:
        """
        Method for extracting the soup (webpage html content)
        url : webpage url
        return: the soup required for the downstream scraping step
        """
        response = requests.get(url, timeout=5)
        soup = BeautifulSoup(response.content, "html.parser")
        return soup

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
        return restaurant_data_dict

    def parse_all_restaurant_data_from_single_page(
        self, soup: BeautifulSoup
    ) -> pd.DataFrame:
        """
        Method for extracting the restaurant data for all the restaurants
        listed in a page
        soup: soup object
        return: results in the form of dataframe of that specific webpage after parsing
        """
        restaurant_raw_results_list = soup.find_all(
            "div", {"data-qa": SoupAttribute.MERCHANT_CARD_WRAPPER.value}
        )

        restaurant_list = [
            RestaurantInfo(
                **self.parse_individual_restaurant_data_from_scrape(raw_result)
            ).model_dump()
            for raw_result in restaurant_raw_results_list
        ]
        return pd.DataFrame(restaurant_list)

    def obtain_scraped_result_for_city(self) -> Union[pd.DataFrame, None]:
        """
        Method for creating the final dataframe by parsing relevant data obtained from all pages
        return: DataFrame (For scenario like Berlin/Frankfurt/Muenchen
        and None (For scenario like Paris/Rome)
        """

        first_page_soup = self.extract_soup_from_webpage(self.url)
        if first_page_soup.title.text == "Not found":
            logger.info(
                "Unfortunately, There is no data for %s!", self.city_name.title()
            )
            return None
        first_page_result = self.parse_all_restaurant_data_from_single_page(
            first_page_soup
        )
        self.final_result_dataframe = pd.concat(
            [self.final_result_dataframe, first_page_result], ignore_index=True
        )

        determined_last_page = self.find_last_page(first_page_soup)
        if determined_last_page:
            for page in range(2, determined_last_page + 1):
                logger.info(
                    "Now Parsing the page number: %s for %s ..........",
                    page,
                    self.city_name.title(),
                )
                nextpage_url = self.url + f"&page={page}"
                result_df = self.parse_all_restaurant_data_from_single_page(
                    self.extract_soup_from_webpage(nextpage_url)
                )
                self.final_result_dataframe = pd.concat(
                    [self.final_result_dataframe, result_df], ignore_index=True
                )
        logger.info("All the pages are successfully parsed!")
        return self.final_result_dataframe


if __name__ == "__main__":
    # r = ScrapedRestaurantResponse(
    #     Restaurant_name='Ram Restaurant', Restaurant_location='Rostock',
    # Restaurant_score= '1.55/6',
    #     Restaurant_cuisine='indian',   Number_of_reviews='78'
    # )
    # print(r.model_dump())

    # Scraping for - Frankfurt - Multiple Page Results
    webscraper_frankfurt = QuandooRestaurantsWebScraper(
        city_name="frankfurt"
    ).obtain_scraped_result_for_city()

    webscraper_frankfurt.to_csv("frankfurt_restaurants.csv", index=False)
