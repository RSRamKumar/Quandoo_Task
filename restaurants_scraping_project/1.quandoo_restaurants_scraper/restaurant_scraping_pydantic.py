"""Script for scraping the Restaurant data using BeautifulSoup and
creating Pydantic Models"""

import asyncio
from functools import partial
import itertools
import json
import logging
from enum import Enum
from typing import Dict, List, Optional, Union
from inflect import engine


import requests
from bs4 import BeautifulSoup, Tag
from pydantic import AnyHttpUrl, BaseModel, Field, field_serializer, field_validator

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


class RestaurantMenu(BaseModel):
    """
    Initalizing the Pydantic data model for the restaurant menu
    """

    dish: str
    price: Optional[str]


class RestaurantMetaData(BaseModel):
    """
    Initalizing the Pydantic data model for the restaurant scraped meta data
    """

    restaurant_tags: List[str]
    restaurant_address: str
    restaurant_menu: List[RestaurantMenu]

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

    def __init__(self, city_name: str, result_limit: int = 10):
        self.city_name = city_name.lower()
        self.result_limit = result_limit
        self.base_url = "https://www.quandoo.de"
        self.search_url = f"{self.base_url}/en/result?destination={self.city_name}"
        logger.info("The city chosen for webscraping is %s", self.city_name.title())

        self.restaurant_url = None
        self.scraped_restaurants_count = 0
        self.reached_result_limit = False
        self.ordinal_number = engine()

    @staticmethod
    def extract_soup_from_webpage(url:str) -> BeautifulSoup:
        """
        Method for extracting the soup (webpage html content)
        url : webpage url
        return: the soup required for the downstream scraping step
        """
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.content, "html.parser")
        return soup

    @staticmethod
    def combine_json_strings(json_strings: List[str]) -> List[Dict]:
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
        return: last page number (for example 'Berlin') or 1 (for example 'Rostock')
        """
        pagination_info = soup.find(
            "div", {"data-qa": SoupAttribute.PAGINATION_BOX.value}
        )
        if pagination_info:
            *_, last_page_info = pagination_info.find_all("a")
            last_page_number_available = int(last_page_info.text)
            logger.info(
                "There are %s pages of results for the city %s",
                last_page_number_available,
                self.city_name.title(),
            )
            return last_page_number_available
        logger.info("We have just one page for the %s.", self.city_name)
        return 1

    async def parse_individual_restaurant_data_from_scrape(
        self, raw_restaurant_html: Tag
    ) -> Dict:
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
        restaurant_data_dict[
            "restaurant_meta_data"
        ] = await self.parse_restaurant_meta_data()
        return restaurant_data_dict

    async def parse_restaurant_meta_data(self) -> Dict:
        """
        Method for extracting the restaurant meta data namely tags and address
        and menu and returns in a dict
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
            "restaurant_tags": restaurant_tag_list,
            "restaurant_menu": await self.parse_restaurant_menu(),
        }

    async def parse_restaurant_menu(self) -> List:
        """
        Method for extracting the restaurant menu namely dish and price
        and returns it in a list
        """
        menu_list = []
        menu_data_response = self.extract_soup_from_webpage(
            f"{self.restaurant_url}/menu"
        )
        # menu_section_list = menu_data_response.find_all("div",
        # {"data-name": 'menu-section'})
        raw_menu_list = menu_data_response.find_all("h5")
        for raw_menu in raw_menu_list:
            dish = raw_menu.text
            price = raw_menu.find_next("div").text
            menu_list.append({"dish": dish, "price": price})

        return menu_list

    async def parse_all_restaurant_data_from_single_page(
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

        # restaurant_list = [
        #     RestaurantData(
        #         **(await self.parse_individual_restaurant_data_from_scrape(raw_result))
        #     ).model_dump_json()
        #     for raw_result in restaurant_raw_results_list
        # ]

        restaurant_list = []
        for raw_result in restaurant_raw_results_list:
            if self.scraped_restaurants_count >= self.result_limit:
                self.reached_result_limit = True
                break
            restaurant_list.append(
                RestaurantData(
                    **(
                        await self.parse_individual_restaurant_data_from_scrape(
                            raw_result
                        )
                    )
                ).model_dump_json()
            )
            self.scraped_restaurants_count += 1
            logger.info(
                "Scraped the %s restaurant for %s",
                 self.ordinal_number.ordinal(self.scraped_restaurants_count),
                self.city_name.title(),
            )

        return restaurant_list

    async def obtain_scraped_result_for_city(self) -> None:
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
        logger.info("The Scraping Process Started ...")
        determined_last_page = self.find_last_page(first_page_soup)
        task_list = []

        for page in range(1, determined_last_page + 1):
            if self.reached_result_limit:
                break
            logger.info(
                "Started parsing the page number: %s for %s ..........",
                page,
                self.city_name.title(),
            )
            nextpage_url = self.search_url + f"&page={page}"

            async with asyncio.TaskGroup() as tg:
                task_list.append(
                    tg.create_task(
                        self.parse_all_restaurant_data_from_single_page(
                            self.extract_soup_from_webpage(nextpage_url)
                        )
                    )
                )
        results = [task.result() for task in task_list]
        final_result_list = list(
            itertools.chain(
                *([self.combine_json_strings(result) for result in results])
            )
        )
        # for result in results:
        #    print(result)
        output_file_name = f"{self.city_name}_restaurants.json"
        with open(output_file_name, "w", encoding="utf-8") as outputfile:
            json.dump(final_result_list, outputfile, indent=4, ensure_ascii=False)

        logger.info(
            "The total number of restaurants parsed are %s",
            len(final_result_list),
        )


if __name__ == "__main__":
    # Scraping for a desired city

    scrape_restaurants_with_limit_15 = partial(
        QuandooRestaurantsWebScraper, result_limit=15
    )

    frankfurt_scraper = asyncio.run(
        scrape_restaurants_with_limit_15(
            city_name="muenchen"
        ).obtain_scraped_result_for_city()
    )
    hannover_scraper = asyncio.run(
        scrape_restaurants_with_limit_15(
            city_name="hannover"
        ).obtain_scraped_result_for_city()
    )

    # r = RestaurantData(
    #     restaurant_name="Ram Restaurant",
    #     restaurant_location="Ram Nagar",
    #     restaurant_score="5.55/6",
    #     restaurant_cuisine="Indian",
    #     number_of_reviews="678",
    #     restaurant_url="https://www.ramrestaurant.com/",
    #     restaurant_meta_data=RestaurantMetaData(
    #         restaurant_tags=["Family-friendly ✨", "Good for groups 🎉"],
    #         restaurant_address=["Ramstraße 1", "Ram Nagar"],
    #         restaurant_menu=[
    #             {"dish": "Dosa", "price": "12€"},
    #             RestaurantMenu(dish="Chicken Briyani", price="25€"),
    #         ],
    #     ),
    # )
    # print((r.model_dump()))
