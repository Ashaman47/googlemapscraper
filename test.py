"""This script serves as an example on how to use Python
   & Playwright to scrape/extract data from Google Maps"""
import pyap
from pandas import read_csv
from playwright.sync_api import sync_playwright
from dataclasses import dataclass, asdict, field
import pandas as pd
import argparse
import pyodbc
from sqlalchemy import create_engine, exc


@dataclass
class Business:
    """holds business data"""

    name: str = None
    address: str = None
    state: str = None
    city: str = None
    zip_code: str = None
    business_type: str = None
    website: str = None
    phone_number: str = None


@dataclass
class BusinessList:
    """holds list of Business objects,
    and save to both excel and csv
    """

    business_list: list[Business] = field(default_factory=list)

    def dataframe(self):
        """transform business_list to pandas dataframe

        Returns: pandas dataframe
        """
        return pd.json_normalize(
            (asdict(business) for business in self.business_list), sep="_"
        )

    def save_to_sql(self, search):
        # Create a connection string
        conn_str = (
            r'DRIVER={SQL Server};'
            r'SERVER=RYAN\MSSQLSERVER01;'
            r'DATABASE=master;'
            r'Trusted_Connection=yes;'

        )
        # Establish a connection
        conn = pyodbc.connect(conn_str)
        for business in self.business_list:
            if "Loans" not in business.name and len(business.address) > 0:
                try:
                    conn.cursor().execute(
                        "INSERT INTO businesses (name, address, state,city,zip_code,business_type, website,"
                        "phone_number) VALUES(?,?,?,?,?,?,?,?)",
                        (business.name, business.address, business.state, business.city, business.zip_code,
                         business.business_type, business.website, business.phone_number))
                except pyodbc.IntegrityError:
                    pass

        conn.commit()

    def save_to_excel(self, filename):
        """saves pandas dataframe to excel (xlsx) file

        Args:
            filename (str): filename
        """
        self.dataframe().to_excel(f"{filename}.xlsx", index=False)

    def save_to_csv(self, filename):
        """saves pandas dataframe to csv file

        Args:
            filename (str): filename
        """
        self.dataframe().to_csv(f"{filename}.csv", index=False)


def main():
    with (sync_playwright() as p):
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()

        page.goto("https://www.google.com/maps", timeout=60000)
        # wait is added for dev phase. can remove it in production

        page.locator('//input[@id="searchboxinput"]').fill(search_for)

        page.keyboard.press("Enter")

        # scrolling
        page.hover('//a[contains(@href, "https://www.google.com/maps/place")]')

        # this variable is used to detect if the bot
        # scraped the same number of listings in the previous iteration
        previously_counted = 0
        while True:
            page.mouse.wheel(0, 10000)
            page.wait_for_timeout(2000)
            if (
                    page.locator(
                        '//a[contains(@href, "https://www.google.com/maps/place")]'
                    ).count()
                    >= total
            ):
                listings = page.locator(
                    '//a[contains(@href, "https://www.google.com/maps/place")]'
                ).all()[:total]
                listings = [listing.locator("xpath=..") for listing in listings]
                print(f"Total Scraped: {len(listings)}")
                break
            else:
                # logic to break from loop to not run infinitely
                # in case arrived at all available listings
                if (
                        page.locator(
                            '//a[contains(@href, "https://www.google.com/maps/place")]'
                        ).count()
                        == previously_counted
                ):
                    listings = page.locator(
                        '//a[contains(@href, "https://www.google.com/maps/place")]'
                    ).all()
                    print(f"Arrived at all available\nTotal Scraped: {len(listings)}")
                    listings = [listing.locator("xpath=..") for listing in listings]
                    break
                else:
                    previously_counted = page.locator(
                        '//a[contains(@href, "https://www.google.com/maps/place")]'
                    ).count()
                    print(
                        f"Currently Scraped: ",
                        page.locator(
                            '//a[contains(@href, "https://www.google.com/maps/place")]'
                        ).count(),
                    )

        business_list = BusinessList()

        # scraping
        for listing in listings:
            listing.click()
            page.wait_for_timeout(500)

            name_xpath = '//div[contains(@class, "fontHeadlineSmall")]'
            address_xpath = '//button[@data-item-id="address"]//div[contains(@class, "fontBodyMedium")]'
            website_xpath = '//a[@data-item-id="authority"]//div[contains(@class, "fontBodyMedium")]'
            phone_number_xpath = '//button[contains(@data-item-id, "phone:tel:")]//div[contains(@class, "fontBodyMedium")]'

            business = Business()

            if listing.locator(name_xpath).count() > 0:
                business.name = listing.locator(name_xpath).inner_text()
            else:
                business.name = ""
            if page.locator(address_xpath).count() > 0:
                address = page.locator(address_xpath).inner_text().split(',')
                business.address = address[0:-2]
                if len(business.address) > 0:
                    business.address = business.address[0]
                business.city = address[-2]
                state_zip = address[-1].split()
                business.state = state_zip[0]
                if len(state_zip) > 1:
                    business.zip_code = state_zip[1]
            else:
                business.address = ""
            if page.locator(website_xpath).count() > 0:
                business.website = page.locator(website_xpath).inner_text()
            else:
                business.website = ""
            if page.locator(phone_number_xpath).count() > 0:
                business.phone_number = page.locator(phone_number_xpath).inner_text()
            else:
                business.phone_number = ""

            business.business_type = search

            business_list.business_list.append(business)

        # saving to both excel and csv just to showcase the features.
        business_list.save_to_sql(search)

        browser.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--search", type=str)
    parser.add_argument("-t", "--total", type=int)
    parser.add_argument("-l", "--location", type=str)
    args = parser.parse_args()
    import csv

    data = read_csv("uscities.csv")
    cities = data['city'].tolist()
    states = data['state_name'].tolist()
    for i in range(len(cities)):
        location = cities[i] + " " + states[i]

        if args.search and location:
            search_for = f'{args.search} {location}'
            search = f'{args.search}'.lower()
        else:
            # in case no arguments passed
            # the scraper will search by defaukt for:
            search_for = "dentist new york"

        # total number of products to scrape. Default is 120
        if args.total:
            total = args.total
        else:
            total = 120
        main()
