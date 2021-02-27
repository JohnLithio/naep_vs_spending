"""Retrieve data for NAEP scores and school expenditures."""

import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup


class Digest:
    """Retrieve data from the Digest of Education Statistics."""

    def __init__(self, year=None):
        """Set url to be used in methods.

        Args:
            year (str, optional): Year of digest. Defaults to current year.

        """
        self.baseurl = "https://nces.ed.gov/programs/digest/"
        if year is not None:
            self.year = str(year)
            assert len(self.year) == 4, "Please enter a valid year"
            assert (
                int(self.year) > 2008
            ), "Only years 2009 and later are currently supported"
            self.tablesurl = self.baseurl + f"{year}menu_tables.asp"
        else:
            self.year = "currentyear"
            self.tablesurl = self.baseurl + "current_tables.asp"

    def get_tables_soup(self):
        """Get parsed HTML of digest tables."""
        page = requests.get(self.tablesurl)
        return BeautifulSoup(page.text, features="lxml")

    def get_per_pupil_url(self):
        """Get the URL of the table for per pupil expenditures."""
        soup = self.get_tables_soup()
        for t in soup.find_all("li"):
            if (
                "total and current expenditures per pupil in"
                " public elementary and secondary schools".replace(" ", "").replace(
                    "s", ""
                )
                in t.text.lower().replace("\r\n", "").replace(" ", "").replace("s", "")
                and len(t.find_all("li")) == 0
            ):
                table_href = t.find("a")["href"]
                perpupilurl = self.baseurl + table_href
                return perpupilurl
        for t in soup.find_all("tr"):
            if (
                "total and current expenditures per pupil in"
                " public elementary and secondary schools".replace(" ", "").replace(
                    "s", ""
                )
                in t.text.lower().replace("\r\n", "").replace(" ", "").replace("s", "")
                and len(t.find_all("tr")) == 0
            ):
                table_href = t.find("a")["href"]
                perpupilurl = self.baseurl + table_href
                return perpupilurl

    def get_per_pupil_expenditures_table(self):
        """Retrieve the table of public elementary per pupil spending."""
        self.perpupilurl = self.get_per_pupil_url()
        self.ppe = pd.read_html(self.perpupilurl, match="1929(-|–)30")[0]
        self.ppe.columns = [" ".join(col).strip() for col in self.ppe.columns.values]

        return self.ppe

    def save_ppe_table(self):
        """Save the raw data to the local raw data folder."""
        current_date = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
        self.ppe.to_feather(
            f"../data/raw/per_pupil_expenditure_{self.year}_{current_date}.feather"
        )


class Naep:
    """Retrieve NAEP data."""

    def __init__(self, year=None):
        """Set url to be used in methods.

        Args:
            year (str, optional): Year of digest. Defaults to current year.

        """
        self.baseurl = "https://nces.ed.gov/programs/digest/"
        if year is not None:
            self.year = str(year)
            assert len(self.year) == 4, "Please enter a valid year"
            assert (
                int(self.year) > 2008
            ), "Only years 2009 and later are currently supported"
            self.tablesurl = self.baseurl + f"{year}menu_tables.asp"
        else:
            self.year = "currentyear"
            self.tablesurl = self.baseurl + "current_tables.asp"
