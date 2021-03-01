"""Retrieve data for school expenditures."""

import codecs
import numpy as np
import pandas as pd
import pathlib
import requests
from bs4 import BeautifulSoup


class Digest:
    """Retrieve data from the Digest of Education Statistics."""

    def __init__(self, year="current"):
        """Set url to be used in methods.

        Args:
            year (str, optional): Year of digest. Defaults to current year.

        """
        # Set URL depending on the year
        self.baseurl = "https://nces.ed.gov/programs/digest/"
        if year != "current":
            self.year = str(year)
            assert len(self.year) == 4, "Please enter a valid year"
            assert (
                int(self.year) > 2008
            ), "Only years 2009 and later are currently supported"
            self.tablesurl = self.baseurl + f"{self.year}menu_tables.asp"
        else:
            self.year = "currentyear"
            self.tablesurl = self.baseurl + "current_tables.asp"

        # Create data directory if it doesn't exist
        pathlib.Path("../data/raw").mkdir(parents=True, exist_ok=True)
        pathlib.Path("../data/clean").mkdir(parents=True, exist_ok=True)

        # File paths for saving data
        self.tables_html_filepath = f"../data/raw/tables_{self.year}.html"
        self.ppe_html_filepath = f"../data/raw/per_pupil_expenditure_{self.year}.html"
        self.ppe_clean_filepath = (
            f"../data/clean/per_pupil_expenditure_{self.year}.feather"
        )

    def save_tables_html(self):
        """Save the HTML for the tables."""
        with codecs.open(self.tables_html_filepath, "w", "utf-8") as f:
            f.write(requests.get(self.tablesurl).text)

    def get_tables_soup(self):
        """Get parsed HTML of digest tables."""
        # Retrieve and save HTML if it hasn't been saved already
        if not pathlib.Path(self.tables_html_filepath).is_file():
            self.save_tables_html()

        with open(self.tables_html_filepath) as f:
            page = f.read()
        return BeautifulSoup(page, features="lxml")

    def get_per_pupil_expenditures_url(self):
        """Get the URL of the table for per pupil expenditures."""
        soup = self.get_tables_soup()
        tags = ["li", "tr"]
        for tag in tags:
            for t in soup.find_all(tag):
                if (
                    "total and current expenditures per pupil in"
                    " public elementary and secondary schools".replace(" ", "").replace(
                        "s", ""
                    )
                    in t.text.lower()
                    .replace("\r\n", "")
                    .replace("\n", "")
                    .replace(" ", "")
                    .replace("s", "")
                    and len(t.find_all(tag)) == 0
                ):
                    table_href = t.find("a")["href"]
                    ppe_url = self.baseurl + table_href
                    return ppe_url

    def save_per_pupil_expenditures_html(self):
        """Save the HTML for the per pupil expenditures table."""
        self.ppe_url = self.get_per_pupil_expenditures_url()
        with open(self.ppe_html_filepath, "w") as f:
            f.write(requests.get(self.ppe_url).text)

    def clean_per_pupil_expenditures_table(self, df):
        """Clean and process the per pupil expenditures table."""
        columns = [
            "year",
            "avgdaily_unadj_total",
            "_",
            "avgdaily_unadj_current",
            "avgdaily_adj_total",
            "_",
            "avgdaily_adj_current",
            "fall_unadj_total",
            "_",
            "fall_unadj_current",
            "fall_adj_total",
            "_",
            "fall_adj_current",
            "pct_change",
        ]
        df.columns = columns
        df = (
            df.loc[(df["year"].notna()) & (df["year"].str.len() >= 4)]  # Drop null rows
            .drop(columns=["_"])  # Remove unnecessary columns
            .assign(
                year=lambda x: x["year"].str[:4].astype(int)
                + 1,  # Use 2020 for 2019-20 school year
                pct_change=lambda x: pd.to_numeric(x["pct_change"], errors="coerce"),
            )
            .replace(r"\$", "", regex=True)
            .astype(float)
            .assign(year=lambda x: x["year"].astype(int))
        )
        df = (
            df.set_index("year")
            .reindex(
                pd.Index(
                    np.arange(df["year"].min(), df["year"].max() + 1, 1), name="year"
                )
            )
            .reset_index()
            .interpolate()  # Fill in missing years using linear interpolation
            .assign(
                pct_change=lambda x: (
                    x["fall_adj_current"] - x["fall_adj_current"].shift(1)
                )
                / x["fall_adj_current"].shift(1),
            )
        )

        return df

    def save_per_pupil_expenditures_table(self):
        """Retrieve the table of public elementary per pupil spending."""
        # Retrieve and save HTML if it hasn't been saved already
        if not pathlib.Path(self.ppe_html_filepath).is_file():
            self.save_per_pupil_expenditures_html()

        self.ppe = pd.read_html(self.ppe_html_filepath, match="1929(-|–)30")[0]
        # self.ppe.columns = [" ".join(col).strip() for col in self.ppe.columns.values]
        self.ppe_clean = self.clean_per_pupil_expenditures_table(self.ppe)

        self.ppe_clean.to_feather(self.ppe_clean_filepath)

        return self.ppe_clean

    def k12_total_cost(self, df, grad_year, column="avgdaily_adj_total"):
        """Get the total per pupil cost for a student graduating in a given year.

        Args:
            df (dataframe): Dataframe used for calculations
            grad_year (str): Calculation will be done for the 13 years leading up to this.
                i.e. if grad_year="2016", the calculation will sum up 2004 through 2016.
            column (str, optional): Column to use for calculation. Defaults to "avgdaily_adj_total".

        Returns:
            float: Sum of the costs

        """
        return df.loc[
            (df["year"].astype(float) <= int(grad_year))
            & (df["year"].astype(float) >= int(grad_year) - 12),
            column,
        ].sum()

    def k12_total_cost_all_years(self, df, column="avgdaily_adj_total"):
        """Get the total cost for all years in the dataset.

        Args:
            df (dataframe): Dataframe used for calculations
            column (str, optional): Column to use for calculation. Defaults to "avgdaily_adj_total".

        Returns:
            pandas Series: Costs for every year in dataset

        """
        min_year = df["year"].astype(int).min()
        max_year = df["year"].astype(int).max()
        costs = [
            self.k12_total_cost(df, year, column)
            for year in range(min_year + 12, max_year + 1)
        ]
        return pd.Series([None] * 12 + costs)
