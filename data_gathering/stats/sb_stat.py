import pandas as pd


class SbStat:
    def __init__(self, df):
        self.df = df
        self.df_count = len(df.index)

    def biggest_amount_of_advertisement_by_decade(self):
        min_year = self.df.year.min()
        max_year = self.df.year.max()
        max_number_of_advs = 0
        decade_starting_year = min_year

        for x in range(max_year - min_year):
            number_of_advs = self.df.loc[(
                self.df['year'] > min_year + x) & (self.df['year'] < min_year + x + 10)]['name'].count()
            if max_number_of_advs < number_of_advs:
                max_number_of_advs = number_of_advs
                decade_starting_year = min_year + x

        return decade_starting_year, decade_starting_year + 10, max_number_of_advs

    def average_price(self, decade_starting_date, length):
        mean = self.df.loc[(self.df['year'] > decade_starting_date) & (self.df['year'] < decade_starting_date + 10)
                           & (self.df['length'] >= length[0]) & (self.df['length'] <= length[-1] + 1)]['price'].mean()
        return int(mean)
