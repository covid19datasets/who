"""
"""
from datetime import datetime
from datetime import date
import pytz
from tabula.io import read_pdf
import pandas as pd
import git
import os
import logging
from send_log import mail


def git_clone(http: str, scrape_date) -> git.Repo:
    """Clone a git repo

    :http: [str] Link to the repo to clone.
    """
    os.mkdir('{}'.format(scrape_date))
    git.Git(r'./{}'.format(scrape_date)).clone(http)
    cloned_repo = git.Repo(os.path.join(scrape_date, 'who'))
    assert cloned_repo.__class__ is git.Repo  # clone an existing repository
    assert cloned_repo.init(os.path.join(scrape_date, 'who')).__class__ is git.Repo

    return cloned_repo


def git_push(scrape_date):
    """Push commit to repo."""
    commit_message = ('Automatic Update: added sit rep for {}.'.format(scrape_date))

    repo = git.Repo(os.path.join(scrape_date, 'who'))
    repo.git.add(update=True)

    repo.index.commit(commit_message)
    origin = repo.remote(name='origin')
    origin.push()


def define_logger():
    logger = logging.getLogger('Situational Report Scraper')

    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    fh = logging.FileHandler('.log')
    logger.addHandler(fh)

    logger.setLevel(logging.WARNING)


def clean(df):
    """Clean the given data frame for known issues."""
    # drop the first row
    df = df.iloc[1:]

    # keep first seven columns, label columns (because i'm useless at indexing)
    df = df[df.columns[:7]]

    unwanted_titles = ["Western Pacific Region", "Territories**", "Territory/Area†", "European Region",
                       "South-East Asia Region",
                       "Eastern Mediterranean Region", "Region of the Americas", "African Region", "Subtotal for all",
                       "regions", "Grand total", "astern Mediterranean Region", "erritories**", "egion of the Americas",
                       "outh-East Asia Region", "Reporting Country/"]

    # Drop unwanted rows that we know are region/area titles
    df = df[~df['Confirmed'].isin(unwanted_titles)]
    df = df[~df['Country/Region'].isin(unwanted_titles)]

    # Combine rows where they are empty
    # Count the completed cells per row
    df['count'] = df.apply(lambda x: x.count(), axis=1)
    df = df.reset_index()

    # if the count is 2 then the value has to be inserted in front of the row below:
    for i in df.index:
        if i is df.index[-1]:
            break
        if df['count'][i] == 2:
            df.at[i + 1, 'Country/Region'] = str(df['Country/Region'][i]) + ' ' + str(df['Country/Region'][i + 1])

    # drop the na columns
    df = df[df['count'] > 2]

    # drop the na rows - need to do an 'awake' eyeball of these before I drop them
    df = df[~df['Country/Region'].isna()]
    df = df[~df['Country/Region'].str.contains("Total")]
    df = df[~df['Country/Region'].str.contains("total")]

    return df[df.columns[:7]]


def get_situation_report(http: str, scrape_date) -> pd.DataFrame:
    """Extract a table from the given URL.

    :http: [str] The url to scrape from.

    :returns: A list of Pandas Dataframes for each pdf page"""
    logger = logging.getLogger('Situational Report Scraper')

    pdf_table = read_pdf(http, pages='all')
    viable_dfs = []

    # The current format is 7 columns with unknown amount of rows.
    # In case a new table is added we will automatically remove it.
    for df in pdf_table:
        if len(df.keys()) is 7:
            try:
                # We set each of the data frames to have the new keys:
                df.columns = [
                    'Country/Region',
                    'Cumulative Confirmed Cases',
                    'Total New Confirmed Cases',
                    'Cumulative Deaths',
                    'Total New Deaths',
                    'Classification of Transmission',
                    'Days Since Previous Reported Case'
                ]
                viable_dfs.append(df)
            except ValueError:
                logger.warning('A table of incorrect size attempted to write!')
        else:
            try:
                pdf_table.remove(df)
            except ValueError:
                logger.warning('Failed to remove an entry of incorrect dimensions')
            logger.warning('We have detected a new format of table in today\'s report')

    # If the table is of a different format the scraper will fail so we throw an
    # exception.
    if len(pdf_table) is 0:
        logger.error('No Table was found in the PDF or the format was changed!')
        mail('ERROR: Failed to scrape.',
             'The scraper could not find table of the correct format to scrape!')
        raise ValueError('The table was of the incorrect proportions or missing!')

    # We concatenate the cleaned up list of data frames:
    viable_dfs = pd.concat(viable_dfs)
    # We clean the table:
    viable_dfs = clean(viable_dfs)

    dates = []
    retrieved_dates = []
    report_nums = []

    report_num = scrape_date - date(2020, 1, 20)
    date_of_retrieval = datetime.now(pytz.timezone('Australia/Canberra'))
    for row in range(viable_dfs.index[-1]):
        dates.append(scrape_date.strftime('%d/%m/%Y'))
        retrieved_dates.append(date_of_retrieval)
        report_nums.append(report_num.days)

    viable_dfs['Date'] = pd.Series(dates, index=viable_dfs.index)
    viable_dfs['Retrieved'] = pd.Series(retrieved_dates, index=viable_dfs.index)
    viable_dfs['Report Number'] = pd.Series(report_nums, index=viable_dfs.index)

    return viable_dfs


def scrape(http, branch, scrape_date, git_access_token):
    """Main pipeline for the scarper"""
    define_logger()
    table = get_situation_report(http, scrape_date)
    repo = git_clone(
        'https://{}'.format(git_access_token) +
        ':x-oauth-basic@github.com/covid19datasets/who',
        scrape_date.strftime('%d%m%Y')
    )

    # We checkout the predefined branch in the args:
    repo.git.checkout(branch)

    # We read the old table and take note of any significant changes.
    # These changes are logged and sent as an email.
    previous_historic = pd.read_csv(os.path.join(scrape_date.strftime('%d%m%Y'), 'who', 'historic.csv'), header=0)
    previous_today = pd.read_csv(os.path.join(scrape_date.strftime('%d%m%Y'), 'who', 'today.csv'), header=0)

    new_countries = set(previous_today['Country/Region']) - set(table['Country/Region'])
    old_countries = set(table['Country/Region']) - set(previous_today['Country/Region'])

    # We write today's table:
    table.to_csv(os.path.join(scrape_date.strftime('%d%m%Y'), 'who', 'today.csv'), index=False) #

    # We concatenate the new table onto the old one and save it:
    table = pd.concat([previous_historic, table])

    table.to_csv(os.path.join(scrape_date.strftime('%d%m%Y'), 'who', 'historic.csv'), index=False)

    git_push(scrape_date=scrape_date.strftime('%d%m%Y'))
    repo.close()

    # We return the difference of the countries between yesterday and today so they can be added
    # to the email message.
    return {'new_countries': new_countries, 'old_countries': old_countries}
