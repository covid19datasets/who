"""
"""
from datetime import datetime
from datetime import date
import pytz
import tabula
import pandas as pd
import git
import os
import errno
import stat
import shutil
import logging
import numpy as np
from send_log import mail


def git_access_token() -> str:
    """Access token for git repositories.

    :returns: [str] A git access token."""
    return "d26e9987762113e07845027136baaf388a0277f1"


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
    commit_message = ('fix: added sit rep for {} (It was not correctly recorded)'.format(scrape_date))

    repo = git.Repo(os.path.join(scrape_date, 'who'))
    repo.git.add(update=True)

    repo.index.commit(commit_message)
    origin = repo.remote(name='origin')
    origin.push()


def remove_readonly(func, path, exc):
    """Remove a readonly directory on unix."""
    logger = logging.getLogger('Situational Report Scraper')
    excvalue = exc[1]
    if func in (os.rmdir, os.remove) and excvalue.errno == errno.EACCES:

        # ensure parent directory is writeable too
        pardir = os.path.abspath(os.path.join(path, os.path.pardir))
        if not os.access(pardir, os.W_OK):
            os.chmod(pardir, stat.S_IRWXU| stat.S_IRWXG| stat.S_IRWXO)

        os.chmod(path, stat.S_IRWXU| stat.S_IRWXG| stat.S_IRWXO)  # 0777
        func(path)
    else:
        logger.error('ERR: Could not remove files in clean up!')
        raise


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
    """Clean the given dataframe for known issues."""
    # drop the first row because it is just a region.
    df = df.iloc[1:]
    # keep first seven columns
    df = df[df.columns[:7]]

    # Combine rows where the are empty
    # Count the empty cells
    df['count'] = df.apply(lambda x: x.count(), axis=1)

    # those will 6 empties need to be moved to join the next
    df['above_country'] = df['Country/Region'].shift(1)
    df['above_count'] = df['count'].shift(1)

    df['new_area'] = (
        np.where(df['above_count'] == 2, df['above_country'] + ' ' + df['Country/Region'], df['Country/Region'])
    )

    # Drop unwanted rows that we know are region/area titles
    df = df.dropna()
    return df[df.columns[:7]]


def get_situation_report(http: str, scrape_date) -> pd.DataFrame:
    """Extract a table from the given URL.

    :http: [str] The url to scrape from.

    :returns: A list of Pandas Dataframes for each pdf page"""
    logger = logging.getLogger('Situational Report Scraper')

    pdf_table = tabula.read_pdf(http, pages='all')
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
    for row in range(len(viable_dfs)):
        dates.append(scrape_date.strftime('%d/%m/%Y'))
        retrieved_dates.append(date_of_retrieval)
        report_nums.append(report_num.days)

    viable_dfs['Date'] = pd.Series(dates)
    viable_dfs['Retrieved'] = pd.Series(retrieved_dates)
    viable_dfs['Report Number'] = pd.Series(report_nums)

    return viable_dfs


def scrape(http, test, scrape_date):
    """Main pipeline for the scarper"""
    define_logger()
    logger = logging.getLogger('Situational Report Scraper')

    table = get_situation_report(http, scrape_date)

    repo = git_clone(
        'https://{}'.format(git_access_token()) +
        ':x-oauth-basic@github.com/covid19datasets/who',
        scrape_date.strftime('%d%m%Y')
    )

    if test.upper() == 'YES':
        repo.git.checkout('test')

    # We read the old table and take note of any significant changes.
    # These changes are logged and sent as an email.
    previous_table = pd.read_csv(os.path.join(scrape_date.strftime('%d%m%Y'), 'who', 'current.csv'), header=0)

    # We concatenate the new table onto the old one and save it:
    # We also force their to only be 10 columns to remove garbage picked up.
    #table = pd.concat([previous_table.loc[:, :11], table.loc[:, :11]])
    table = pd.concat([previous_table, table])

    if scrape_date.strftime('%d/%m/%Y') in table['Date'].unique():
        table.to_csv(os.path.join(scrape_date.strftime('%d%m%Y'), 'who', 'current.csv'), index=False)
    else:
        logger.error(
            'ERROR: Failed to scrape. The scraper has failed for an unknown reason to scrape!'
            ' No data was appended to the csv!!'
        )
        raise Exception('The data was lost during the scraping!!!')

    if test.upper() != 'YES':
        git_push(scrape_date=scrape_date)
        repo.close()

    # Cleanup:
    #shutil.rmtree('who', ignore_errors=False, onerror=remove_readonly)
