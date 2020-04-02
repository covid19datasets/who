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
    print(df.keys())
    # keep first seven columns
    df = df[df.columns[:7]]

    # TODO
    #   REGEX needs to be used to check whether or not the first character is there in the below strings etc..
    unwanted_titles = [
        "Western Pacific Region",
        "Territories**",
        "Territory/Area†",
        "European Region",
        "South-East Asia Region",
        "Eastern Mediterranean Region",
        "Region of the Americas",
        "African Region",
        "Subtotal for all",
        "regions",
        "Grand total",
        "astern Mediterranean Region",
        "erritories**",
        "egion of the Americas",
        "outh-East Asia Region",
        "Reporting Country/"
    ]

    # Combine rows where the are empty
    # Count the empty cells
    df['count'] = df.apply(lambda x: x.count(), axis=1)

    # those will 6 empties need to be moved to join the next
    df['above_country'] = df['Territory/Area'].shift(1)
    df['above_count'] = df['count'].shift(1)

    df['new_area'] = (
        np.where(df['above_count'] == 2, df['above_country'] + ' ' + df['Territory/Area'], df['Territory/Area'])
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

    # The current format is 7 columns with unknown amount of rows.
    # In case a new table is added we will automatically remove it.
    for df in pdf_table:
        if len(df.keys()) is not 7:
            try:
                pdf_table.remove(df)
            except ValueError:
                logger.error('Err: Failed to remove an entry of incorrect dimensions')
            logger.warning('WARN: We have detected a new format of table in today\'s report')

    # If the table is of a different format the scraper will fail so we throw an
    # exception.
    if len(pdf_table) is 0:
        logger.error('ERR: No Table was found in the PDF or the format was changed!')
        mail('ERROR: Failed to scrape.',
             'The scraper could not find table of the correct format to scrape!')
        raise ValueError('The table was of the incorrect proportions or missing!')

    # We set each of the data frames to have the new keys:
    for df in pdf_table:
        try:
            df.columns = [
                'Territory/Area',
                'Cumulative Confirmed Cases',
                'Total New Confirmed Cases',
                'Cumulative Deaths',
                'Total New Deaths',
                'Classification of Transmission',
                'Days Since Previous Reported Case'
            ]
        except ValueError:
            logger.warning('WARN: A table of incorrect size attempted to write!')

    # We concatenate the cleaned up list of dataframes:
    pdf_table = pd.concat(pdf_table)

    # We clean the table:
    pdf_table = clean(pdf_table)

    dates = []
    retrieved_dates = []

    date_of_retrieval = datetime.now(pytz.timezone('Australia/Canberra'))
    for row in range(len(pdf_table)):
        dates.append(scrape_date)
        retrieved_dates.append(date_of_retrieval)

    pdf_table['date'] = pd.Series(dates)
    pdf_table['retrieved'] = pd.Series(retrieved_dates)

    return pdf_table


def scrape(http, test, scrape_date):
    """Main pipeline for the scarper"""
    define_logger()

    table = get_situation_report(http, scrape_date)

    repo = git_clone(
        'https://{}'.format(git_access_token()) +
        ':x-oauth-basic@github.com/covid19datasets/who',
        scrape_date
    )

    if test.upper() == 'YES':
        repo.git.checkout('test')

    # We read the old table and take note of any significant changes.
    # These changes are logged and sent as an email.
    previous_table = pd.read_csv(os.path.join(scrape_date, 'who', 'current.csv'), header=0)

    # We concatenate the new table onto the old one and save it:
    table = pd.concat([previous_table, table])
    table.to_csv(os.path.join(scrape_date, 'who', 'current.csv'), index=False)

    if test.upper() != 'YES':
        git_push(scrape_date=scrape_date)
        repo.close()

    # Cleanup:
    #shutil.rmtree('who', ignore_errors=False, onerror=remove_readonly)
