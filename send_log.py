""""""
import yagmail


def mail(subject, contents):
    yag = yagmail.SMTP(user='covid.scraper@gmail.com', password='lkwyoezdembekvoq')

    mailing_list = []
    with open('mailing_list.txt', 'r') as file:
        lines = file.readlines()
        for line in lines:
            mailing_list.append(line)

    for email in mailing_list:
        yag.send(email, subject, contents, r'.log')
