import praw
import pandas as pd

import datetime as dt
import ExasolConnector
import os
reddit_read_only = praw.Reddit(client_id="client_id",         # your client id
                               client_secret=os.environ.get("REDDIT_SECRET"),      # your client secret
                               user_agent="DataChallenge")
subreddits = ['de', 'fragreddit']
companies = ['auto', 'flixbus', 'ryanair', 'bahn', 'oeffis']

def get_reddit_data(subreddit, company):
    """
    Search a subreddit for posts about a company
    :param subreddit: The subreddit to search
    :param company: The company to search for
    :return: The reddit data dataframe
    """
    subreddit = reddit_read_only.subreddit(subreddit)

    topics_dict = {
                    "likes": [],

                    "id": [],

                    "replies": [],

                    "created_at": [],

                    "text": []}
    query = company
    if company == 'bahn':
        query = 'bahn db'
    elif company == 'oeffis':
        query = 'öffis öpnv nahverkehr'

    for submission in subreddit.search(query, limit=1000):
        if company in submission.selftext.lower():

            topics_dict["likes"].append(submission.score)

            topics_dict["id"].append(submission.id)

            topics_dict["replies"].append(submission.num_comments)

            topics_dict["created_at"].append(dt.datetime.fromtimestamp(submission.created))

            topics_dict["text"].append(submission.selftext)

        url = "https://www.reddit.com" + submission.permalink

        comments = reddit_read_only.submission(url=url)
        comments.comments.replace_more(limit=None)

        # Also search all comments
        for x in comments.comments:
            if company in x.body.lower():

                topics_dict["likes"].append(x.score)

                topics_dict["id"].append(x.id)

                topics_dict["replies"].append(len(x.replies))

                topics_dict["text"].append(x.body)

                topics_dict["created_at"].append(dt.datetime.fromtimestamp(x.created_utc))

    df_data = pd.DataFrame(topics_dict)
    df_data['company'] = company
    df_data['rating'] = None
    df_data['social_media'] = 'reddit'
    df_data = df_data[['id', 'likes', 'replies', 'social_media', 'rating', 'created_at', 'text', 'company']]
    return df_data.replace(r'\n', ' ', regex=True)


if __name__ == '__main__':
    data = None
    for company in companies:
        company_data = None
        for subreddit in subreddits:
            print(company, subreddit)
            if company_data is None:
                company_data = get_reddit_data(subreddit, company)
            else:
                company_data = pd.concat([
                    company_data,
                    get_reddit_data(subreddit, company)
                ])
        if data is None:
            data = company_data
        else:
            data = pd.concat([
                data,
                company_data
            ])

    data['id'] = data['id'].apply(lambda x: x + '_reddit')
    data = data.replace(r'\n', ' ', regex=True)
    data = data[['id', 'likes', 'replies', 'social_media', 'rating', 'created_at', 'text', 'company']]
    connector = ExasolConnector.ExasolConnector()
    connector.to_db(data, 'datachallenge')

    data.to_csv('data_reddit.csv')