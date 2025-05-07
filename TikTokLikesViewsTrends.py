#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from tikapi import TikAPI, ValidationException, ResponseException
from datetime import datetime, timedelta
import pandas as pd
import time

# Initialize TikAPI
api = TikAPI("8mqoTQs1AXfSs6nskRCr5obvsWVytvQ1J0YPvIS1ylfEtl2D")

# Fetch followed users
def fetch_following_users(secUid):
    try:
        response = api.public.followingList(secUid=secUid)
        users = []

        while response:
            for user_entry in response.json().get('userList', []):
                user = user_entry.get('user', {})
                secUid = user.get('secUid', '')
                username = user.get('uniqueId', '')
                users.append({"username": username, "secUid": secUid})

            nextCursor = response.json().get('nextCursor')
            if not nextCursor:
                break
            response = response.next_items()

        return users

    except (ValidationException, ResponseException) as e:
        print(f"Error fetching following list: {e}")
        return []

# Fetch posts from past 52 weeks
def fetch_posts_last_year(secUid):
    try:
        response = api.public.posts(secUid=secUid)
        posts = response.json().get('itemList', [])

        one_year_ago = datetime.utcnow() - timedelta(weeks=52)
        return [
            {
                "createTime": datetime.utcfromtimestamp(post.get("createTime")),
                "views": post.get("stats", {}).get("playCount", 0),
                "likes": post.get("stats", {}).get("diggCount", 0),
            }
            for post in posts
            if datetime.utcfromtimestamp(post.get("createTime")) >= one_year_ago
        ]
    except Exception as e:
        print(f"Error fetching posts for {secUid}: {e}")
        return []

# Create daily totals and 28-day rolling averages
def build_daily_stats(posts):
    if not posts:
        return pd.DataFrame()

    df = pd.DataFrame(posts)
    df['date'] = df['createTime'].dt.date

    daily = df.groupby('date').agg({
        'views': 'sum',
        'likes': 'sum'
    }).reset_index()

    daily['videos'] = df['date'].value_counts().sort_index().values
    daily = daily.set_index('date').asfreq('D', fill_value=0)

    daily['views_28day_avg'] = daily['views'].rolling(window=28).mean()
    daily['likes_28day_avg'] = daily['likes'].rolling(window=28).mean()
    daily['videos_28day_avg'] = daily['videos'].rolling(window=28).mean()

    return daily.reset_index()

# Main workflow
def main():
    main_secUid = "MS4wLjABAAAAboanSl94WMrjvJtHejLumdRGgy9oYuygOQfbC-iVne34BIfjcygpqSH84qsh2XcT"

    print("Fetching followed users...")
    users = fetch_following_users(main_secUid)

    all_users_df = pd.DataFrame()

    for user in users:
        print(f"Processing @{user['username']}")
        posts = fetch_posts_last_year(user['secUid'])
        df_stats = build_daily_stats(posts)

        if not df_stats.empty:
            df_stats['username'] = user['username']
            all_users_df = pd.concat([all_users_df, df_stats], ignore_index=True)
        else:
            print(f"No posts found for @{user['username']}")

    # Save to CSV for BigQuery / Looker Studio
    all_users_df.to_csv("tiktok_looker_data.csv", index=False)
    print("Saved combined data to tiktok_looker_data.csv")

if __name__ == "__main__":
    main()

