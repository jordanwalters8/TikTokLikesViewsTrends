from tikapi import TikAPI, ValidationException, ResponseException
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import bigquery
import os

# ✅ Set BigQuery credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "tiktokanalyticskey.json"

# ✅ BigQuery upload function
def upload_to_bigquery(df, table_name):
    project_id = "tiktokanalytics-459417"  # 🔁 Replace this
    dataset_id = "tiktok_data"
    table_id = f"{project_id}.{dataset_id}.{table_name}"

    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(autodetect=True)

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"✅ Uploaded {len(df)} rows to {table_id}")

# 🔍 TikAPI key
api = TikAPI(os.environ.get("TIKAPI_KEY"))

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
                "comments": post.get("stats", {}).get("commentCount", 0),
                "shares": post.get("stats", {}).get("shareCount", 0),
            }
            for post in posts
            if datetime.utcfromtimestamp(post.get("createTime")) >= one_year_ago
        ]
    except Exception as e:
        print(f"Error fetching posts for {secUid}: {e}")
        return []

def build_daily_stats(posts):
    if not posts:
        return pd.DataFrame()

    df = pd.DataFrame(posts)
    df['date'] = df['createTime'].dt.date

    daily = df.groupby('date').agg({
        'views': 'sum',
        'likes': 'sum',
        'comments': 'sum',
        'shares': 'sum'
    }).reset_index()

    daily['videos'] = df['date'].value_counts().sort_index().values
    daily = daily.set_index('date').asfreq('D', fill_value=0)

    # Per-post engagement metrics
    daily['likes_per_post'] = daily['likes'] / daily['videos'].replace(0, pd.NA)
    daily['comments_per_post'] = daily['comments'] / daily['videos'].replace(0, pd.NA)
    daily['shares_per_post'] = daily['shares'] / daily['videos'].replace(0, pd.NA)
    daily['views_per_post'] = daily['views'] / daily['videos'].replace(0, pd.NA)

    # 🔥 Overall engagement rate per day
    daily['engagement_rate'] = (
        (daily['likes'] + daily['comments'] + daily['shares']) / daily['views'].replace(0, pd.NA)
    )

    # Rolling averages
    for col in ['views', 'likes', 'comments', 'shares', 'videos',
                'likes_per_post', 'comments_per_post', 'shares_per_post',
                'views_per_post', 'engagement_rate']:
        daily[f'{col}_28day_avg'] = daily[col].rolling(window=28).mean()

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

    if not all_users_df.empty:
        upload_to_bigquery(all_users_df, table_name="likes_views_engagement")
    else:
        print("⚠️ No data to upload.")

if __name__ == "__main__":
    main()

# Sample data for two users over a few days
sample_data = pd.DataFrame({
    "username": ["@user1", "@user1", "@user2", "@user2"],
    "date": [datetime(2025, 4, 1).date(), datetime(2025, 4, 2).date(),
             datetime(2025, 4, 1).date(), datetime(2025, 4, 3).date()],
    "views": [12000, 6000, 8000, 9000],
    "likes": [800, 400, 600, 700],
    "comments": [100, 50, 70, 80],
    "shares": [50, 25, 35, 40],
    "videos": [2, 1, 1, 1],
    "likes_per_post": [400.0, 400.0, 600.0, 700.0],
    "comments_per_post": [50.0, 50.0, 70.0, 80.0],
    "shares_per_post": [25.0, 25.0, 35.0, 40.0],
    "views_28day_avg": [None]*4,
    "likes_28day_avg": [None]*4,
    "comments_28day_avg": [None]*4,
    "shares_28day_avg": [None]*4,
    "videos_28day_avg": [None]*4,
    "likes_per_post_28day_avg": [None]*4,
    "comments_per_post_28day_avg": [None]*4,
    "shares_per_post_28day_avg": [None]*4
})

# Call your BigQuery upload function
upload_to_bigquery(sample_data, table_name="likes_views_engagement_test")