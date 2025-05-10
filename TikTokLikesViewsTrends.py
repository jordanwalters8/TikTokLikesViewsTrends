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

# Create daily totals and rolling engagement metrics
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

    # Rolling averages
    for col in ['views', 'likes', 'comments', 'shares', 'videos', 
                'likes_per_post', 'comments_per_post', 'shares_per_post']:
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
