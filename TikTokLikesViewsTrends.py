from tikapi import TikAPI, ValidationException, ResponseException
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import bigquery
from scipy.stats import linregress
import os

# ✅ Set BigQuery credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "tiktokanalyticskey.json"

# ✅ BigQuery upload function
def upload_to_bigquery(df, table_name):
    project_id = "tiktokanalytics-459417"
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

    daily['likes_per_post'] = daily['likes'] / daily['videos'].replace(0, pd.NA)
    daily['comments_per_post'] = daily['comments'] / daily['videos'].replace(0, pd.NA)
    daily['shares_per_post'] = daily['shares'] / daily['videos'].replace(0, pd.NA)
    daily['views_per_post'] = daily['views'] / daily['videos'].replace(0, pd.NA)
    daily['engagement_rate'] = (daily['likes'] + daily['comments'] + daily['shares']) / daily['views'].replace(0, pd.NA)

    for col in ['views', 'likes', 'comments', 'shares', 'videos',
                'likes_per_post', 'comments_per_post', 'shares_per_post',
                'views_per_post', 'engagement_rate']:
        daily[f'{col}_28day_avg'] = daily[col].rolling(window=28).mean()

    return daily.reset_index()

# Calculate growth slopes and custom metrics
def calculate_slopes(df, username):
    today = df['date'].max()
    results = []

    for metric in ['views', 'likes', 'engagement_rate']:
        for label, days in [('slope_3mo', 90), ('slope_6mo', 180), ('slope_12mo', 365)]:
            cutoff = today - timedelta(days=days)
            subset = df[df['date'] >= cutoff]

            if len(subset) > 1:
                x = pd.to_datetime(subset['date']).map(datetime.toordinal)
                y = subset[metric]
                slope = linregress(x, y).slope
            else:
                slope = None

            results.append({
                'username': username,
                'metric': metric,
                'slope_window': label,
                'slope': slope
            })

    # Additional custom metrics based on the last 2 weeks vs previous 2 weeks
    recent = df[df['date'] >= (today - timedelta(days=14))]
    prev = df[(df['date'] < (today - timedelta(days=14))) & (df['date'] >= (today - timedelta(days=28)))]

    if not recent.empty and not prev.empty:
        velocity = recent['views'].sum() - prev['views'].sum()
        momentum_score = (recent['views'].sum() / prev['views'].sum()) if prev['views'].sum() > 0 else None
        heat_score = velocity * (recent['engagement_rate'].mean() if not pd.isna(recent['engagement_rate'].mean()) else 1)

        results.append({
            'username': username,
            'metric': 'velocity',
            'slope_window': '14_day_delta',
            'slope': velocity
        })
        results.append({
            'username': username,
            'metric': 'momentum_score',
            'slope_window': '2wk_ratio',
            'slope': momentum_score
        })
        results.append({
            'username': username,
            'metric': 'heat_score',
            'slope_window': 'engagement_weighted',
            'slope': heat_score
        })

    return results

# Main workflow
def main():
    main_secUid = "MS4wLjABAAAAboanSl94WMrjvJtHejLumdRGgy9oYuygOQfbC-iVne34BIfjcygpqSH84qsh2XcT"
    print("Fetching followed users...")
    users = fetch_following_users(main_secUid)

    all_users_df = pd.DataFrame()
    slope_summaries = []

    for user in users:
        print(f"Processing @{user['username']}")
        posts = fetch_posts_last_year(user['secUid'])
        df_stats = build_daily_stats(posts)

        if not df_stats.empty:
            df_stats['username'] = user['username']
            all_users_df = pd.concat([all_users_df, df_stats], ignore_index=True)
            slope_summaries.extend(calculate_slopes(df_stats, user['username']))
        else:
            print(f"No posts found for @{user['username']}")

    if not all_users_df.empty:
        upload_to_bigquery(all_users_df, table_name="likes_views_engagement")

    if slope_summaries:
        slope_df = pd.DataFrame(slope_summaries)

        # Calculate heat_score_plus (normalized where 100 = average)
        heat_scores = slope_df[slope_df['metric'] == 'heat_score']
        if not heat_scores.empty:
            avg_heat = heat_scores['slope'].mean()
            slope_df.loc[slope_df['metric'] == 'heat_score', 'heat_score_plus'] = (
                slope_df[slope_df['metric'] == 'heat_score']['slope'] / avg_heat * 100
            )

        upload_to_bigquery(slope_df, table_name="artist_growth_summary")
    else:
        print("⚠️ No slope data to upload.")

if __name__ == "__main__":
    main()
