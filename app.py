import requests
import time
import json
import os


def fetch_reddit_posts(subreddit, limit=100, cache_file=None, refresh=False):
    if cache_file and os.path.exists(cache_file) and not refresh:
        print(f"Loading from cache: {cache_file}")
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"https://www.reddit.com/r/{subreddit}/.json"
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; my-reddit-script/1.0)"
    }

    posts = []
    after = None

    while len(posts) < limit:
        params = {
            "limit": min(100, limit - len(posts))
        }
        if after:
            params["after"] = after

        response = requests.get(url, headers=headers, params=params)

        if response.status_code != 200:
            raise Exception(f"Request failed: {response.status_code}")

        data = response.json()
        children = data["data"]["children"]

        if not children:
            break

        for item in children:
            post_data = item["data"]

            # Extract image if available
            image_url = None
            if "preview" in post_data:
                images = post_data["preview"].get("images", [])
                if images:
                    image_url = images[0]["source"]["url"]

            posts.append({
                "id": post_data["id"],
                "title": post_data["title"],
                "author": post_data["author"],
                "author_flair": post_data.get("author_flair_text"),
                
                # Actual content
                "is_self": post_data["is_self"],
                "text": post_data.get("selftext") or post_data["title"],
                "url": post_data["url"],  # external link or media
                "post_link": "https://www.reddit.com" + post_data["permalink"],
                
                # Media (optional)
                "image": image_url,
                
                # Engagement
                "score": post_data["score"],
                "num_comments": post_data["num_comments"],
                
                # Time
                "created_utc": post_data["created_utc"]
            })
                
            if len(posts) >= limit:
                break

        after = data["data"]["after"]
        if not after:
            break

        print(f'fetching {len(posts)}')
        time.sleep(2)        

    if cache_file:
        print(f"Saving to cache: {cache_file}")
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(posts, f, indent=2)

    return posts


# Example usage
if __name__ == "__main__":
    posts = fetch_reddit_posts(
        "UBC",
        limit=5000,
        cache_file="ubc_posts.json"
    )

    filtered = []
    for p in posts:
        if p["author_flair"] != None and 'Computer Science' in p["author_flair"]:
            filtered.append(p)

    with open('computer_science.json', "w", encoding="utf-8") as f:
        json.dump(filtered, f, indent=2)
