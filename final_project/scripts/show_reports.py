"""Show live, batch and unified minute metrics and the current T."""
from ch_client import query

if __name__ == "__main__":
    for title, sql in [
        ("T (cutover)", "SELECT t FROM report_cutover FINAL WHERE id=1"),
        ("Realtime minutes", "SELECT count(),sum(views),sum(watched_seconds) FROM live_minute"),
        ("Published batch minutes", "SELECT count(),sum(views),sum(watched_seconds) FROM published_minute"),
        ("Final report", "SELECT count(),sum(views),sum(watched_seconds) FROM final_minute"),
        ("Daily country/tier", "SELECT * FROM published_daily_country_tier ORDER BY day,country,subscription_tier LIMIT 20"),
        ("Daily top5", "SELECT * FROM published_top5 ORDER BY day,rank LIMIT 20"),
        ("Search words", "SELECT * FROM published_search_words ORDER BY day,word LIMIT 20"),
    ]:
        print("\n==", title, "==")
        print(query(sql))
