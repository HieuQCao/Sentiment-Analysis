from GoogleNews import GoogleNews
import logging
import datetime
from datetime import date, timedelta
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from typing import List
import csv
from feedparser import parse
from textblob import TextBlob
import requests
import time
import pandas as pd
import os

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def check_internet_connection():
    """Check if there's an internet connection by attempting to reach Google."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    try:
        requests.get('http://www.google.com', headers=headers, timeout=10)
        return True
    except requests.RequestException:
        return False

class StockSentimentAnalyzer:
    def __init__(self, bull_threshold=0.05, bear_threshold=0, days=1):
        self.bull_threshold = bull_threshold
        self.bear_threshold = bear_threshold
        self.days = days

    def load_google_news_rss(self, query: str, start_date: date):
        """
        Fetch news articles via Google News RSS feed with improved date handling and logging.
        """
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        while True:
            if not check_internet_connection():
                logging.warning("No internet connection. Checking again in 1 minute...")
                time.sleep(60)
            else:
                try:
                    from_date = start_date - timedelta(days=self.days)
                    # Simplified RSS URL to ensure date-specific querying (Google RSS doesn't fully support 'from'/'to')
                    rss_url = f"https://news.google.com/rss/search?q={query.replace(' ', '+')}+after:{from_date.strftime('%Y-%m-%d')}+before:{start_date.strftime('%Y-%m-%d')}&hl=en-US&gl=US&ceid=US:en"
                    
                    logging.info(f"Fetching RSS for query: {query}, date: {start_date}")
                    response = requests.get(rss_url, headers=headers, timeout=15)  # Increased timeout
                    response.raise_for_status()
                    
                    feed = parse(response.content)
                    logging.info(f"Fetched {len(feed.entries)} articles for {query} on {start_date}")
                    return feed.entries
                except requests.exceptions.Timeout:
                    logging.warning("Request timed out. Retrying in 1 minute...")
                    time.sleep(60)
                except requests.exceptions.HTTPError as e:
                    logging.error(f"HTTP Error: {e}")
                    time.sleep(60)
                except requests.exceptions.RequestException as e:
                    logging.error(f"Request Exception: {e}")
                    time.sleep(60)
                except Exception as e:
                    logging.error(f"Unexpected error: {e}")
                    time.sleep(60)

    def analyze_sentiment(self, texts):
        scores = []
        analyzer = SentimentIntensityAnalyzer()  # Adding VADER in case
        for text in texts:
            blob = TextBlob(text)
            vader_score = analyzer.polarity_scores(text)['compound']  # VADER compound score
            scores.append(blob.sentiment.polarity)  # Keeping TextBlob 
            logging.debug(f"Text: {text[:50]}..., TextBlob: {blob.sentiment.polarity}, VADER: {vader_score}")
        return scores

    def extract_key_info(self, stock: str, start_date: date):
        """
        Extract sentiment information for a stock for a specific start date.
        """
        queries = [
            f"{stock} Outlook",
            f"{stock} News",
            f"{stock} Stock Analysis",
            f"{stock} Stock Market",
            f"{stock} Predictions",
            f"{stock} Report"
        ]
        all_articles = []

        for query in queries:
            articles = self.load_google_news_rss(query, start_date)
            all_articles.extend(articles)

        # Remove duplicates based on URL and title to avoid overlap
        unique_articles = []
        seen = set()
        for article in all_articles:
            identifier = (article.get('link', ''), article.get('title', ''))
            if identifier not in seen:
                unique_articles.append(article)
                seen.add(identifier)

        texts = [f"{article.get('title', '')} {article.get('summary', '')}" for article in unique_articles]
        sentiment_scores = self.analyze_sentiment(texts)

        # Handle case with no articles
        if not sentiment_scores:
            logging.warning(f"No articles found for {stock} on {start_date}")
            return {
                "stock": stock,
                "date": start_date.strftime('%Y-%m-%d'),
                "average_sentiment_score": 0,
                "dominant_sentiment_average": "neutral",
                "bullish_articles": 0,
                "bearish_articles": 0,
                "neutral_articles": 0,
                "dominant_sentiment_majority": "neutral",
                "num_articles_fetched": 0
            }

        # Sentiment analysis
        average_sentiment_score = sum(sentiment_scores) / len(sentiment_scores)
        dominant_sentiment_average = (
            "bullish" if average_sentiment_score >= self.bull_threshold else 
            "bearish" if average_sentiment_score <= self.bear_threshold else 
            "neutral"
        )

        sentiments_categorical = [
            "bullish" if score >= self.bull_threshold else 
            "bearish" if score <= self.bear_threshold else 
            "neutral" for score in sentiment_scores
        ]
        sentiment_counts = {
            "bullish": sentiments_categorical.count("bullish"),
            "bearish": sentiments_categorical.count("bearish"),
            "neutral": sentiments_categorical.count("neutral")
        }
        dominant_sentiment_majority = max(sentiment_counts, key=sentiment_counts.get)

        logging.info(f"{stock} {start_date}: Avg Score={average_sentiment_score}, Bull={sentiment_counts['bullish']}, Bear={sentiment_counts['bearish']}, Neutral={sentiment_counts['neutral']}")

        return {
            "stock": stock,
            "date": start_date.strftime('%Y-%m-%d'),
            "average_sentiment_score": average_sentiment_score,
            "dominant_sentiment_average": dominant_sentiment_average,
            "bullish_articles": sentiment_counts['bullish'],
            "bearish_articles": sentiment_counts['bearish'],
            "neutral_articles": sentiment_counts['neutral'],
            "dominant_sentiment_majority": dominant_sentiment_majority,
            "num_articles_fetched": len(unique_articles)
        }

    def analyze_stocks_over_range(self, stock_list: list, start_date: date, end_date: date, filename="sentiment_analysis.csv"):
        """
        Analyzes sentiment for each stock over the specified date range and saves to CSV.
        """
        results = []
        business_days = pd.bdate_range(start=start_date, end=end_date)
        
        for business_day in business_days:
            current_date = business_day.date()
            daily_results = []
            for stock in stock_list:
                result = self.extract_key_info(stock, current_date)
                daily_results.append(result)
            
            results.extend(daily_results)
            logging.info(f"Finished analysis for date: {current_date.strftime('%Y-%m-%d')}, Articles: {sum(r['num_articles_fetched'] for r in daily_results)}")
        
        local_folder = os.path.join(os.getcwd(), 'data1')
        if not os.path.exists(local_folder):
            os.makedirs(local_folder)
        
        file_path = os.path.join(local_folder, filename)
        with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = results[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in results:
                writer.writerow(row)

def main():
    logging.info("Starting main execution")
    try:
        mag7_stocks = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA','BTC']
        logging.info(f"Using stock list: {mag7_stocks}")
        
        analyzer = StockSentimentAnalyzer(bull_threshold=0.05, bear_threshold=-0.025, days=1)
        start_date = date(2021, 1, 1)
        end_date = date(2024, 12, 31)
        file_name = "sentiment_analysis_2021.csv"
        
        analyzer.analyze_stocks_over_range(mag7_stocks, start_date, end_date, filename=file_name)
        logging.info(f"Sentiment analysis completed and saved to {file_name}")
        
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()