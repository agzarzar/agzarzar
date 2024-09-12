import requests
import aiohttp
import asyncio
import pandas as pd
import spacy
import re
import time
from loguru import logger
from bs4 import BeautifulSoup

# Load NLP model for guest and entity recognition
nlp = spacy.load('en_core_web_sm')

# Define a class to structure the scraping logic
class PodcastScraper:
    
    def __init__(self):
        self.apple_podcasts_api = "https://itunes.apple.com/search?media=podcast&term="
        self.spotify_base_url = "https://api.spotify.com/v1"
        self.guest_pattern = r"(featuring|guest|with)\s([A-Z][a-z]+ [A-Z][a-z]+)"
        self.email_pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
        self.headers = {"Authorization": "Bearer YOUR_SPOTIFY_ACCESS_TOKEN"}
        
    async def fetch_apple_podcasts(self, search_term):
        """Fetch podcasts using the Apple Podcasts API"""
        url = f"{self.apple_podcasts_api}{search_term}&limit=50"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return self.process_apple_data(data)
                else:
                    logger.error(f"Apple Podcasts API error: {response.status}")
                    return []

    def process_apple_data(self, data):
        """Extract relevant data from Apple Podcasts API"""
        podcasts = []
        for item in data['results']:
            podcast_name = item.get('collectionName')
            episode_name = item.get('trackName')
            link = item.get('trackViewUrl')
            description = item.get('description')
            guest_name = self.extract_guest_name(description)
            guest_email = self.extract_email(description)
            podcasts.append([podcast_name, episode_name, link, guest_name, guest_email])
        return podcasts

    async def fetch_spotify_podcasts(self):
        """Fetch podcasts using the Spotify API"""
        url = f"{self.spotify_base_url}/shows?market=US&limit=50"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=self.headers) as response:
                if response.status == 200:
                    data = await response.json()
                    return self.process_spotify_data(data)
                else:
                    logger.error(f"Spotify API error: {response.status}")
                    return []

    def process_spotify_data(self, data):
        """Extract relevant data from Spotify"""
        podcasts = []
        for show in data['shows']['items']:
            podcast_name = show.get('name')
            episodes_url = show.get('href') + "/episodes"
            # Process episodes for each podcast...
            episodes = self.fetch_episodes_spotify(episodes_url)
            for ep in episodes:
                podcast_data = [podcast_name, ep['name'], ep['link'], ep['guest'], ep['email']]
                podcasts.append(podcast_data)
        return podcasts
    
    def fetch_episodes_spotify(self, url):
        """Extract episode data for Spotify (pseudo-code)"""
        # This is simplified; you'd need to make an API request here.
        return [{"name": "Episode 1", "link": "episode_link", "guest": "John Doe", "email": None}]

    async def fetch_google_podcasts(self):
        """Scrape Google Podcasts using Selenium (pseudo-code)"""
        # Implement Selenium-based scraper for Google Podcasts here
        return []

    def extract_guest_name(self, description):
        """Use regex and NLP to extract guest names"""
        if description:
            match = re.search(self.guest_pattern, description, re.IGNORECASE)
            if match:
                return match.group(2)
        return None

    def extract_email(self, text):
        """Use regex to extract emails from text"""
        match = re.search(self.email_pattern, text)
        return match.group(0) if match else None

    def export_to_csv(self, data):
        """Export the collected data to CSV"""
        df = pd.DataFrame(data, columns=['Podcast Name', 'Episode', 'Link', 'Guest Name', 'Email'])
        df.to_csv('podcast_guests_advanced.csv', index=False)
        logger.info("Data saved to podcast_guests_advanced.csv")

    async def run(self, search_term):
        """Run the full scraping pipeline asynchronously"""
        tasks = [
            self.fetch_apple_podcasts(search_term),
            self.fetch_spotify_podcasts(),
            self.fetch_google_podcasts()
        ]
        all_data = await asyncio.gather(*tasks)
        combined_data = [item for sublist in all_data for item in sublist]
        self.export_to_csv(combined_data)

# Asynchronous execution
if __name__ == "__main__":
    scraper = PodcastScraper()
    search_term = "technology"  # Example category to scrape
    asyncio.run(scraper.run(search_term))
