import os

from services.adapters.ads.base import BaseAdsAdapter


class GoogleAdsAdapter(BaseAdsAdapter):
    def __init__(self):
        self.developer_token = os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN")
        self.customer_id = os.getenv("GOOGLE_ADS_CUSTOMER_ID")
        self.client_id = os.getenv("GOOGLE_ADS_CLIENT_ID")
        self.client_secret = os.getenv("GOOGLE_ADS_CLIENT_SECRET")
        self.refresh_token = os.getenv("GOOGLE_ADS_REFRESH_TOKEN")

    def fetch_campaign_performance(self) -> list[dict]:
        print("Using Google Ads credentials from environment variables")

        return [
            {
                "platform": "google_ads",
                "campaign_id": 1001,
                "campaign_name": "Summer Campaign",
                "clicks": 1200,
                "impressions": 45000,
                "spend": 350.75,
            }
        ]