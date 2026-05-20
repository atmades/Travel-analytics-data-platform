import os

from services.adapters.ads.base import BaseAdsAdapter


class MetaAdsAdapter(BaseAdsAdapter):
    def __init__(self):
        self.access_token = os.getenv("META_ACCESS_TOKEN")
        self.account_id = os.getenv("META_AD_ACCOUNT_ID")

    def fetch_campaign_performance(self) -> list[dict]:
        # TODO: Replace with real Meta Marketing API request
        print("Using Meta Ads credentials from environment variables")

        return [
            {
                "platform": "meta_ads",
                "campaign_id": 2001,
                "campaign_name": "Facebook Retargeting",
                "clicks": 640,
                "impressions": 19000,
                "spend": 145.20,
            }
        ]