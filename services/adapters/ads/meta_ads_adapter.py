from services.adapters.ads.base import BaseAdsAdapter
from services.config.settings import get_ads_settings


class MetaAdsAdapter(BaseAdsAdapter):
    def __init__(self):
        self.settings = get_ads_settings()

    def fetch_campaign_performance(self) -> list[dict]:
        print(
            "Using Meta Ads credentials from environment variables:",
            self.settings.meta_ad_account_id,
        )

        # TODO: Replace with real Meta Marketing API integration

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