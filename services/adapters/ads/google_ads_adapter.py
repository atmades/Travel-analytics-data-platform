from services.adapters.ads.base import BaseAdsAdapter
from services.config.enums import AdsApiMode
from services.config.settings import (
    get_ads_settings,
    validate_google_ads_settings,
)

class GoogleAdsAdapter(BaseAdsAdapter):
    def __init__(self):
        self.settings = get_ads_settings()
        
        if self.settings.ads_api_mode == AdsApiMode.REAL:
            validate_google_ads_settings(self.settings)
       

    def fetch_campaign_performance(self) -> list[dict]:
        # TODO: Replace with real Google Ads API integration
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