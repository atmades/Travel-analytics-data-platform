from services.adapters.ads.base import BaseAdsAdapter
from services.adapters.exceptions import ExternalApiError
from services.adapters.ads.validators import validate_ad_records
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
        if self.settings.ads_api_mode == AdsApiMode.REAL:
            raw_records = self._fetch_from_real_api()
        else:
            raw_records = [
                {
                    "platform": "google_ads",
                    "campaign_id": 1001,
                    "campaign_name": "Summer Campaign",
                    "clicks": 1200,
                    "impressions": 45000,
                    "spend": 350.75,
                }
            ]

        return validate_ad_records(raw_records)
    
    def _fetch_from_real_api(self) -> list[dict]:
        # TODO: Implement real Google Ads API request here.
        # This method should handle:
        # - OAuth credentials
        # - customer_id
        # - pagination
        # - retries
        # - rate limits

        try:
            raise NotImplementedError("Real Google Ads API integration is not implemented yet")

        except Exception as error:
            raise ExternalApiError(
                f"Google Ads API request failed: {error}"
            )