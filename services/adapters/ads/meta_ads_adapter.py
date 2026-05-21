from services.adapters.ads.base import BaseAdsAdapter
from services.adapters.ads.validators import validate_ad_records
from services.adapters.exceptions import ExternalApiError
from services.config.enums import AdsApiMode
from services.config.settings import (
    get_ads_settings,
    validate_meta_ads_settings,
)


class MetaAdsAdapter(BaseAdsAdapter):
    def __init__(self):
        self.settings = get_ads_settings()

        if self.settings.ads_api_mode == AdsApiMode.REAL:
            validate_meta_ads_settings(self.settings)

    def fetch_campaign_performance(self) -> list[dict]:
        if self.settings.ads_api_mode == AdsApiMode.REAL:
            raw_records = self._fetch_from_real_api()
        else:
            raw_records = [
                {
                    "platform": "meta_ads",
                    "campaign_id": 2001,
                    "campaign_name": "Facebook Retargeting",
                    "clicks": 640,
                    "impressions": 19000,
                    "spend": 145.20,
                }
            ]

        return validate_ad_records(raw_records)
    
    
    def _fetch_from_real_api(self) -> list[dict]:
        # TODO: Implement real Meta Marketing API request here.

        try:
            raise NotImplementedError("Real Meta Ads API integration is not implemented yet")

        except Exception as error:
            raise ExternalApiError(
                f"Meta Ads API request failed: {error}"
            )