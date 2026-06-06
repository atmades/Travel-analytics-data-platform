import os
from dataclasses import dataclass
from dotenv import load_dotenv
from services.config.enums import AdsApiMode
from services.adapters.exceptions import MissingCredentialsError


load_dotenv()


@dataclass(frozen=True)
class AdsSettings:
    meta_access_token: str | None
    meta_ad_account_id: str | None

    google_ads_developer_token: str | None
    google_ads_client_id: str | None
    google_ads_client_secret: str | None
    google_ads_refresh_token: str | None
    google_ads_customer_id: str | None
    ads_api_mode: AdsApiMode



def get_ads_settings() -> AdsSettings:
    return AdsSettings(
        meta_access_token=os.getenv("META_ACCESS_TOKEN"),
        meta_ad_account_id=os.getenv("META_AD_ACCOUNT_ID"),
        google_ads_developer_token=os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN"),
        google_ads_client_id=os.getenv("GOOGLE_ADS_CLIENT_ID"),
        google_ads_client_secret=os.getenv("GOOGLE_ADS_CLIENT_SECRET"),
        google_ads_refresh_token=os.getenv("GOOGLE_ADS_REFRESH_TOKEN"),
        google_ads_customer_id=os.getenv("GOOGLE_ADS_CUSTOMER_ID"),
        ads_api_mode=AdsApiMode(os.getenv("ADS_API_MODE", "mock"))
    )

def validate_meta_ads_settings(settings: AdsSettings) -> None:
    required = {
        "META_ACCESS_TOKEN": settings.meta_access_token,
        "META_AD_ACCOUNT_ID": settings.meta_ad_account_id,
    }

    missing = [name for name, value in required.items() if not value]

    if missing:
       raise MissingCredentialsError(
            f"Missing required Meta Ads settings: {', '.join(missing)}"
        )


def validate_google_ads_settings(settings: AdsSettings) -> None:
    required = {
        "GOOGLE_ADS_DEVELOPER_TOKEN": settings.google_ads_developer_token,
        "GOOGLE_ADS_CUSTOMER_ID": settings.google_ads_customer_id,
    }

    missing = [name for name, value in required.items() if not value]

    if missing:
        raise MissingCredentialsError(
            f"Missing required Meta Ads settings: {', '.join(missing)}"
        )