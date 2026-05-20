from fastapi import FastAPI

from services.adapters.ads.google_ads_adapter import GoogleAdsAdapter
from services.adapters.ads.meta_ads_adapter import MetaAdsAdapter


app = FastAPI(title="Mock Ads API")


@app.get("/google-ads")
def get_google_ads():
    adapter = GoogleAdsAdapter()
    return adapter.fetch_campaign_performance()


@app.get("/meta-ads")
def get_meta_ads():
    adapter = MetaAdsAdapter()
    return adapter.fetch_campaign_performance()