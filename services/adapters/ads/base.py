from abc import ABC, abstractmethod


class BaseAdsAdapter(ABC):
    @abstractmethod
    def fetch_campaign_performance(self) -> list[dict]:
        pass