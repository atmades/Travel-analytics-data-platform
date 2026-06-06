from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
)

from services.adapters.exceptions import AdapterValidationError





class AdMetricsRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")

    platform: str = Field(pattern="^(google_ads|meta_ads)$")
    campaign_id: int
    campaign_name: str
    impressions: int = 0
    clicks: int = 0
    spend: float = 0.0

    @field_validator("spend", "clicks", "impressions")
    @classmethod
    def non_negative(cls, value: float | int) -> float | int:
        if value < 0:
            raise ValueError("Metric value cannot be negative")
        return value

    @field_validator("campaign_name")
    @classmethod
    def name_not_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("campaign_name cannot be empty or whitespace")
        return value.strip()


def validate_ad_records(records: list[dict]) -> list[dict]:
    validated_records = []

    for record in records:
        try:
            validated_records.append(
                AdMetricsRecord(**record).model_dump()
            )
        except ValidationError as error:
            raise AdapterValidationError(
                f"Ad record validation failed: {error}"
            )

    return validated_records


# from services.adapters.exceptions import AdapterValidationError


# REQUIRED_AD_FIELDS = {
#     "platform",
#     "campaign_id",
#     "campaign_name",
#     "clicks",
#     "impressions",
#     "spend",
# }


# def validate_ad_record(record: dict) -> None:
#     missing_fields = REQUIRED_AD_FIELDS - record.keys()

#     if missing_fields:
#         raise AdapterValidationError(
#             f"Missing required ad fields: {', '.join(missing_fields)}"
#         )

#     if not isinstance(record["campaign_id"], int):
#         raise AdapterValidationError("campaign_id must be int")

#     if not isinstance(record["campaign_name"], str):
#         raise AdapterValidationError("campaign_name must be str")

#     if not isinstance(record["clicks"], int):
#         raise AdapterValidationError("clicks must be int")

#     if not isinstance(record["impressions"], int):
#         raise AdapterValidationError("impressions must be int")

#     if not isinstance(record["spend"], float):
#         raise AdapterValidationError("spend must be float")


# def validate_ad_records(records: list[dict]) -> None:
#     for record in records:
#         validate_ad_record(record)