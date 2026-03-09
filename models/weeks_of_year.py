from sqlmodel import Field, SQLModel, UniqueConstraint


class WeeksEnergy(SQLModel, table=True):
    __table_args__ = (UniqueConstraint("year", "week", "meter_id"),)

    id: int | None = Field(default=None, primary_key=True)
    meter_id: int = Field(index=True)
    year: int = Field(index=True)
    week: int = Field(index=True)
    total_energy: float
