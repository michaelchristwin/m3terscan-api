from sqlmodel import Field, SQLModel, UniqueConstraint


class MonthlyEnergy(SQLModel, table=True):
    __table_args__ = (UniqueConstraint("year", "month", "day"),)
    id: int | None = Field(default=None, primary_key=True)
    day: int = Field(index=True)
    month: int = Field(index=True)
    year: int = Field(index=True)
    total_energy: float
