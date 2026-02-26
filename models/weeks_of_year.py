from sqlmodel import Field, SQLModel, UniqueConstraint


class WeeksEnergy(SQLModel, table=True):
    __table_args__ = (UniqueConstraint("year", "week"),)

    id: int | None = Field(default=None, primary_key=True)
    year: int = Field(index=True)
    week: int = Field(index=True)
    total_energy: float
