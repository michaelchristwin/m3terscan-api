from sqlmodel import SQLModel


class WeeksEnergyCreate(SQLModel):
    year: int
    week: int
    total_energy: float


class WeeksEnergyRead(SQLModel):
    year: int
    week: int
    total_energy: float
