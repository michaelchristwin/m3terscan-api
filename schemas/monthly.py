from sqlmodel import SQLModel


class MonthlyEnergyCreate(SQLModel):
    day: int
    month: int
    year: int
    total_energy: float


class MonthlyEnergyRead(SQLModel):
    day: int
    month: int
    year: int
    total_energy: float
