from sqlmodel import SQLModel


class MonthlyEnergyCreate(SQLModel):
    day: int
    month: int
    year: int
    meter_id: int
    total_energy: float


class MonthlyEnergyRead(SQLModel):
    day: int
    month: int
    year: int
    meter_id: int
    total_energy: float
