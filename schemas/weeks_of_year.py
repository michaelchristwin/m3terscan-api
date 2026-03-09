from sqlmodel import SQLModel


class WeeksEnergyCreate(SQLModel):
    year: int
    week: int
    meter_id: int
    total_energy: float


class WeeksEnergyRead(SQLModel):
    year: int
    week: int
    meter_id: int
    total_energy: float
