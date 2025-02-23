import math
import pandas as pd
from typing import Optional

class GEO_Map:
    __instance = None

    @staticmethod
    def get_instance() -> 'GEO_Map':
        """Trả về instance singleton của GEO_Map."""
        if GEO_Map.__instance is None:
            GEO_Map.__instance = GEO_Map()
        return GEO_Map.__instance

    def __init__(self):
        """Khởi tạo GEO_Map, đọc file CSV từ /app/data/uszipsv.csv với exception handling."""
        if GEO_Map.__instance is not None:
            raise Exception("This class is a singleton!")
        GEO_Map.__instance = self
        try:
            # Đọc file CSV, giả sử file nằm trong /app/data
            self.map = pd.read_csv("/app/data/uszipsv.csv", header=None, names=['A', 'B', 'C', 'D', 'E'])
            self.map['A'] = self.map['A'].astype(str)  # Đảm bảo cột A là string
        except FileNotFoundError:
            raise FileNotFoundError("File /app/data/uszipsv.csv not found. Please ensure it exists in the container.")
        except pd.errors.EmptyDataError:
            raise ValueError("File /app/data/uszipsv.csv is empty or invalid.")
        except Exception as e:
            raise Exception(f"Error loading uszipsv.csv: {str(e)}")

    def get_lat(self, postcode: str) -> Optional[float]:
        """Trả về latitude dựa trên postcode (giả sử postcode là chuỗi)."""
        try:
            result = self.map[self.map['A'] == postcode]['B'].iloc[0]
            return float(result) if pd.notna(result) else None
        except (IndexError, ValueError, KeyError) as e:
            print(f"Warning: Could not find latitude for postcode {postcode}: {str(e)}")
            return None

    def get_long(self, postcode: str) -> Optional[float]:
        """Trả về longitude dựa trên postcode."""
        try:
            result = self.map[self.map['A'] == postcode]['C'].iloc[0]
            return float(result) if pd.notna(result) else None
        except (IndexError, ValueError, KeyError) as e:
            print(f"Warning: Could not find longitude for postcode {postcode}: {str(e)}")
            return None

    def distance(self, lat1: float, long1: float, lat2: float, long2: float) -> Optional[float]:
        """Tính khoảng cách giữa 2 điểm sử dụng công thức Haversine."""
        if any(x is None for x in [lat1, long1, lat2, long2]):
            print("Warning: Invalid coordinates, returning None")
            return None

        try:
            theta = long1 - long2
            dist = math.sin(self.deg2rad(lat1)) * math.sin(self.deg2rad(lat2)) + \
                   math.cos(self.deg2rad(lat1)) * math.cos(self.deg2rad(lat2)) * math.cos(self.deg2rad(theta))
            dist = math.acos(dist)
            dist = self.rad2deg(dist)
            dist = dist * 60 * 1.1515 * 1.609344  # Chuyển đổi thành km
            return dist
        except Exception as e:
            print(f"Error calculating distance: {str(e)}")
            return None

    def rad2deg(self, rad: float) -> float:
        """Chuyển đổi radian thành độ."""
        return rad * 180.0 / math.pi

    def deg2rad(self, deg: float) -> float:
        """Chuyển đổi độ thành radian."""
        return deg * math.pi / 180.0