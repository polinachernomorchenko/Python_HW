from dataclasses import dataclass
from datetime import datetime, timedelta
from collections import namedtuple
from typing import Any
import pymongo


MONGO_CLIENT = pymongo.MongoClient(host="localhost:27017")
DailyData = namedtuple("DailyData", ["date", "amount"])
WeeklyData = namedtuple("WeeklyData", ["week_number", "amount"])


class NotEnoughWaterError(Exception):
    pass


class DBHelper:
    """Set of methods for querying MongoDB"""

    @staticmethod
    def sum_by_field_value(
        collection: pymongo.collection.Collection,
        field_to_match: str,
        value: Any,
        field_to_sum: str,
        operator: str = None,
    ):
        """Returns sum of target field based on matching condition"""
        if operator:
            value = {operator: value}

        pipeline = [
            {"$match": {field_to_match: value}},
            {"$group": {"_id": None, "total": {"$sum": f"${field_to_sum}"}}},
        ]
        result = list(collection.aggregate(pipeline))
        return result[0]["total"] if result else 0

    @staticmethod
    def sum_field_by_date(
        collection: pymongo.collection.Collection,
        field,
        timestamp_field_name="timestamp"
    ):
        """Returns docs with field grouped by date"""
        pipeline = [
            {
                "$addFields": {
                    "date": {"$dateToParts": {"date": f"${timestamp_field_name}"}}
                }
            },
            {
                "$group": {
                    "_id": {
                        "year": "$date.year",
                        "month": "$date.month",
                        "day": "$date.day",
                    },
                    "total": {"$sum": f"${field}"},
                }
            },
            {"$sort": {"year": 1, "month": 1, "day": 1}},
            {
                "$project": {
                    "date": {
                        "$dateFromParts": {
                            "year": "$_id.year",
                            "month": "$_id.month",
                            "day": "$_id.day",
                        }
                    },
                    "total": 1,
                }
            },
        ]
        result = list(collection.aggregate(pipeline))
        return result

    @staticmethod
    def sum_field_by_week(collection, field, timestamp_field_name="timestamp"):
        """Returns docs with field grouped by week"""
        pipeline = [
            {"$addFields": {"week_number": {"$week": f"${timestamp_field_name}"}}},
            {"$group": {"_id": "$week_number", "total": {"$sum": f"${field}"}}},
            {"$sort": {"week": 1}},
        ]
        result = list(collection.aggregate(pipeline))
        return result


@dataclass
class Robot:
    """Base Robot class"""
    _id_number: int

    @property
    def id_number(self):
        return self._id_number


class Journal:
    def __init__(
            self,
            mongo_client: pymongo.mongo_client.MongoClient
    ) -> None:
        self.db_client = mongo_client
        self.db = mongo_client.BinaryTreeJournal
        self.cut_journal = self.db.CutJournal
        self.water_journal = self.db.WaterJournal
        self.stored_actions = ["cut", "water"]
        self.max_water = 2
        self.min_water = 1
        self.max_branches = 8
        self.min_branches = 4

    def update(self, robot: Robot, data: dict) -> None:
        """Update DB after tree caring"""
        target_collection = (
            self.cut_journal if isinstance(robot, CutRobot) else self.water_journal
        )
        target_collection.insert_one(data)

    def get_required_amount_of_water(self) -> float:
        """Returns required amount of water in liters"""
        today = datetime.today().replace(hour=0, minute=0, second=0)
        todays_water_amount = DBHelper.sum_by_field_value(
            self.water_journal, "timestamp", today, "water_amount", "$gte"
        )
        if todays_water_amount >= self.max_water:
            return 0
        return self.max_water - todays_water_amount

    def get_number_of_branches_to_cut(self) -> int:
        """Returns required number of branches to cut"""
        today = datetime.today().replace(hour=0, minute=0, second=0)
        monday_date = today - timedelta(days=today.weekday())
        weeks_branches_amount = DBHelper.sum_by_field_value(
            self.cut_journal, "timestamp", monday_date, "number_of_branches", "$gte"
        )

        if weeks_branches_amount >= self.max_branches:
            return 0
        return self.max_branches - weeks_branches_amount

    def get_ordering_errors(self) -> list:
        """Returns list of ObjectIds of wrongly placed records in journal"""
        collections = list(self.db.list_collection_names())
        ordering_errors = {collection: [] for collection in collections}
        for collection in collections:
            id_sort_res = list(self.db[collection].find().sort("_id", 1))
            time_sort_res = list(self.db[collection].find().sort("timestamp", 1))
            if id_sort_res != time_sort_res:
                for i, entry in enumerate(id_sort_res[:-1]):
                    if id_sort_res[i]["timestamp"] > id_sort_res[i + 1]["timestamp"]:
                        ordering_errors[collection].append(entry["_id"])

        return ordering_errors

    def get_water_errors(self):
        """Returns list of dates with wrong amount of water"""
        daily_water_data = DBHelper.sum_field_by_date(
            self.water_journal, "water_amount"
        )
        existing_data = [
            DailyData(day["date"], day["total"]) for day in daily_water_data
        ]
        existing_data = sorted(existing_data, key=lambda x: x.date)
        existing_dates = [day.date for day in existing_data]
        start = existing_dates[0]
        stop = existing_dates[-1]
        total_days = (stop - start).days
        for i in range(total_days):
            next_date = start + timedelta(i)
            if next_date not in existing_dates:
                existing_data.append(DailyData(next_date, 0))
        errors = [
            day
            for day in existing_data
            if not (self.min_water <= day.amount <= self.max_water)
        ]

        return errors

    def get_cut_errors(self):
        """Returns list of week numbers with wrong number of cut branches
        (week numbers are relative to current year)"""
        res = DBHelper.sum_field_by_week(self.cut_journal, "number_of_branches")
        week_data = [WeeklyData(week["_id"], week["total"]) for week in res]
        week_data = sorted(week_data, key=lambda x: x.week_number)
        existing_weeks = [week.week_number for week in week_data]
        start = week_data[0]
        stop = week_data[-1]
        for week_number in range(start.week_number, stop.week_number):
            if week_number not in existing_weeks:
                week_data.append(WeeklyData(week_number, 0))

        wrong_weeks = [
            week
            for week in week_data
            if not self.min_branches <= week.amount <= self.max_branches
        ]

        return wrong_weeks

    def print_last_10_records(self, action: str):
        """Prints last 10 records of target care action"""
        if action not in self.stored_actions:
            raise ValueError(
                f'Unknown action. Should be one of {", ".join(self.stored_actions)}'
            )

        collection = self.cut_journal if action == "cut" else self.water_journal
        last_10_documents = collection.find().sort("_id", -1).limit(10)
        str_res = "\n".join([str(doc) for doc in last_10_documents])
        print(str_res)


@dataclass
class WaterRobot(Robot):
    """Robot for watering"""
    _volume: float
    _water_level: float = 0

    @property
    def tank_volume(self):
        return self._volume

    @tank_volume.setter
    def tank_volume(self, new_volume: float):
        if not isinstance(new_volume, (int, float)):
            raise ValueError(f"Expected int or float, {type(new_volume)} passed")
        elif new_volume <= 0:
            raise ValueError("Volume should be > 0")

        else:
            self._volume = new_volume

    @property
    def water_level(self):
        return self._water_level

    def refill_tank(self):
        self._water_level = self._volume

    def water_tree(self, journal: Journal):
        """Waters tree if needed and updates journal"""
        required_volume = journal.get_required_amount_of_water()
        if required_volume:
            if required_volume <= self._water_level:
                self._water_level = self._water_level - required_volume
                update_data = {
                    "robot_id": self.id_number,
                    "water_amount": required_volume,
                    "timestamp": datetime.now(),
                }
                journal.update(self, update_data)
            else:
                raise NotEnoughWaterError(
                    f"Current water level is {self.water_level}l,\
                                           {required_volume}l is required "
                )
        else:
            print("Tree is already watered")


@dataclass
class CutRobot(Robot):
    """Robot for cutting"""
    def cut_branches(self, journal: Journal):
        """Cuts branches if needed and updates journal"""
        branches_to_cut = journal.get_number_of_branches_to_cut()
        if branches_to_cut:
            update_data = {
                "robot_id": self.id_number,
                "number_of_branches": branches_to_cut,
                "timestamp": datetime.now(),
            }
            journal.update(self, update_data)
        else:
            print("No branches to cut")
