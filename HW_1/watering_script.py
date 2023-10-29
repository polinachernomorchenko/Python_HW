from robots import WaterRobot, Journal, NotEnoughWaterError
from pymongo import MongoClient

MONGO_CLIENT = MongoClient(host = 'localhost:27017')

water_robot = WaterRobot('w1', 5)
journal = Journal(MONGO_CLIENT)

try:
    water_robot.water_tree(journal)
except NotEnoughWaterError:
    water_robot.refill_tank()
    water_robot.water_tree(journal)

    

