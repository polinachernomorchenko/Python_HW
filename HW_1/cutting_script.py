from robots import CutRobot, Journal
from pymongo import MongoClient

MONGO_CLIENT = MongoClient(host = 'localhost:27017')

cut_robot = CutRobot('c1')
journal = Journal(MONGO_CLIENT)

cut_robot.cut_branches(journal)
