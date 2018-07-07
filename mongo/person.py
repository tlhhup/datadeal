from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client.test


class Person(object):

    def find_persons(self):
        persons = db.person.find()
        if persons:
            for person in persons:
                print(person)

    def insert(self):
        person = {
            'name': '张三',
            'age': 26
        }
        db.person.insert_one(person)


person = Person()
person.insert()
