from pymongo import MongoClient

from templateMonitor import TemplateComparator, CallbackAction


def get_db():
    myclient = MongoClient("mongodb://localhost:27017/")
    return myclient["mydb"]


mydb = get_db()


class TestAction(CallbackAction):
    def callback(self, comparison_result):
        print("test print", comparison_result)


test = TestAction()
comparison = TemplateComparator(mydb, 'pages', test)
comparison.run_task()
