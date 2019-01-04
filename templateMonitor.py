# encoding=utf-8

from pymongo import MongoClient
from html_similarity import style_similarity
from html_similarity import structural_similarity
import datetime


class Action(object):
    """
    被调用触发器
    """
    def callback(self, comparison_result):
        """
        虚函数，需要被集成实现触发回调，根据下面数据判断是否需要触发，以及触发何种动作
        :param comparison_result: 比较结果集，包括seed，html，time，差异，总数，成功数
        :return:
        """
        pass


class TemplateComparator(object):
    debug = True

    def __init__(self, database, pages_name, callback, baseline='baseline', comparison_result='comparisonResult',
                 style_weight=0.8):
        """获取数据库及表名
        :param database: 数据库
        :param pages_name: 待比较页面表
        :param baseline: 基线表名
        :param comparison_result: 比较结果存放表名
        :param style_weight: 比较结果风格权重，一般>=0.7
        """
        self.pages = database.get_collection(pages_name)
        self.baseline = database.get_collection(baseline)
        self.comparison = database.get_collection(comparison_result)
        self.callback = callback

        self.STYLE_WEIGHT = style_weight

    @staticmethod
    def get_row(result):
        # print(type(result))
        try:
            page = result.next()
        except StopIteration:
            return None, False
        return page, True

    @staticmethod
    def compare(h1, h2):
        # print("h1",h1)
        # print("h2",h2)
        style_rate = style_similarity(h1, h2)
        struc_rate = structural_similarity(h1, h2)
        return style_rate, struc_rate

    def get_similarity(self, style_rate, struc_rate):
        similarity = self.STYLE_WEIGHT*style_rate+(1-self.STYLE_WEIGHT)*struc_rate
        if TemplateComparator.debug:
            print(style_rate, struc_rate, similarity)
        return similarity

    def save_compare(self, base, page, style_rate, struc_rate):
        """保存比较结果
        :param base:
        :param page:
        :param style_rate:
        :param struc_rate:
        :return: 比较结果数据
        """
        dict = {"seed": page['seed'], "last_time": base['time'], "current_time": page['time'], "style_rate": style_rate,
                "struc_rate": struc_rate, "last_count": page['all'], "success_count": page['success']}
        self.comparison.insert(dict)
        return dict

    def save_baseline(self, base, page, newpage):
        to_save_dict = {'time': page['time'], 'seed': page['seed'], 'html': page['html'],
                        'timestamp': datetime.datetime.now()}
        if not newpage:
            to_save_dict['_id'] = base['_id']
        if TemplateComparator.debug:
            print("tosave:", newpage, to_save_dict)
        self.baseline.save(to_save_dict)

    def save_compare_task(self, base, page, style_rate, struc_rate, newpage):
        """将来改成push到队列，异步操作会更理想
        :param base: 历史基线页面
        :param page: 待比较新页面
        :param style_rate: 风格差异率
        :param struc_rate: 结构差异率
        :param newpage: 是否新页面；False老页面需更新，True新页面
        :return:
        """
        dict = self.save_compare(base, page, style_rate, struc_rate)
        self.save_baseline(base, page, newpage)
        # print('callback ',self.callback, not self.callback)
        if self.callback: # != None:
            self.callback.callback(dict)

    def compare_baseline(self, base, page):
        if base['time'] >= page['time']:
            return -1
        style_rate, struc_rate = TemplateComparator.compare(base['html'], page['html'])
        self.save_compare_task(base, page, style_rate, struc_rate, False)
        return self.get_similarity(style_rate, struc_rate)

    def compare_history(self, page):
        history = self.pages.find({"seed": page['seed'], "time": {"$lt": page['time']}}).sort("time", 1).limit(1)

        first, br = TemplateComparator.get_row(history)
        if not br:
            return
        style_rate, struc_rate = TemplateComparator.compare(first['html'], page['html'])
        if TemplateComparator.debug:
            self.get_similarity(style_rate, struc_rate)
        self.save_compare_task(first, page, style_rate, struc_rate, True)

    # 转化查询结果对象为记录对象
    @staticmethod
    def get_lastest(pages):
        row, bp = TemplateComparator.get_row(pages)
        if bp:
            return row['row'], True
        return None, False

    def run_task(self):
        files = self.pages.aggregate([
            {"$project": {
                "seed": "$seed",
                "time": "$time",
                "document": "$$ROOT"
            }},
            {"$sort": {"time": -1}},
            {"$group": {"_id": "$seed", "row": {"$first": "$document"}}},
            {"$sort": {"row.seed": 1}}])

        baseline = self.baseline.find().sort('seed', 1)
        page, bp = TemplateComparator.get_lastest(files)
        base, bb = TemplateComparator.get_row(baseline)
        while bp:
            seed = page['seed']
            if TemplateComparator.debug and bb:
                print("B", base['seed'])
            if not bb or base['seed'] > seed:
                if TemplateComparator.debug:
                    print(">", seed)
                self.compare_history(page)
                page, bp = TemplateComparator.get_lastest(files)
                continue
            elif base['seed'] == seed:
                if TemplateComparator.debug:
                    print("=", seed)
                self.compare_baseline(base, page)
                page, bp = TemplateComparator.get_lastest(files)
            else:
                # skip baseline, nothing to be compared, do nothing
                if TemplateComparator.debug:
                    print("<", seed)
            base, bb = TemplateComparator.get_row(baseline)

