class Person(object):
    def __init__(self, name):
        self.name = name

    def print_name(self):
        print(self.name)


class ZhangSan(Person):
    def test(self):
        self.print_name()


if __name__ == '__main__':
    zhangsan = ZhangSan()
    zhangsan.test()
