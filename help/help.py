from BaseMigration import BaseMigration


class Help(BaseMigration):
    def handle_data(self, datas=[]):
        pass


if __name__ == '__main__':
    help = Help("info_base.AppHelp")
    help.export_data()
